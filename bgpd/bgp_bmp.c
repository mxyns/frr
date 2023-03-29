// SPDX-License-Identifier: GPL-2.0-or-later
/* BMP support.
 * Copyright (C) 2018 Yasuhiro Ohara
 * Copyright (C) 2019 David Lamparter for NetDEF, Inc.
 */

#include <zebra.h>

#include "log.h"
#include "stream.h"
#include "sockunion.h"
#include "command.h"
#include "prefix.h"
#include "thread.h"
#include "linklist.h"
#include "queue.h"
#include "pullwr.h"
#include "memory.h"
#include "network.h"
#include "filter.h"
#include "lib_errors.h"
#include "stream.h"
#include "libfrr.h"
#include "lib/version.h"
#include "jhash.h"
#include "termtable.h"
#include "string.h"
#include "time.h"

#include "bgpd/bgp_table.h"
#include "bgpd/bgpd.h"
#include "bgpd/bgp_route.h"
#include "bgpd/bgp_attr.h"
#include "bgpd/bgp_dump.h"
#include "bgpd/bgp_errors.h"
#include "bgpd/bgp_packet.h"
#include "bgpd/bgp_bmp.h"
#include "bgpd/bgp_fsm.h"
#include "bgpd/bgp_updgrp.h"
#include "bgpd/bgp_vty.h"
#include "bgpd/bgp_trace.h"
#include "bgpd/bgp_network.h"
#include "bgp_addpath.h"

static void bmp_close(struct bmp *bmp);
static struct bmp_bgp *bmp_bgp_find(struct bgp *bgp);
static void bmp_targets_put(struct bmp_targets *bt);
static struct bmp_bgp_peer *bmp_bgp_peer_find(uint64_t peerid);
static struct bmp_bgp_peer *bmp_bgp_peer_get(struct peer *peer);
static void bmp_active_disconnected(struct bmp_active *ba);
static void bmp_active_put(struct bmp_active *ba);

DEFINE_MGROUP(BMP, "BMP (BGP Monitoring Protocol)");

DEFINE_MTYPE_STATIC(BMP, BMP_CONN,	"BMP connection state");
DEFINE_MTYPE_STATIC(BMP, BMP_TARGETS,	"BMP targets");
DEFINE_MTYPE_STATIC(BMP, BMP_TARGETSNAME, "BMP targets name");
DEFINE_MTYPE_STATIC(BMP, BMP_LISTENER,	"BMP listener");
DEFINE_MTYPE_STATIC(BMP, BMP_ACTIVE,	"BMP active connection config");
DEFINE_MTYPE_STATIC(BMP, BMP_ACLNAME,	"BMP access-list name");
DEFINE_MTYPE_STATIC(BMP, BMP_QUEUE,	"BMP update queue item");
DEFINE_MTYPE_STATIC(BMP, BMP,		"BMP instance state");
DEFINE_MTYPE_STATIC(BMP, BMP_MIRRORQ,	"BMP route mirroring buffer");
DEFINE_MTYPE_STATIC(BMP, BMP_PEER,	"BMP per BGP peer data");
DEFINE_MTYPE_STATIC(BMP, BMP_OPEN,	"BMP stored BGP OPEN message");
DEFINE_MTYPE_STATIC(BMP, BMP_LBPI,	"BMP locked BPI");

DEFINE_QOBJ_TYPE(bmp_targets);

/* module startup time for the startup-delay */
static struct timeval bmp_startup_time = { 0 };

/* compute the time in millis since the bmp_startup_time recorded */
static uint32_t bmp_time_since_startup(struct timeval *delay) {

	if (bmp_startup_time.tv_sec == 0 && bmp_startup_time.tv_usec == 0) {
		zlog_info("bmp [%s]: Startup time not recorded", __func__);
		return 0;
	}

	uint32_t micros = (uint32_t) (monotime_since(&bmp_startup_time, delay));

	return micros / 1000;
}

/* convert the enum BMP_State to a human-friendly display string */
static const char *bmp_state_str(enum BMP_State state) {
	switch (state) {

	case BMP_StartupIdle:
		return "Startup-Wait";
	case BMP_PeerUp:
		return "Peer-Up";
	case BMP_Run:
		return "Running";
	default:
		return "Unknown";
	}
}

/* comparison function for struct bmp_bgp used in bmp_bgph hashtable
 * compares the pointer values of the bgp instance
 */
static int bmp_bgp_cmp(const struct bmp_bgp *a, const struct bmp_bgp *b)
{
	if (a->bgp < b->bgp)
		return -1;
	if (a->bgp > b->bgp)
		return 1;
	return 0;
}

/* hash function for struct bmp_bgp used in bmp_bgph hashtable
 * hashes based on the pointer value of the bgp instance
 */
static uint32_t bmp_bgp_hash(const struct bmp_bgp *e)
{
	return jhash(&e->bgp, sizeof(e->bgp), 0x55aa5a5a);
}

DECLARE_HASH(bmp_bgph, struct bmp_bgp, bbi, bmp_bgp_cmp, bmp_bgp_hash);

/* hashtable to store the bmp state for a bgp instance
 * lookup by struct bgp pointer value
 */
struct bmp_bgph_head bmp_bgph;

/* comparison function for struct bmp_bgp_peer used in bmp_peerh hashtable
 * compares the peer id of each peer (qobj ids)
 */
static int bmp_bgp_peer_cmp(const struct bmp_bgp_peer *a,
		const struct bmp_bgp_peer *b)
{
	if (a->peerid < b->peerid)
		return -1;
	if (a->peerid > b->peerid)
		return 1;
	return 0;
}

/* hash function for struct bmp_bgp_peer used in bmp_peerh hashtable
 * hashes based on the peer id (qobj id)
 */
static uint32_t bmp_bgp_peer_hash(const struct bmp_bgp_peer *e)
{
	return e->peerid;
}

DECLARE_HASH(bmp_peerh, struct bmp_bgp_peer, bpi,
		bmp_bgp_peer_cmp, bmp_bgp_peer_hash);

/* hashtable to store the bmp state for bgp peers state (open messages store)
 * lookup by peer id (qobj id)
 * this hashtable holds the head of head of a linked list of bgp path info
 * for a specific bgp + destination prefix tuple then match on bpi pointer value
 */
struct bmp_peerh_head bmp_peerh;

/* comparison function for struct bmp_bpi_lock used in bmp_lbpi_h hashtable
 * compares the prefixes and bgp instance pointer values
 */
static int bmp_bpi_lock_cmp(const struct bmp_bpi_lock *a,
			    const struct bmp_bpi_lock *b)
{
	int cmp = prefix_cmp(&a->dest->p, &b->dest->p);
	if (cmp)
		return cmp;

	if (a->bgp < b->bgp)
		return -1;
	if (a->bgp > b->bgp)
		return 1;

	return 0;
}

/* hash function for struct bmp_bpi_lock used in bmp_lbpi_h hashtable
 * hashes based on the dest prefix and bgp instance pointer value
 */
static uint32_t bmp_bpi_lock_hash(const struct bmp_bpi_lock *e)
{
	uint32_t key = prefix_hash_key(&e->dest->p);
	key = jhash(&e->bgp, sizeof(e->bgp), key);

	return key;
}

DECLARE_HASH(bmp_lbpi_h, struct bmp_bpi_lock, lbpi_h,
		bmp_bpi_lock_cmp, bmp_bpi_lock_hash);

/* hashtable to store the bgp path state when withdrawn
 * allows rib-out pre-policy to run the pre-policy check on the path after
 * it has been withdrawn
 * lookup by destination prefix and bgp instance pointer value (vrf safe)
 */
struct bmp_lbpi_h_head bmp_lbpi;

/* lock a bgp path info for a bgp instance and store it in bmp_lbpi
 * allocate and store in hashtable if not exist
 * lock bgp_path_info, dest and bgp to keep then allocated
 * increment the lock
 * returns the lock structure if successful
 */
static struct bmp_bpi_lock *bmp_lock_bpi(struct bgp *bgp,
					 struct bgp_path_info *bpi)
{
	if (!bpi && !bpi)
		return NULL;

	BMP_LBPI_LOOKUP_BPI(head, prev, hash_lookup, bpi, bgp);

	if (!hash_lookup) {
		hash_lookup = XCALLOC(MTYPE_BMP_LBPI, sizeof(struct bmp_bpi_lock));
		SET_FLAG(bpi->flags, BGP_PATH_BMP_LOCKED);
		hash_lookup->bgp = bgp;
		hash_lookup->locked = bpi;
		hash_lookup->dest = bpi->net;
		hash_lookup->next = NULL;
		hash_lookup->lock = 0;
		bgp_lock(hash_lookup->bgp);
		bgp_path_info_lock(hash_lookup->locked);
		bgp_dest_lock_node(hash_lookup->dest);

		/* here prev is tail bc hash_lookup == tail->next == NULL */
		if (!prev)
			bmp_lbpi_h_add(&bmp_lbpi, hash_lookup);
		else
			prev->next = hash_lookup;

	}

	hash_lookup->lock++;

	return hash_lookup;
}


/* lock a bgp path info for a bgp instance and store it in bmp_lbpi
 * look up locks for this bgp_path_info and bgp instance
 * decrement the lock
 * if lock is <= 0 we need to free the lock and unlock held structures
 * returns the lock structure if it is not freed
 */
static struct bmp_bpi_lock *bmp_unlock_bpi(struct bgp *bgp,
					   struct bgp_path_info *bpi)
{

	if (!bpi)
		return NULL;

	BMP_LBPI_LOOKUP_BPI(head, prev, hash_lookup, bpi, bgp);

	/* nothing found, bpi is not locked, cannot unlock */
	if (!hash_lookup)
		return NULL;

	/* unlock once */
	hash_lookup->lock--;

	/* if bpi is not used by bmp anymore */
	if (hash_lookup->lock <= 0) {

		struct bgp_path_info *tmp_bpi = hash_lookup->locked;
		struct bgp_dest *tmp_dest = hash_lookup->dest;
		struct bgp *tmp_bgp = hash_lookup->bgp;

		/* swap hash list head */
		if (head == hash_lookup) {
			bmp_lbpi_h_del(&bmp_lbpi, hash_lookup);
			if (head->next)
				bmp_lbpi_h_add(&bmp_lbpi, head->next);
		}

		/* relink list */
		if (prev)
			prev->next = hash_lookup->next;

		UNSET_FLAG(bpi->flags, BGP_PATH_BMP_LOCKED);
		XFREE(MTYPE_BMP_LBPI, hash_lookup);
		bgp_unlock(tmp_bgp);
		bgp_dest_unlock_node(tmp_dest);
		bgp_path_info_unlock(tmp_bpi);

		return NULL;
	}

	return hash_lookup;
}

/* free a bqe */
static inline void bmp_bqe_free(struct bmp_queue_entry *bqe)
{
	if (!bqe)
		return;

	XFREE(MTYPE_BMP_QUEUE, bqe);
}

DECLARE_LIST(bmp_mirrorq, struct bmp_mirrorq, bmi);

/* listener management */

static int bmp_listener_cmp(const struct bmp_listener *a,
		const struct bmp_listener *b)
{
	int c;

	c = sockunion_cmp(&a->addr, &b->addr);
	if (c)
		return c;
	if (a->port < b->port)
		return -1;
	if (a->port > b->port)
		return 1;
	return 0;
}

DECLARE_SORTLIST_UNIQ(bmp_listeners, struct bmp_listener, bli,
		      bmp_listener_cmp);

static void bmp_listener_put(struct bmp_listener *bl)
{
	bmp_listeners_del(&bl->targets->listeners, bl);
	XFREE(MTYPE_BMP_LISTENER, bl);
}

static int bmp_targets_cmp(const struct bmp_targets *a,
			   const struct bmp_targets *b)
{
	return strcmp(a->name, b->name);
}

DECLARE_SORTLIST_UNIQ(bmp_targets, struct bmp_targets, bti, bmp_targets_cmp);

DECLARE_LIST(bmp_session, struct bmp, bsi);

DECLARE_DLIST(bmp_qlist, struct bmp_queue_entry, bli);

static int bmp_qhash_cmp(const struct bmp_queue_entry *a,
		const struct bmp_queue_entry *b)
{
	int ret;
	if (a->afi == AFI_L2VPN && a->safi == SAFI_EVPN && b->afi == AFI_L2VPN
	    && b->safi == SAFI_EVPN) {
		ret = prefix_cmp(&a->rd, &b->rd);
		if (ret)
			return ret;
	} else if (a->afi == AFI_L2VPN && a->safi == SAFI_EVPN)
		return 1;
	else if (b->afi == AFI_L2VPN && b->safi == SAFI_EVPN)
		return -1;

	if (a->afi == b->afi && a->safi == SAFI_MPLS_VPN &&
	    b->safi == SAFI_MPLS_VPN) {
		ret = prefix_cmp(&a->rd, &b->rd);
		if (ret)
			return ret;
	} else if (a->safi == SAFI_MPLS_VPN)
		return 1;
	else if (b->safi == SAFI_MPLS_VPN)
		return -1;

	ret = prefix_cmp(&a->p, &b->p);
	if (ret)
		return ret;
	ret = memcmp(&a->peerid, &b->peerid,
			offsetof(struct bmp_queue_entry, refcount) -
			offsetof(struct bmp_queue_entry, peerid));
	return ret;
}

static uint32_t bmp_qhash_hkey(const struct bmp_queue_entry *e)
{
	uint32_t key;

	key = prefix_hash_key((void *)&e->p);
	key = jhash(&e->peerid,
		    offsetof(struct bmp_queue_entry, refcount)
			    - offsetof(struct bmp_queue_entry, peerid),
		    key);
	if ((e->afi == AFI_L2VPN && e->safi == SAFI_EVPN) ||
	    (e->safi == SAFI_MPLS_VPN))
		key = jhash(&e->rd,
			    offsetof(struct bmp_queue_entry, rd)
				    - offsetof(struct bmp_queue_entry, refcount)
				    + PSIZE(e->rd.prefixlen),
			    key);
	key = jhash(&e->addpath_id, sizeof(uint32_t), key);

	return key;
}

DECLARE_HASH(bmp_qhash, struct bmp_queue_entry, bhi,
		bmp_qhash_cmp, bmp_qhash_hkey);

static int bmp_active_cmp(const struct bmp_active *a,
		const struct bmp_active *b)
{
	int c;

	c = strcmp(a->hostname, b->hostname);
	if (c)
		return c;
	if (a->port < b->port)
		return -1;
	if (a->port > b->port)
		return 1;
	return 0;
}

DECLARE_SORTLIST_UNIQ(bmp_actives, struct bmp_active, bai, bmp_active_cmp);

static struct bmp *bmp_new(struct bmp_targets *bt, int bmp_sock)
{
	struct bmp *new = XCALLOC(MTYPE_BMP_CONN, sizeof(struct bmp));
	afi_t afi;
	safi_t safi;

	monotime(&new->t_up);
	new->targets = bt;
	new->socket = bmp_sock;
	new->syncafi = AFI_MAX;

	FOREACH_AFI_SAFI (afi, safi) {
		new->afistate[afi][safi] = bt->afimon[afi][safi]
			? BMP_AFI_NEEDSYNC : BMP_AFI_INACTIVE;
	}

	bmp_session_add_tail(&bt->sessions, new);
	return new;
}

static void bmp_free(struct bmp *bmp)
{
	bmp_session_del(&bmp->targets->sessions, bmp);
	XFREE(MTYPE_BMP_CONN, bmp);
}

#define BMP_PEER_TYPE_GLOBAL_INSTANCE 0
#define BMP_PEER_TYPE_RD_INSTANCE 1
#define BMP_PEER_TYPE_LOCAL_INSTANCE 2
#define BMP_PEER_TYPE_LOC_RIB_INSTANCE 3

/* compute peer distinguisher from bmp session, afi and peer_type
 * store it in result_ref
 * returns 1 on error that needs message discarding
 * 	   0 if successful
 */
static inline int bmp_get_peer_distinguisher(struct bmp *bmp, afi_t afi,
					     uint8_t peer_type,
					     uint64_t *result_ref)
{

	/* remove this check when the other peer types get correct peer dist.
	 *(RFC7854) impl.
	 * for now, always return no error and 0 peer distinguisher as before
	 */
	if (peer_type != BMP_PEER_TYPE_LOC_RIB_INSTANCE)
		return (int)(*result_ref = 0);

	struct bgp *bgp = bmp->targets->bgp;

	// vrf default => ok, distinguisher 0
	if (bgp->inst_type == VRF_DEFAULT)
		return (int)(*result_ref = 0);

	// use RD if set in VRF config for this AFI
	struct prefix_rd *prd = &bgp->vpn_policy[afi].tovpn_rd;
	if (CHECK_FLAG(bgp->vpn_policy[afi].flags,
		       BGP_VPN_POLICY_TOVPN_RD_SET)) {
		memcpy(result_ref, prd->val, sizeof(prd->val));
		return 0;
	}

	// VRF has no id => error => message should be skipped
	if (bgp->vrf_id == VRF_UNKNOWN)
		return 1;

	// use VRF id converted to ::vrf_id 64bits format
	*result_ref = ((uint64_t)htonl(bgp->vrf_id)) << 32;
	return 0;
}

/* add common header to the stream */
static void bmp_common_hdr(struct stream *s, uint8_t ver, uint8_t type)
{
	stream_putc(s, ver);
	stream_putl(s, 0); //dummy message length. will be set later.
	stream_putc(s, type);
}

/* add per-peer header to the stream */
static void bmp_per_peer_hdr(struct stream *s, struct bgp *bgp,
			     struct peer *peer, uint8_t flags,
			     uint8_t peer_type_flag,
			     uint64_t peer_distinguisher,
			     const struct timeval *tv)
{
#define BMP_PEER_FLAG_V (1 << 7)
#define BMP_PEER_FLAG_L (1 << 6)
#define BMP_PEER_FLAG_A (1 << 5)
#define BMP_PEER_FLAG_O (1 << 4)

	bool is_locrib = peer_type_flag == BMP_PEER_TYPE_LOC_RIB_INSTANCE;

	/* Peer Type */
	stream_putc(s, peer_type_flag);

	/* Peer Flags */
	if (peer->su.sa.sa_family == AF_INET6)
		SET_FLAG(flags, BMP_PEER_FLAG_V);
	else
		UNSET_FLAG(flags, BMP_PEER_FLAG_V);
	stream_putc(s, flags);

	/* Peer Distinguisher */
	stream_put(s, (uint8_t *)&peer_distinguisher, 8);

	/* Peer Address */
	/* Set to 0 if it's a LOC-RIB INSTANCE (RFC 9069) or if it's not an
	 * IPv4/6 address
	 */
	if (is_locrib || (peer->su.sa.sa_family != AF_INET6 &&
			  peer->su.sa.sa_family != AF_INET)) {
		stream_putl(s, 0);
		stream_putl(s, 0);
		stream_putl(s, 0);
		stream_putl(s, 0);
	} else if (peer->su.sa.sa_family == AF_INET6)
		stream_put(s, &peer->su.sin6.sin6_addr, 16);
	else if (peer->su.sa.sa_family == AF_INET) {
		stream_putl(s, 0);
		stream_putl(s, 0);
		stream_putl(s, 0);
		stream_put_in_addr(s, &peer->su.sin.sin_addr);
	}

	/* Peer AS */
	/* set peer ASN but for LOC-RIB INSTANCE (RFC 9069) put the local bgp
	 * ASN if available or 0
	 */
	as_t asn = !is_locrib ? peer->as : bgp ? bgp->as : 0L;

	stream_putl(s, asn);

	/* Peer BGP ID */
	/* set router-id but for LOC-RIB INSTANCE (RFC 9069) put the instance
	 * router-id if available or 0
	 */
	struct in_addr *bgp_id =
		!is_locrib ? &peer->remote_id : bgp ? &bgp->router_id : NULL;

	stream_put_in_addr(s, bgp_id);

	/* Timestamp */
	if (tv) {
		stream_putl(s, tv->tv_sec);
		stream_putl(s, tv->tv_usec);
	} else {
		stream_putl(s, 0);
		stream_putl(s, 0);
	}
}

/* add a bmpv4 tlv header without an index field */
inline static void bmp_put_info_tlv_hdr(struct stream *s, uint16_t type,
					uint16_t len)
{
	stream_putw(s, type);
	stream_putw(s, len);
}

/* add a bmpv4 tlv header with an index field */
inline static void bmp_put_info_tlv_hdr_with_index(struct stream *s, uint16_t type,
					uint16_t len, uint16_t index)
{
	bmp_put_info_tlv_hdr(s, type, len);
	stream_putw(s, index);
}

/* add a CAPABILITY TLV containing for the capability 'type'
 */
__attribute__((__unused__))
inline static void bmp_put_info_tlv_capability(struct stream *s, uint16_t type) {

	bmp_put_info_tlv_hdr(s, type, BMP_INFO_LENGTH_CAPABILITY_SIZE);
	stream_putc(s, 1);
}

/* add a GROUP TLV containing reference to 'count' NLRIs for found in 'indices'
 */
__attribute__((__unused__))
inline static void bmp_put_info_tlv_group(struct stream *s, uint16_t *indices, int count) {

	bmp_put_info_tlv_hdr_with_index(s, BMP_INFO_TYPE_GROUP,
			     count * BMP_INFO_LENGTH_GROUP_ITEM_SIZE, 0);
	stream_put(s, indices, count * sizeof(*indices));
}

/* put a string tlv with type 'type' to the stream */
static void bmp_put_info_tlv_str(struct stream *s, uint16_t type,
		const char *string, uint16_t maxlen)
{
	uint16_t len = (uint16_t) strlen(string);
	len = maxlen > 0 && len > maxlen ? maxlen : len;
	bmp_put_info_tlv_hdr(s, type, len);
	stream_put(s, string, len);
}

/* put the vrf table name of the bgp instance bmp is bound to in a tlv on the
 * stream */
static void __attribute__((unused))
bmp_put_info_tlv_vrftablename_with_index(struct stream *s, struct bgp *bgp,
					 uint16_t index) {

	const char *vrftablename = "global";
	if (bgp->inst_type != BGP_INSTANCE_TYPE_DEFAULT) {
		struct vrf *vrf = vrf_lookup_by_id(bgp->vrf_id);

		vrftablename = vrf ? vrf->name : NULL;
	}
	if (vrftablename != NULL) {
		size_t len = strlen(vrftablename);
		len = len > BMP_INFO_LENGTH_TABLE_NAME_MAX ? BMP_INFO_LENGTH_TABLE_NAME_MAX : len;
		bmp_put_info_tlv_hdr_with_index(s, BMP_INFO_TYPE_VRFTABLENAME,
						len, index);
		stream_put(s, vrftablename, len);
	}
}

static void __attribute__((unused))
bmp_put_info_tlv_vrftablename(struct stream *s, struct bgp *bgp)
{

	const char *vrftablename = "global";
	if (bgp->inst_type != BGP_INSTANCE_TYPE_DEFAULT) {

		struct vrf *vrf = vrf_lookup_by_id(bgp->vrf_id);

		vrftablename = vrf ? vrf->name : NULL;
	}
	if (vrftablename != NULL)
		bmp_put_info_tlv_str(s, BMP_INFO_TYPE_VRFTABLENAME,
				     vrftablename, BMP_INFO_LENGTH_TABLE_NAME_MAX);
}

enum bmp_path_status_code {
	bmp_path_status_reserved	= 0x00000000,
	bmp_path_status_invalid		= 0x00000001,
	bmp_path_status_best		= 0x00000002,
	bmp_path_status_non_selected	= 0x00000004,
	bmp_path_status_primary		= 0x00000008,
	bmp_path_status_backup		= 0x00000010,
	bmp_path_status_non_installed	= 0x00000020,
	bmp_path_status_best_external	= 0x00000040,
	bmp_path_status_addpath		= 0x00000080,
	bmp_path_status_filtered_in	= 0x00000100,
	bmp_path_status_filtered_out	= 0x00000200,
	bmp_path_status_invalid_rov	= 0x00000400,
};

enum bmp_path_status_reason_code {

	bmp_path_status_reason_unkown = 0x0000,
	bmp_path_status_reason_invalid_for_AS_loop			= 0x0006,
	bmp_path_status_reason_invalid_for_unresolvable_nexthop        	= 0x0007,
	bmp_path_status_reason_not_preferred_for_Local_preference      	= 0x0016,
	bmp_path_status_reason_not_preferred_for_AS_Path_Length        	= 0x0014,
	bmp_path_status_reason_not_preferred_for_origin                	= 0x0013,
	bmp_path_status_reason_not_preferred_for_MED                   	= 0x0012,
	bmp_path_status_reason_not_preferred_for_peer_type             	= 0x0011,
	bmp_path_status_reason_not_preferred_for_IGP_cost              	= 0x0010,
	bmp_path_status_reason_not_preferred_for_router_ID             	= 0x000E,
	bmp_path_status_reason_not_preferred_for_peer_address          	= 0x000D,
	bmp_path_status_reason_not_preferred_for_AIGP	                = 0x0020
};

enum bgp_reason_type {
	bgp_path_selection_reason,
	bgp_inbound_filtered_reason,
};

struct bgp_reason {
	enum bgp_reason_type type;
	union {
		enum bgp_path_selection_reason select_reason;
		enum bgp_inbound_filtered_reason inbound_reason;
	};
};

static inline struct bgp_reason
bmp_make_bgp_reason_select(struct bgp_dest* dest) {
	return (struct bgp_reason) {
		.type = bgp_path_selection_reason,
		.select_reason = dest ? dest->reason : bgp_path_selection_none,
	};
}

static inline struct bgp_reason
bmp_make_bgp_reason_inbound(struct bgp_adj_in* adjin) {
	return (struct bgp_reason) {
		.type = bgp_inbound_filtered_reason,
		.inbound_reason = adjin ? adjin->reason : bgp_inbound_filtered_none,
	};
}

struct bmp_path_status {

	enum bmp_path_status_code status_code;
	enum bmp_path_status_reason_code reason_code;
};

static enum bmp_path_status_code bmp_path_status_get_status(uint32_t bpi_flags, uint16_t dest_flags) {

	enum bmp_path_status_code status = bmp_path_status_reserved;

	if (!CHECK_FLAG(bpi_flags, BGP_PATH_VALID))
		status |= bmp_path_status_invalid;

	if (CHECK_FLAG(bpi_flags, BGP_PATH_SELECTED))
		status |= bmp_path_status_primary | bmp_path_status_best;

	if (CHECK_FLAG(bpi_flags, BGP_PATH_MULTIPATH))
		status |= bmp_path_status_addpath | bmp_path_status_primary;

	if (!CHECK_FLAG(bpi_flags, BGP_PATH_SELECTED | BGP_PATH_MULTIPATH))
		status |= bmp_path_status_non_selected;

	if (!(CHECK_FLAG(bpi_flags, BGP_PATH_SELECTED | BGP_PATH_MULTIPATH)
	    && CHECK_FLAG(dest_flags, BGP_NODE_FIB_INSTALLED)))
		status |= bmp_path_status_non_installed;


	// TODO
	// 0x00000040 bmp_path_status_best_external (NEED IMPLEMENT)
	// 0x00000400 bmp_path_status_invalid_rov (NO IDEA)
	// 0x00000400 bmp_path_status_backup (NO IDEA)

	return status;
}


static enum bmp_path_status_reason_code
bmp_path_status_get_reason(struct bgp_reason reason)
{

	switch (reason.type) {
	case bgp_path_selection_reason:
		goto select_reason;
	case bgp_inbound_filtered_reason:
		goto filter_in_reason;
	default:
		zlog_warn("%s: Unknown path status reason type!", __func__);
		return bmp_path_status_reason_unkown;
	}

select_reason:
	switch(reason.select_reason) {
	case bgp_path_selection_local_pref:
		return bmp_path_status_reason_not_preferred_for_Local_preference;
	case bgp_path_selection_aigp:
		return bmp_path_status_reason_not_preferred_for_AIGP;
	case bgp_path_selection_confed_as_path:
	case bgp_path_selection_as_path:
		return bmp_path_status_reason_not_preferred_for_AS_Path_Length;
	case bgp_path_selection_origin:
		return bmp_path_status_reason_not_preferred_for_origin;
	case bgp_path_selection_med:
		return bmp_path_status_reason_not_preferred_for_MED;
	case bgp_path_selection_peer:
		return bmp_path_status_reason_not_preferred_for_peer_type;
	case bgp_path_selection_igp_metric:
		return bmp_path_status_reason_not_preferred_for_IGP_cost;
	case bgp_path_selection_router_id:
		return bmp_path_status_reason_not_preferred_for_router_ID;
	case bgp_path_selection_neighbor_ip:
		return bmp_path_status_reason_not_preferred_for_peer_address;
	case bgp_path_selection_none:
	case bgp_path_selection_default:
	case bgp_path_selection_first:
	case bgp_path_selection_older:
	case bgp_path_selection_weight:
	case bgp_path_selection_local_route:
	case bgp_path_selection_stale:
	case bgp_path_selection_confed:
	case bgp_path_selection_cluster_length:
	case bgp_path_selection_local_configured:
	case bgp_path_selection_evpn_sticky_mac:
	case bgp_path_selection_evpn_seq:
	case bgp_path_selection_evpn_local_path:
	case bgp_path_selection_evpn_non_proxy:
	case bgp_path_selection_evpn_lower_ip:
	case bgp_path_selection_accept_own:
	default:
		return bmp_path_status_reason_unkown;
	}

filter_in_reason:
	switch(reason.inbound_reason) {
	case bgp_inbound_filtered_local_as_path_loop:
	case bgp_inbound_filtered_as_path_loop:
	case bgp_inbound_filtered_confed_loop:
	case bgp_inbound_filtered_RR_loop:
		return bmp_path_status_reason_invalid_for_AS_loop;
	case bgp_inbound_filtered_nhs_or_martian:
	case bgp_inbound_filtered_nhs_mac:
		return bmp_path_status_reason_invalid_for_unresolvable_nexthop;
	case bgp_inbound_filtered_none:
	case bgp_inbound_filtered_self_originated:
	case bgp_inbound_filtered_filter_policy:
	case bgp_inbound_filtered_ebgp_requires_policy:
	case bgp_inbound_filtered_reject_as_sets:
	case bgp_inbound_filtered_routemap_policy:
	case bgp_inbound_filtered_otc:
	case bgp_inbound_filtered_maximum_prefix_overflow:
	default:
		return bmp_path_status_reason_unkown;
	}

}


static inline struct bmp_path_status bmp_make_path_status(
	struct bgp_path_info *bpi,
	struct bgp_dest *dest) {

	return (struct bmp_path_status) {
		.status_code = bmp_path_status_get_status(bpi ? bpi->flags : 0,
							  dest ? dest->flags : 0),
		.reason_code = bmp_path_status_get_reason(
			bmp_make_bgp_reason_select(dest)
			),
	};
}

static inline struct bmp_path_status bmp_make_path_status_manual(
	enum bmp_path_status_code status_code,
	enum bmp_path_status_reason_code reason_code) {

	return (struct bmp_path_status) {
		.status_code = status_code,
		.reason_code = reason_code,
	};
}

static inline struct bmp_path_status bmp_make_path_status_adjin(
	struct bgp_adj_in *adjin) {

	return bmp_make_path_status_manual(
		adjin && adjin->filtered ? bmp_path_status_filtered_in
					 : bmp_path_status_reserved,
		bmp_path_status_get_reason(bmp_make_bgp_reason_inbound(adjin)));
}

static inline struct bmp_path_status bmp_make_path_status_adjout(
	struct bgp_path_info *bpi,
	struct bgp_dest *dest,
	struct peer *dest_peer,
	uint32_t addpath_tx_id) {

	struct bmp_path_status path_status = bmp_make_path_status(bpi, dest);
	if (dest && dest_peer
	    && !bgp_adj_out_lookup(dest_peer, dest, addpath_tx_id))
		path_status.status_code |=
				 bmp_path_status_filtered_out;

	return path_status;
}

static void bmp_put_info_tlv_path_status(struct stream *s, struct bmp_path_status status)
{
	bool E_bit = false;
	enum bmp_path_status_code status_code = status.status_code;
	enum bmp_path_status_reason_code reason_code = status.reason_code;

	zlog_info("%s: status is status_code=%"PRIu32"  reason_code=%"PRIu16, __func__, status_code, reason_code);

	bool include_reason = reason_code != bmp_path_status_reason_unkown;

	bmp_put_info_tlv_hdr_with_index(s,
		BMP_INFO_TYPE_PATH_STATUS | (E_bit ? (1 << 15) : 0),
		4 + (include_reason ? 2 : 0), 0);
	stream_putl(s, (uint32_t) status_code);

	if (include_reason)
		stream_putw(s, (uint16_t) reason_code);
}

/* send initiation message */
static int bmp_send_initiation(struct bmp *bmp)
{
	struct stream *s;
	s = stream_new(BGP_MAX_PACKET_SIZE);
	bmp_common_hdr(s, BMP_VERSION_3, BMP_TYPE_INITIATION);

	bmp_put_info_tlv_str(s, BMP_INFO_TYPE_SYSDESCR,
			     FRR_FULL_NAME " " FRR_VER_SHORT, 0);
	bmp_put_info_tlv_str(s, BMP_INFO_TYPE_SYSNAME, cmd_hostname_get(), 0);

	stream_putl_at(s, BMP_LENGTH_POS, stream_get_endp(s)); //message length is set.

	pullwr_write_stream(bmp->pullwr, s);
	stream_free(s);
	return 0;
}

static void bmp_notify_put(struct stream *s, struct bgp_notify *nfy)
{
	size_t len_pos;
	uint8_t marker[16] = {
		0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff,
	};

	stream_put(s, marker, sizeof(marker));
	len_pos = stream_get_endp(s);
	stream_putw(s, 0);
	stream_putc(s, BGP_MSG_NOTIFY);
	stream_putc(s, nfy->code);
	stream_putc(s, nfy->subcode);
	stream_put(s, nfy->data, nfy->length);

	stream_putw_at(s, len_pos, stream_get_endp(s) - len_pos
			+ sizeof(marker));
}

/* send peer up/down for peer based on down boolean value */
static struct stream *bmp_peerstate(struct peer *peer, bool down)
{
	struct stream *s;
	size_t len;
	struct timeval uptime, uptime_real;

	uptime.tv_sec = peer->uptime;
	uptime.tv_usec = 0;
	monotime_to_realtime(&uptime, &uptime_real);

#define BGP_BMP_MAX_PACKET_SIZE	1024
	s = stream_new(BGP_MAX_PACKET_SIZE);

	if (peer_established(peer) && !down) {
		struct bmp_bgp_peer *bbpeer;

		bmp_common_hdr(s, BMP_VERSION_3,
				BMP_TYPE_PEER_UP_NOTIFICATION);
		bmp_per_peer_hdr(s, peer->bgp, peer, 0,
				 BMP_PEER_TYPE_GLOBAL_INSTANCE, 0,
				 &uptime_real);

		/* Local Address (16 bytes) */
		if (peer->su_local->sa.sa_family == AF_INET6)
			stream_put(s, &peer->su_local->sin6.sin6_addr, 16);
		else if (peer->su_local->sa.sa_family == AF_INET) {
			stream_putl(s, 0);
			stream_putl(s, 0);
			stream_putl(s, 0);
			stream_put_in_addr(s, &peer->su_local->sin.sin_addr);
		}

		/* Local Port, Remote Port */
		if (peer->su_local->sa.sa_family == AF_INET6)
			stream_putw(s, peer->su_local->sin6.sin6_port);
		else if (peer->su_local->sa.sa_family == AF_INET)
			stream_putw(s, peer->su_local->sin.sin_port);
		if (peer->su_remote->sa.sa_family == AF_INET6)
			stream_putw(s, peer->su_remote->sin6.sin6_port);
		else if (peer->su_remote->sa.sa_family == AF_INET)
			stream_putw(s, peer->su_remote->sin.sin_port);

		static const uint8_t dummy_open[] = {
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			0x00, 0x13, 0x01,
		};

		bbpeer = bmp_bgp_peer_find(peer->qobj_node.nid);

		if (bbpeer && bbpeer->open_tx)
			stream_put(s, bbpeer->open_tx, bbpeer->open_tx_len);
		else {
			stream_put(s, dummy_open, sizeof(dummy_open));
			zlog_warn("bmp: missing TX OPEN message for peer %s",
				  peer->host);
		}
		if (bbpeer && bbpeer->open_rx)
			stream_put(s, bbpeer->open_rx, bbpeer->open_rx_len);
		else {
			stream_put(s, dummy_open, sizeof(dummy_open));
			zlog_warn("bmp: missing RX OPEN message for peer %s",
				  peer->host);
		}

		if (peer->desc)
			bmp_put_info_tlv_str(s, 0, peer->desc, 0);
	} else {
		uint8_t type;
		size_t type_pos;

		bmp_common_hdr(s, BMP_VERSION_3,
				BMP_TYPE_PEER_DOWN_NOTIFICATION);
		bmp_per_peer_hdr(s, peer->bgp, peer, 0,
				 BMP_PEER_TYPE_GLOBAL_INSTANCE, 0,
				 &uptime_real);

		type_pos = stream_get_endp(s);
		stream_putc(s, 0);	/* placeholder for down reason */

		switch (peer->last_reset) {
		case PEER_DOWN_NOTIFY_RECEIVED:
			type = BMP_PEERDOWN_REMOTE_NOTIFY;
			bmp_notify_put(s, &peer->notify);
			break;
		case PEER_DOWN_CLOSE_SESSION:
			type = BMP_PEERDOWN_REMOTE_CLOSE;
			break;
		case PEER_DOWN_WAITING_NHT:
			type = BMP_PEERDOWN_LOCAL_FSM;
			stream_putw(s, BGP_FSM_TcpConnectionFails);
			break;
		/*
		 * TODO: Map remaining PEER_DOWN_* reasons to RFC event codes.
		 * TODO: Implement BMP_PEERDOWN_LOCAL_NOTIFY.
		 *
		 * See RFC7854 ss. 4.9
		 */
		default:
			type = BMP_PEERDOWN_LOCAL_FSM;
			stream_putw(s, BMP_PEER_DOWN_NO_RELEVANT_EVENT_CODE);
			break;
		}
		stream_putc_at(s, type_pos, type);
	}

	len = stream_get_endp(s);
	stream_putl_at(s, BMP_LENGTH_POS, len); //message length is set.
	return s;
}

/* send a peer up for each bgp peer of the bgp instance bmp is bound to */
static int bmp_send_peerup(struct bmp *bmp)
{
	struct peer *peer;
	struct listnode *node;
	struct stream *s;

	/* Walk down all peers */
	for (ALL_LIST_ELEMENTS_RO(bmp->targets->bgp->peer, node, peer)) {
		s = bmp_peerstate(peer, false);
		pullwr_write_stream(bmp->pullwr, s);
		stream_free(s);
	}

	return 0;
}

/* send a stream to all bmp sessions configured in a bgp instance */
/* XXX: kludge - filling the pullwr's buffer */
static void bmp_send_all(struct bmp_bgp *bmpbgp, struct stream *s)
{
	struct bmp_targets *bt;
	struct bmp *bmp;

	frr_each(bmp_targets, &bmpbgp->targets, bt)
		frr_each(bmp_session, &bt->sessions, bmp)
			pullwr_write_stream(bmp->pullwr, s);
	stream_free(s);
}

/*
 * Route Mirroring
 */

#define BMP_MIRROR_TLV_TYPE_BGP_MESSAGE 0
#define BMP_MIRROR_TLV_TYPE_INFO        1

#define BMP_MIRROR_INFO_CODE_ERRORPDU   0
#define BMP_MIRROR_INFO_CODE_LOSTMSGS   1

/* pull a queue item from the bmp mirroring queue */
static struct bmp_mirrorq *bmp_pull_mirror(struct bmp *bmp)
{
	struct bmp_mirrorq *bmq;

	bmq = bmp->mirrorpos;
	if (!bmq)
		return NULL;

	bmp->mirrorpos = bmp_mirrorq_next(&bmp->targets->bmpbgp->mirrorq, bmq);

	bmq->refcount--;
	if (!bmq->refcount) {
		bmp->targets->bmpbgp->mirror_qsize -= sizeof(*bmq) + bmq->len;
		bmp_mirrorq_del(&bmp->targets->bmpbgp->mirrorq, bmq);
	}
	return bmq;
}

static void bmp_mirror_cull(struct bmp_bgp *bmpbgp)
{
	while (bmpbgp->mirror_qsize > bmpbgp->mirror_qsizelimit) {
		struct bmp_mirrorq *bmq, *inner;
		struct bmp_targets *bt;
		struct bmp *bmp;

		bmq = bmp_mirrorq_first(&bmpbgp->mirrorq);

		frr_each(bmp_targets, &bmpbgp->targets, bt) {
			if (!bt->mirror)
				continue;
			frr_each(bmp_session, &bt->sessions, bmp) {
				if (bmp->mirrorpos != bmq)
					continue;

				while ((inner = bmp_pull_mirror(bmp))) {
					if (!inner->refcount)
						XFREE(MTYPE_BMP_MIRRORQ,
								inner);
				}

				zlog_warn("bmp[%s] lost mirror messages due to buffer size limit",
						bmp->remote);
				bmp->mirror_lost = true;
				pullwr_bump(bmp->pullwr);
			}
		}
	}
}

/* queue a bmp mirror queue item for a peer */
static int bmp_mirror_packet(struct peer *peer, uint8_t type, bgp_size_t size,
		struct stream *packet)
{
	struct bmp_bgp *bmpbgp = bmp_bgp_find(peer->bgp);
	struct timeval tv;
	struct bmp_mirrorq *qitem;
	struct bmp_targets *bt;
	struct bmp *bmp;

	frrtrace(3, frr_bgp, bmp_mirror_packet, peer, type, packet);

	gettimeofday(&tv, NULL);

	if (type == BGP_MSG_OPEN) {
		struct bmp_bgp_peer *bbpeer = bmp_bgp_peer_get(peer);

		XFREE(MTYPE_BMP_OPEN, bbpeer->open_rx);

		bbpeer->open_rx_len = size;
		bbpeer->open_rx = XMALLOC(MTYPE_BMP_OPEN, size);
		memcpy(bbpeer->open_rx, packet->data, size);
	}

	if (!bmpbgp)
		return 0;

	qitem = XCALLOC(MTYPE_BMP_MIRRORQ, sizeof(*qitem) + size);
	qitem->peerid = peer->qobj_node.nid;
	qitem->tv = tv;
	qitem->len = size;
	memcpy(qitem->data, packet->data, size);

	frr_each(bmp_targets, &bmpbgp->targets, bt) {
		if (!bt->mirror)
			continue;
		frr_each(bmp_session, &bt->sessions, bmp) {
			qitem->refcount++;
			if (!bmp->mirrorpos)
				bmp->mirrorpos = qitem;
			pullwr_bump(bmp->pullwr);
		}
	}
	if (qitem->refcount == 0)
		XFREE(MTYPE_BMP_MIRRORQ, qitem);
	else {
		bmpbgp->mirror_qsize += sizeof(*qitem) + size;
		bmp_mirrorq_add_tail(&bmpbgp->mirrorq, qitem);

		bmp_mirror_cull(bmpbgp);

		bmpbgp->mirror_qsizemax = MAX(bmpbgp->mirror_qsizemax,
				bmpbgp->mirror_qsize);
	}
	return 0;
}

/* send a bmp mirror lost message */
static void bmp_wrmirror_lost(struct bmp *bmp, struct pullwr *pullwr)
{
	struct stream *s;
	struct timeval tv;

	gettimeofday(&tv, NULL);

	s = stream_new(BGP_MAX_PACKET_SIZE);

	bmp_common_hdr(s, BMP_VERSION_3, BMP_TYPE_ROUTE_MIRRORING);
	bmp_per_peer_hdr(s, bmp->targets->bgp, bmp->targets->bgp->peer_self, 0,
			 BMP_PEER_TYPE_GLOBAL_INSTANCE, 0, &tv);

	stream_putw(s, BMP_MIRROR_TLV_TYPE_INFO);
	stream_putw(s, 2);
	stream_putw(s, BMP_MIRROR_INFO_CODE_LOSTMSGS);
	stream_putl_at(s, BMP_LENGTH_POS, stream_get_endp(s));

	bmp->cnt_mirror_overruns++;
	pullwr_write_stream(bmp->pullwr, s);
	stream_free(s);
}


/* pulls a bmq and sends a bmp mirror message on the session
 */
static bool bmp_wrmirror(struct bmp *bmp, struct pullwr *pullwr)
{
	struct bmp_mirrorq *bmq;
	struct peer *peer;
	bool written = false;

	if (bmp->mirror_lost) {
		bmp_wrmirror_lost(bmp, pullwr);
		bmp->mirror_lost = false;
		return true;
	}

	bmq = bmp_pull_mirror(bmp);
	if (!bmq)
		return false;

	peer = QOBJ_GET_TYPESAFE(bmq->peerid, peer);
	if (!peer) {
		zlog_info("bmp: skipping mirror message for deleted peer");
		goto out;
	}

	struct stream *s;
	s = stream_new(BGP_MAX_PACKET_SIZE);

	bmp_common_hdr(s, BMP_VERSION_3, BMP_TYPE_ROUTE_MIRRORING);
	bmp_per_peer_hdr(s, bmp->targets->bgp, peer, 0,
			 BMP_PEER_TYPE_GLOBAL_INSTANCE, 0, &bmq->tv);

	/* BMP Mirror TLV. */
	stream_putw(s, BMP_MIRROR_TLV_TYPE_BGP_MESSAGE);
	stream_putw(s, bmq->len);
	stream_putl_at(s, BMP_LENGTH_POS, stream_get_endp(s) + bmq->len);

	bmp->cnt_mirror++;
	pullwr_write_stream(bmp->pullwr, s);
	pullwr_write(bmp->pullwr, bmq->data, bmq->len);

	stream_free(s);
	written = true;

out:
	if (!bmq->refcount)
		XFREE(MTYPE_BMP_MIRRORQ, bmq);
	return written;
}


/* triggered when a bgp packet is sent
 * saves the packet if the packet was a bgp open
 */
static int bmp_outgoing_packet(struct peer *peer, uint8_t type, bgp_size_t size,
		struct stream *packet)
{
	if (type == BGP_MSG_OPEN) {
		frrtrace(2, frr_bgp, bmp_update_saved_open, peer, packet);

		struct bmp_bgp_peer *bbpeer = bmp_bgp_peer_get(peer);

		XFREE(MTYPE_BMP_OPEN, bbpeer->open_tx);

		bbpeer->open_tx_len = size;
		bbpeer->open_tx = XMALLOC(MTYPE_BMP_OPEN, size);
		memcpy(bbpeer->open_tx, packet->data, size);
	}
	return 0;
}


/* triggered when a bgp peer goes up
 * sends a bmp peer up to all bmp peers and saves the bgp open packet
 */
static int bmp_peer_status_changed(struct peer *peer)
{
	struct bmp_bgp *bmpbgp = bmp_bgp_find(peer->bgp);
	struct bmp_bgp_peer *bbpeer, *bbdopp;

	frrtrace(1, frr_bgp, bmp_peer_status_changed, peer);

	if (!bmpbgp)
		return 0;

	if (peer->status == Deleted) {
		bbpeer = bmp_bgp_peer_find(peer->qobj_node.nid);
		if (bbpeer) {
			XFREE(MTYPE_BMP_OPEN, bbpeer->open_rx);
			XFREE(MTYPE_BMP_OPEN, bbpeer->open_tx);
			bmp_peerh_del(&bmp_peerh, bbpeer);
			XFREE(MTYPE_BMP_PEER, bbpeer);
		}
		return 0;
	}

	/* Check if this peer just went to Established */
	if ((peer->ostatus != OpenConfirm) || !(peer_established(peer)))
		return 0;

	if (peer->doppelganger && (peer->doppelganger->status != Deleted)) {
		bbpeer = bmp_bgp_peer_get(peer);
		bbdopp = bmp_bgp_peer_find(peer->doppelganger->qobj_node.nid);
		if (bbdopp) {
			XFREE(MTYPE_BMP_OPEN, bbpeer->open_tx);
			XFREE(MTYPE_BMP_OPEN, bbpeer->open_rx);

			bbpeer->open_tx = bbdopp->open_tx;
			bbpeer->open_tx_len = bbdopp->open_tx_len;
			bbpeer->open_rx = bbdopp->open_rx;
			bbpeer->open_rx_len = bbdopp->open_rx_len;

			bmp_peerh_del(&bmp_peerh, bbdopp);
			XFREE(MTYPE_BMP_PEER, bbdopp);
		}
	}

	bmp_send_all(bmpbgp, bmp_peerstate(peer, false));
	return 0;
}

/* triggered when a bgp peer goes down
 * sends a bmp peer down to all bmp peers and free the saved bgp open packet
 */
static int bmp_peer_backward(struct peer *peer)
{
	struct bmp_bgp *bmpbgp = bmp_bgp_find(peer->bgp);
	struct bmp_bgp_peer *bbpeer;

	frrtrace(1, frr_bgp, bmp_peer_backward_transition, peer);

	if (!bmpbgp)
		return 0;

	bbpeer = bmp_bgp_peer_find(peer->qobj_node.nid);
	if (bbpeer) {
		XFREE(MTYPE_BMP_OPEN, bbpeer->open_tx);
		bbpeer->open_tx_len = 0;
		XFREE(MTYPE_BMP_OPEN, bbpeer->open_rx);
		bbpeer->open_rx_len = 0;
	}

	bmp_send_all(bmpbgp, bmp_peerstate(peer, true));
	return 0;
}


/* sends a bmp end-of-rib on the bmp session for the given afi/safi
 * for each peer
 */
static void bmp_eor(struct bmp *bmp, afi_t afi, safi_t safi, uint8_t flags,
		    uint8_t peer_type_flag)
{
	struct peer *peer;
	struct listnode *node;
	struct stream *s, *s2;
	iana_afi_t pkt_afi = IANA_AFI_IPV4;
	iana_safi_t pkt_safi = IANA_SAFI_UNICAST;

	frrtrace(3, frr_bgp, bmp_eor, afi, safi, flags, peer_type_flag);

	s = stream_new(BGP_MAX_PACKET_SIZE);

	/* wrap in tlv and record header length */
	size_t tlv_hdr_pos_before = stream_get_endp(s);
	bmp_put_info_tlv_hdr_with_index(s, BMP_INFO_TYPE_BGP_PDU, 0, 0);
	size_t tlv_hdr_pos_after = stream_get_endp(s);

	/* Make BGP update packet. */
	bgp_packet_set_marker(s, BGP_MSG_UPDATE);

	/* Unfeasible Routes Length */
	stream_putw(s, 0);

	if (afi == AFI_IP && safi == SAFI_UNICAST) {
		/* Total Path Attribute Length */
		stream_putw(s, 0);
	} else {
		/* Convert AFI, SAFI to values for packet. */
		bgp_map_afi_safi_int2iana(afi, safi, &pkt_afi, &pkt_safi);

		/* Total Path Attribute Length */
		stream_putw(s, 6);
		stream_putc(s, BGP_ATTR_FLAG_OPTIONAL);
		stream_putc(s, BGP_ATTR_MP_UNREACH_NLRI);
		stream_putc(s, 3);
		stream_putw(s, pkt_afi);
		stream_putc(s, pkt_safi);
	}

	/* set bgp packet size ignoring tlv header size */
	bgp_packet_set_size_with_offset(s, tlv_hdr_pos_after);
	/* set tlv length  */
	stream_putw_at(s, tlv_hdr_pos_before + BMP_INFO_OFFSET_TLV_LENGTH, stream_get_endp(s) - tlv_hdr_pos_after);

	for (ALL_LIST_ELEMENTS_RO(bmp->targets->bgp->peer, node, peer)) {
		if (!peer->afc_nego[afi][safi])
			continue;

		uint64_t peer_distinguisher = 0;
		// skip this message if peer distinguisher is not available
		if (bmp_get_peer_distinguisher(bmp, afi, peer_type_flag,
					       &peer_distinguisher)) {
			zlog_debug(
				"skipping bmp message for reason: can't get peer distinguisher");
			continue;
		}

		s2 = stream_new(BGP_MAX_PACKET_SIZE);

		bmp_common_hdr(s2, BMP_VERSION_3,
				BMP_TYPE_ROUTE_MONITORING);

		bmp_per_peer_hdr(s2, bmp->targets->bgp, peer, flags,
				 peer_type_flag, peer_distinguisher, NULL);

		stream_putl_at(s2, BMP_LENGTH_POS,
				stream_get_endp(s) + stream_get_endp(s2));

		bmp->cnt_update++;
		pullwr_write_stream(bmp->pullwr, s2);
		pullwr_write_stream(bmp->pullwr, s);
		stream_free(s2);
	}
	stream_free(s);
}

/* makes a bgp update to be embedded in a bmp monitoring message
 */
static struct stream *bmp_update(const struct prefix *p, struct prefix_rd *prd,
				 uint32_t addpath_id, struct peer *peer,
				 struct attr *attr, afi_t afi, safi_t safi)
{
	struct bpacket_attr_vec_arr vecarr;
	struct stream *s;
	size_t attrlen_pos = 0, mpattrlen_pos = 0, tlv_hdr_pos_before = 0, tlv_hdr_pos_after = 0;
	bgp_size_t total_attr_len = 0;

	bpacket_attr_vec_arr_reset(&vecarr);

	s = stream_new(BGP_MAX_PACKET_SIZE);

	/* wrap in tlv and record header length */
	tlv_hdr_pos_before = stream_get_endp(s);
	bmp_put_info_tlv_hdr_with_index(s, BMP_INFO_TYPE_BGP_PDU, 0, 0);
	tlv_hdr_pos_after = stream_get_endp(s);

	bgp_packet_set_marker(s, BGP_MSG_UPDATE);

	/* 2: withdrawn routes length */
	stream_putw(s, 0);

	/* 3: total attributes length - attrlen_pos stores the position */
	attrlen_pos = stream_get_endp(s);
	stream_putw(s, 0);

	/* 5: Encode all the attributes, except MP_REACH_NLRI attr. */
	total_attr_len =
		bgp_packet_attribute(NULL, peer, s, attr, &vecarr, NULL, afi,
				     safi, peer, NULL, NULL, 0, 1, addpath_id, NULL);

	/* space check? */

	/* peer_cap_enhe & add-path removed */
	if (afi == AFI_IP && safi == SAFI_UNICAST)
		stream_put_prefix_addpath(s, p, 1, addpath_id);
	else {
		size_t p1 = stream_get_endp(s);

		/* MPLS removed for now */

		mpattrlen_pos = bgp_packet_mpattr_start(s, peer, afi, safi,
				&vecarr, attr);
		bgp_packet_mpattr_prefix(s, afi, safi, p, prd, NULL, 0, 1, addpath_id,
					 attr);
		bgp_packet_mpattr_end(s, mpattrlen_pos);
		total_attr_len += stream_get_endp(s) - p1;
	}
	/* set the total attribute length correctly */
	stream_putw_at(s, attrlen_pos, total_attr_len);
	/* set bgp packet size ignoring tlv header size */
	bgp_packet_set_size_with_offset(s, tlv_hdr_pos_after);
	/* set tlv length  */
	stream_putw_at(s, tlv_hdr_pos_before + BMP_INFO_OFFSET_TLV_LENGTH, stream_get_endp(s) - tlv_hdr_pos_after);

	return s;
}

/* makes a bgp withdraw to be embedded in a bmp monitoring message
 */
static struct stream *bmp_withdraw(const struct prefix *p,
				   struct prefix_rd *prd, uint32_t addpath_id,
				   afi_t afi, safi_t safi)
{
	struct stream *s;
	size_t attrlen_pos = 0, mp_start, mplen_pos, tlv_hdr_pos_before = 0, tlv_hdr_pos_after = 0;
	bgp_size_t total_attr_len = 0;
	bgp_size_t unfeasible_len;

	s = stream_new(BGP_MAX_PACKET_SIZE);
	tlv_hdr_pos_before = stream_get_endp(s);
	bmp_put_info_tlv_hdr_with_index(s, BMP_INFO_TYPE_BGP_PDU, 0, 0);
	tlv_hdr_pos_after = stream_get_endp(s);


	/* wrap in tlv and record header length */

	tlv_hdr_pos_before = stream_get_endp(s);
	bmp_put_info_tlv_hdr_with_index(s, BMP_INFO_TYPE_BGP_PDU, 0, 0);
	tlv_hdr_pos_after = stream_get_endp(s);

	bgp_packet_set_marker(s, BGP_MSG_UPDATE);

	/* withdraw length temporary size */
	size_t withdraw_len_pos = stream_get_endp(s);
	stream_putw(s, 0);

	if (afi == AFI_IP && safi == SAFI_UNICAST) {
		size_t unfeasible_start = stream_get_endp(s);
		stream_put_prefix_addpath(s, p, 1, addpath_id);
		unfeasible_len = stream_get_endp(s) - unfeasible_start;
		stream_putw_at(s, withdraw_len_pos, unfeasible_len);
		stream_putw(s, 0);
	} else {
		attrlen_pos = stream_get_endp(s);
		/* total attr length = 0 for now. reevaluate later */
		stream_putw(s, 0);
		mp_start = stream_get_endp(s);
		mplen_pos = bgp_packet_mpunreach_start(s, afi, safi);

		bgp_packet_mpunreach_prefix(s, p, afi, safi, prd, NULL, 0, 1, addpath_id,
					    NULL);
		/* Set the mp_unreach attr's length */
		bgp_packet_mpunreach_end(s, mplen_pos);

		/* Set total path attribute length. */
		total_attr_len = stream_get_endp(s) - mp_start;
		stream_putw_at(s, attrlen_pos, total_attr_len);
	}

	/* set bgp packet size ignoring tlv header size */
	bgp_packet_set_size_with_offset(s, tlv_hdr_pos_after);
	/* set tlv length */
	stream_putw_at(s, tlv_hdr_pos_before + BMP_INFO_OFFSET_TLV_LENGTH, stream_get_endp(s) - tlv_hdr_pos_after);
	return s;
}

/* sends a bmp monitoring message using the given information on the bmp session
 *
 * if uptime is (time_t)(-1L) then do not include the timestamp in the message
 */
static void bmp_monitor(struct bmp *bmp, struct peer *peer, uint8_t flags,
			uint8_t peer_type_flag, const struct prefix *p,
			struct prefix_rd *prd, struct attr *attr, afi_t afi,
			safi_t safi, uint32_t addpath_id, time_t uptime,
			struct bmp_path_status path_status)
{
	struct stream *hdr, *msg;
	struct timeval tv = { .tv_sec = uptime, .tv_usec = 0 };
	struct timeval uptime_real;

	uint64_t peer_distinguisher = 0;
	// skip this message if peer distinguisher is not available
	if (bmp_get_peer_distinguisher(bmp, afi, peer_type_flag,
				       &peer_distinguisher)) {
		zlog_debug(
			"skipping bmp message for reason: can't get peer distinguisher");
		return;
	}

	zlog_info("%s called: for peer %pBP prefix %pFX, attr %p, afi/safi %s, addpath %"PRIu32, __func__, peer, p, attr, get_afi_safi_str(afi, safi, false), addpath_id);

	monotime_to_realtime(&tv, &uptime_real);
	bool update = attr != NULL;
	if (update)
		msg = bmp_update(p, prd, addpath_id, peer, attr, afi, safi);
	else
		msg = bmp_withdraw(p, prd, addpath_id, afi, safi);

	hdr = stream_new(BGP_MAX_PACKET_SIZE);
	bmp_common_hdr(hdr, BMP_VERSION_3, BMP_TYPE_ROUTE_MONITORING);
	bmp_per_peer_hdr(hdr, bmp->targets->bgp, peer, flags, peer_type_flag,
			 peer_distinguisher,
			 uptime == (time_t)(-1L) ? NULL : &uptime_real);

	bmp_put_info_tlv_vrftablename_with_index(msg, bmp->targets->bgp, 0);
	if (update)
		bmp_put_info_tlv_path_status(hdr, path_status);

	stream_putl_at(hdr, BMP_LENGTH_POS,
		stream_get_endp(hdr) + stream_get_endp(msg));

	bmp->cnt_update++;
	pullwr_write_stream(bmp->pullwr, hdr);
	pullwr_write_stream(bmp->pullwr, msg);
	stream_free(hdr);
	stream_free(msg);
}


struct rib_out_pre_updgrp_walkctx {
	struct bmp *bmp;
	struct prefix *pfx;
	struct bgp_dest *dest;
	struct bgp_path_info *bpi;
	struct prefix_rd *prd;
	struct attr *attr;
	bool *written_ref;
};

/* bmp sync for rib-out pre-policy callback for each update group */
static int bmp_monitor_rib_out_pre_updgrp_walkcb(struct update_group *updgrp, void *hidden_ctx) {


	struct rib_out_pre_updgrp_walkctx *ctx = (struct rib_out_pre_updgrp_walkctx *)hidden_ctx;

	struct update_subgroup *subgrp;
	struct peer_af *paf;
	uint32_t addpath_tx_id;

	UPDGRP_FOREACH_SUBGRP (updgrp, subgrp) {

		struct attr dummy_attr = { 0 };
		if (!subgroup_announce_check(ctx->dest, ctx->bpi, subgrp, ctx->pfx, &dummy_attr, NULL,
					    BGP_ANNCHK_SPECIAL_PREPOLICY))
			continue;

		SUBGRP_FOREACH_PEER (subgrp, paf) {

			addpath_tx_id = !ctx->bpi ? 0
						  : bgp_addpath_id_for_peer(
							    SUBGRP_PEER(subgrp),
							    SUBGRP_AFI(subgrp),
							    SUBGRP_SAFI(subgrp),
							    &ctx->bpi->tx_addpath);

			bmp_monitor(ctx->bmp, PAF_PEER(paf), BMP_PEER_FLAG_O,
				    BMP_PEER_TYPE_GLOBAL_INSTANCE,
				    &ctx->dest->p, ctx->prd, ctx->attr,
				    SUBGRP_AFI(subgrp), SUBGRP_SAFI(subgrp),
				    addpath_tx_id, monotime(NULL),
				    bmp_make_path_status_adjout(ctx->bpi, ctx->dest, PAF_PEER(paf), addpath_tx_id));

			*ctx->written_ref = true;
		}
	}

	return HASHWALK_CONTINUE;
};

/* bmp sync for rib-out pre-policy
 * calls bmp_monitor_rib_out_pre_updgrp_walkcb foreach update group */
static inline bool bmp_monitor_rib_out_pre_walk(struct bmp *bmp, afi_t afi,
						safi_t safi,
						struct prefix *pfx,
						struct bgp_dest *dest,
						struct bgp_path_info *bpi,
						struct attr *attr,
						struct prefix_rd *prd) {
	bool written = false;
	struct rib_out_pre_updgrp_walkctx walkctx = {
		.bmp = bmp,
		.pfx = pfx,
		.dest = dest ? dest : bpi->net,
		.bpi = bpi,
		.attr = attr,
		.prd = prd,
		.written_ref = &written
	};

	update_group_af_walk(bmp->targets->bgp, afi, safi,
			     bmp_monitor_rib_out_pre_updgrp_walkcb,
			     (void *)&walkctx);

	return written;
}

struct rib_out_post_updgrp_walkctx {
	struct bmp *bmp;
	struct prefix *pfx;
	struct bgp_dest *dest;
	struct bgp_path_info *bpi;
	struct prefix_rd *prd;
	bool *written_ref;
};

/* bmp sync for rib-out post-policy callback for each update group */
static int bmp_monitor_rib_out_post_updgrp_walkcb(struct update_group *updgrp, void *hidden_ctx) {


	struct rib_out_post_updgrp_walkctx *ctx = (struct rib_out_post_updgrp_walkctx *)hidden_ctx;

	struct update_subgroup *subgrp;
	struct peer_af *paf;
	struct bgp_adj_out *adj;
	struct attr *advertised_attr;
	uint32_t addpath_tx_id;

	UPDGRP_FOREACH_SUBGRP (updgrp, subgrp) {

		addpath_tx_id = !ctx->bpi ? 0
					  : bgp_addpath_id_for_peer(
						    SUBGRP_PEER(subgrp),
						    SUBGRP_AFI(subgrp),
						    SUBGRP_SAFI(subgrp),
						    &ctx->bpi->tx_addpath);

		adj = adj_lookup(ctx->dest, subgrp, addpath_tx_id);

		if (!adj)
			continue;

		advertised_attr = !adj->adv ? adj->attr
				  : adj->adv->baa ?
						  adj->adv->baa->attr : NULL;

		SUBGRP_FOREACH_PEER (subgrp, paf) {
			bmp_monitor(ctx->bmp, PAF_PEER(paf),
				    BMP_PEER_FLAG_O | BMP_PEER_FLAG_L,
				    BMP_PEER_TYPE_GLOBAL_INSTANCE, ctx->pfx,
				    ctx->prd, advertised_attr,
				    SUBGRP_AFI(subgrp), SUBGRP_SAFI(subgrp),
				    addpath_tx_id, monotime(NULL),
				    bmp_make_path_status(ctx->bpi, ctx->dest));

			*ctx->written_ref = true;
		}


	}

	return HASHWALK_CONTINUE;
};

/* bmp sync for rib-out post-policy
 * calls bmp_monitor_rib_out_post_updgrp_walkcb for each update group */
static inline bool bmp_monitor_rib_out_post_walk(struct bmp *bmp, afi_t afi,
						safi_t safi, struct prefix *pfx,
						struct bgp_dest *dest,
						struct bgp_path_info *bpi,
						struct prefix_rd *prd) {
	bool written = false;
	struct rib_out_post_updgrp_walkctx walkctx = {
		.bmp = bmp,
		.pfx = pfx,
		.dest = dest,
		.bpi = bpi,
		.prd = prd,
		.written_ref = &written
	};

	update_group_af_walk(bmp->targets->bgp, afi, safi,
			     bmp_monitor_rib_out_post_updgrp_walkcb,
			     (void *)&walkctx);


	return written;
}

/* does the bmp initial rib synchronization
 */
static bool bmp_wrsync(struct bmp *bmp, struct pullwr *pullwr)
{
	afi_t afi;
	safi_t safi;

	if (bmp->syncafi == AFI_MAX) {
		FOREACH_AFI_SAFI (afi, safi) {
			if (bmp->afistate[afi][safi] != BMP_AFI_NEEDSYNC)
				continue;

			bmp->afistate[afi][safi] = BMP_AFI_SYNC;

			bmp->syncafi = afi;
			bmp->syncsafi = safi;
			bmp->syncpeerid = 0;
			memset(&bmp->syncpos, 0, sizeof(bmp->syncpos));
			bmp->syncpos.family = afi2family(afi);
			bmp->syncrdpos = NULL;
			zlog_info("bmp[%s] %s %s sending table",
					bmp->remote,
					afi2str(bmp->syncafi),
					safi2str(bmp->syncsafi));
			/* break does not work here, 2 loops... */
			goto afibreak;
		}
		if (bmp->syncafi == AFI_MAX)
			return false;
	}

afibreak:
	afi = bmp->syncafi;
	safi = bmp->syncsafi;

	if (!bmp->targets->afimon[afi][safi]) {
		/* shouldn't happen */
		bmp->afistate[afi][safi] = BMP_AFI_INACTIVE;
		bmp->syncafi = AFI_MAX;
		bmp->syncsafi = SAFI_MAX;
		return true;
	}

	struct bgp_table *table = bmp->targets->bgp->rib[afi][safi];
	struct bgp_dest *bn = NULL;
	struct bgp_path_info *bpi = NULL, *bpiter;
	struct bgp_adj_in *adjin = NULL, *adjiter;

	if ((afi == AFI_L2VPN && safi == SAFI_EVPN) ||
	    (safi == SAFI_MPLS_VPN)) {
		/* initialize syncrdpos to the first
		 * mid-layer table entry
		 */
		if (!bmp->syncrdpos) {
			bmp->syncrdpos = bgp_table_top(table);
			if (!bmp->syncrdpos)
				goto eor;
		}

		/* look for a valid mid-layer table */
		do {
			table = bgp_dest_get_bgp_table_info(bmp->syncrdpos);
			if (table) {
				break;
			}
			bmp->syncrdpos = bgp_route_next(bmp->syncrdpos);
		} while (bmp->syncrdpos);

		/* mid-layer table completed */
		if (!bmp->syncrdpos)
			goto eor;
	}

	bn = bgp_node_lookup(table, &bmp->syncpos);
	do {
		if (!bn) {
			bn = bgp_table_get_next(table, &bmp->syncpos);
			if (!bn) {
				if ((afi == AFI_L2VPN && safi == SAFI_EVPN) ||
				    (safi == SAFI_MPLS_VPN)) {
					/* reset bottom-layer pointer */
					memset(&bmp->syncpos, 0,
					       sizeof(bmp->syncpos));
					bmp->syncpos.family = afi2family(afi);
					/* check whethere there is a valid
					 * next mid-layer table, otherwise
					 * declare table completed (eor)
					 */
					for (bmp->syncrdpos = bgp_route_next(
						     bmp->syncrdpos);
					     bmp->syncrdpos;
					     bmp->syncrdpos = bgp_route_next(
						     bmp->syncrdpos))
						if (bgp_dest_get_bgp_table_info(
							    bmp->syncrdpos))
							return true;
				}
			eor:
				zlog_info("bmp[%s] %s %s table completed (EoR)",
						bmp->remote, afi2str(afi),
						safi2str(safi));

				if (CHECK_FLAG(bmp->targets->afimon[afi][safi], BMP_MON_IN_PREPOLICY))
					bmp_eor(bmp, afi, safi, 0,
						BMP_PEER_TYPE_GLOBAL_INSTANCE);
				if (CHECK_FLAG(bmp->targets->afimon[afi][safi], BMP_MON_IN_POSTPOLICY))
					bmp_eor(bmp, afi, safi, BMP_PEER_FLAG_L,
					BMP_PEER_TYPE_GLOBAL_INSTANCE);
				if (CHECK_FLAG(bmp->targets->afimon[afi][safi], BMP_MON_LOC_RIB))
					bmp_eor(bmp, afi, safi, 0,
					BMP_PEER_TYPE_LOC_RIB_INSTANCE);
				if (CHECK_FLAG(bmp->targets->afimon[afi][safi], BMP_MON_OUT_PREPOLICY))
					bmp_eor(bmp, afi, safi, BMP_PEER_FLAG_O,
					BMP_PEER_TYPE_GLOBAL_INSTANCE);
				if (CHECK_FLAG(bmp->targets->afimon[afi][safi], BMP_MON_OUT_POSTPOLICY))
					bmp_eor(bmp, afi, safi, BMP_PEER_FLAG_O | BMP_PEER_FLAG_L,
					BMP_PEER_TYPE_GLOBAL_INSTANCE);

				bmp->afistate[afi][safi] = BMP_AFI_LIVE;
				bmp->syncafi = AFI_MAX;
				bmp->syncsafi = SAFI_MAX;
				return true;
			}
			bmp->syncpeerid = 0;
			prefix_copy(&bmp->syncpos, bgp_dest_get_prefix(bn));
		}

		if (CHECK_FLAG(bmp->targets->afimon[afi][safi],
			       BMP_MON_IN_POSTPOLICY
				       | BMP_MON_LOC_RIB
				       | BMP_MON_OUT_PREPOLICY
				       | BMP_MON_OUT_POSTPOLICY)) {
			for (bpiter = bgp_dest_get_bgp_path_info(bn); bpiter;
			     bpiter = bpiter->next) {
				if (!CHECK_FLAG(bpiter->flags,
						BGP_PATH_VALID) &&
				    !CHECK_FLAG(bpiter->flags,
						BGP_PATH_SELECTED
						| BGP_PATH_MULTIPATH))
					continue;
				if (bpiter->peer->qobj_node.nid
				    <= bmp->syncpeerid)
					continue;
				if (bpi && bpiter->peer->qobj_node.nid
						> bpi->peer->qobj_node.nid)
					continue;
				bpi = bpiter;
			}
		}
		if (CHECK_FLAG(bmp->targets->afimon[afi][safi], BMP_MON_IN_PREPOLICY)) {
			for (adjiter = bn->adj_in; adjiter;
			     adjiter = adjiter->next) {
				if (adjiter->peer->qobj_node.nid
				    <= bmp->syncpeerid)
					continue;
				if (adjin && adjiter->peer->qobj_node.nid
						> adjin->peer->qobj_node.nid)
					continue;
				adjin = adjiter;
			}
		}
		if (bpi || adjin)
			break;

		bn = NULL;
	} while (1);

	if (adjin && bpi
	    && adjin->peer->qobj_node.nid < bpi->peer->qobj_node.nid) {
		bpi = NULL;
		bmp->syncpeerid = adjin->peer->qobj_node.nid;
	} else if (adjin && bpi
		   && adjin->peer->qobj_node.nid > bpi->peer->qobj_node.nid) {
		adjin = NULL;
		bmp->syncpeerid = bpi->peer->qobj_node.nid;
	} else if (bpi) {
		bmp->syncpeerid = bpi->peer->qobj_node.nid;
	} else if (adjin) {
		bmp->syncpeerid = adjin->peer->qobj_node.nid;
	}

	const struct prefix *bn_p = bgp_dest_get_prefix(bn);
	struct prefix_rd *prd = NULL;
	if (((afi == AFI_L2VPN) && (safi == SAFI_EVPN)) ||
	    (safi == SAFI_MPLS_VPN))
		prd = (struct prefix_rd *)bgp_dest_get_prefix(bmp->syncrdpos);

	bool written = false;

	if (adjin) {
		bmp_monitor(bmp, adjin->peer, 0, BMP_PEER_TYPE_GLOBAL_INSTANCE,
			    bn_p, prd, adjin->attr, afi, safi,
			    adjin->addpath_rx_id, adjin->uptime,
			    bmp_make_path_status_adjin(adjin));
		written = true;
	}

	uint8_t mon_flags = bmp->targets->afimon[afi][safi];

	if (bpi && CHECK_FLAG(bpi->flags, BGP_PATH_VALID) &&
	    CHECK_FLAG(mon_flags, BMP_MON_IN_POSTPOLICY)) {
		bmp_monitor(bmp, bpi->peer, BMP_PEER_FLAG_L,
			    BMP_PEER_TYPE_GLOBAL_INSTANCE, bn_p, prd, bpi->attr,
			    afi, safi, bpi->addpath_rx_id, bpi->uptime,
			    bmp_make_path_status(bpi, bn));

		UNSET_FLAG(bpi->flags, BGP_PATH_BMP_ADJIN_CHG);
		written = true;
	}

	bool bpi_selected = bpi && CHECK_FLAG(bpi->flags, BGP_PATH_SELECTED
								  | BGP_PATH_MULTIPATH);

	if (bpi_selected && CHECK_FLAG(mon_flags, BMP_MON_LOC_RIB)) {
		bmp_monitor(bmp, bpi->peer, 0, BMP_PEER_TYPE_LOC_RIB_INSTANCE,
			    bn_p, prd, bpi->attr, afi, safi, bpi->addpath_rx_id,
			    bpi && bpi->extra ? bpi->extra->bgp_rib_uptime
					      : (time_t)(-1L),
			    bmp_make_path_status(bpi, bn));
		written = true;
	}

	if (bpi_selected && CHECK_FLAG(mon_flags, BMP_MON_OUT_PREPOLICY)) {
		written |= bmp_monitor_rib_out_pre_walk(bmp, afi, safi, &bn->p, bn, bpi, bpi->attr, prd);
	}

	if (bpi_selected && CHECK_FLAG(mon_flags, BMP_MON_OUT_POSTPOLICY)) {
		written |= bmp_monitor_rib_out_post_walk(bmp, afi, safi, &bn->p, bn, bpi, prd);
	}

	if (bn)
		bgp_dest_unlock_node(bn);

	/* if we got here and nothing is written it means this specific
	 * destination had no monitoring configured or configured monitoring
	 * had nothing to send. we need to bump for wrsync to be called for the
	 * following destinations in the table
	 */
	if (!written)
		pullwr_bump(bmp->pullwr);

	return written;
}

/* pulls a bqe from a given list
 */
static struct bmp_queue_entry *
bmp_pull_from_queue(struct bmp_qlist_head *list, struct bmp_qhash_head *hash,
		    struct bmp_queue_entry **queuepos_ptr)
{
	struct bmp_queue_entry *bqe;
	bqe = *queuepos_ptr;

	if (!bqe)
		return NULL;

	*queuepos_ptr = bmp_qlist_next(list, bqe);

	bqe->refcount--;
	if (!bqe->refcount) {
		bmp_qhash_del(hash, bqe);
		bmp_qlist_del(list, bqe);
	}
	return bqe;
}

/* shortcut to pull a bqe from the rib-in pre-policy queue
 */
static inline struct bmp_queue_entry *bmp_pull_ribin(struct bmp *bmp)
{
	return bmp_pull_from_queue(&bmp->targets->mon_in_updlist,
				   &bmp->targets->mon_in_updhash, &bmp->mon_in_queuepos);
}

/* shortcut to pull a bqe from the loc-rib + rib-in post-policy queue
 */
static inline struct bmp_queue_entry *bmp_pull_locrib(struct bmp *bmp)
{
	return bmp_pull_from_queue(&bmp->targets->mon_loc_updlist,
				   &bmp->targets->mon_loc_updhash,
				   &bmp->mon_loc_queuepos);
}

/* shortcut to pull a bqe from the rib-out pre/post queue
 */
static inline struct bmp_queue_entry *bmp_pull_ribout(struct bmp *bmp)
{
	return bmp_pull_from_queue(&bmp->targets->mon_out_updlist,
				   &bmp->targets->mon_out_updhash,
				   &bmp->mon_out_queuepos);
}

/* returns 1 if the prefix will be synced later
 * it means that we do not need to send an update about this prefix
 */
static inline int bmp_prefix_will_sync(struct bmp *bmp, afi_t afi, safi_t safi,
				       struct prefix *prefix) {

	switch (bmp->afistate[afi][safi]) {
	case BMP_AFI_INACTIVE:
	case BMP_AFI_NEEDSYNC:
		/* this afi will be synced later, wait for sync
		 */
		return 1;
	case BMP_AFI_SYNC:
		if (prefix_cmp(prefix, &bmp->syncpos) <= 0)
			/* currently syncing but have already passed this
			 * prefix => send it. */
			return 0;

		/* currently syncing & haven't reached this prefix yet
		 * => it'll be sent as part of the table sync, no update here
		 */

		return 1;
	case BMP_AFI_LIVE:
		/* this afi has already been synced, send an update
		 */
		return 0;
	}

	return 0;
}

/* gets a bqe from the loc-rib queue and sends a bmp monitoring message for
 * loc-rib (if configured) and rib-out post-policy (if configured)
 * the messages use the first selected path found in rib matching the prefix
 *
 * TODO BMP_MON_LOCRIB find a way to merge properly this function with
 * bmp_wrqueue_in or abstract it if possible
 */
static bool bmp_wrqueue_locrib(struct bmp *bmp, struct pullwr *pullwr)
{
	struct bmp_queue_entry *bqe;
	struct peer *peer;
	struct bgp_dest *bn = NULL;
	bool written = false;

	bqe = bmp_pull_locrib(bmp);
	if (!bqe)
		return false;

	afi_t afi = bqe->afi;
	safi_t safi = bqe->safi;
	uint8_t flags = bmp->targets->afimon[afi][safi] & bqe->flags;

	uint32_t addpath_rx_id = bqe->addpath_id;

	if (!CHECK_FLAG(bmp->targets->afimon[afi][safi], BMP_MON_IN_POSTPOLICY
							 | BMP_MON_LOC_RIB))
		goto out;

	if (bmp_prefix_will_sync(bmp, afi, safi, &bqe->p)) {
		goto out;
	}

	peer = QOBJ_GET_TYPESAFE(bqe->peerid, peer);
	if (!peer) {
		/* skipping queued item for deleted peer
		 */
		goto out;
	}
	if (peer != bmp->targets->bgp->peer_self && !peer_established(peer)) {
		/* peer is neither self, nor established
		 */
		goto out;
	}

	/* retrieve info about the selected path
	 */
	bool is_vpn = (bqe->afi == AFI_L2VPN && bqe->safi == SAFI_EVPN) ||
		      (bqe->safi == SAFI_MPLS_VPN);

	struct prefix_rd *prd = is_vpn ? &bqe->rd : NULL;

	bn = bgp_afi_node_lookup(bmp->targets->bgp->rib[afi][safi], afi, safi,
				 &bqe->p, prd);

	struct bgp_path_info *locrib = NULL, *ribin = NULL;
	for (struct bgp_path_info *bpi = bn ? bgp_dest_get_bgp_path_info(bn) : NULL; bpi;
	     bpi = bpi->next) {

		/* match the right path */
		if (bpi->peer != peer || bpi->addpath_rx_id != addpath_rx_id)
			continue;

		/* rib-in post-policy configured and path is valid */
		if (CHECK_FLAG(flags, BMP_MON_IN_POSTPOLICY)
		    && CHECK_FLAG(bpi->flags, BGP_PATH_VALID)) {

			bmp_monitor(bmp, peer, BMP_PEER_FLAG_L,
				    BMP_PEER_TYPE_GLOBAL_INSTANCE, &bqe->p, prd,
				    bpi->attr, afi, safi, addpath_rx_id,
				    bpi->uptime, bmp_make_path_status(bpi, bn));
			ribin = bpi;
			written = true;
		}

		/* loc-rib configured and path is selected */
		if (CHECK_FLAG(flags, BMP_MON_LOC_RIB)
		    && CHECK_FLAG(bpi->flags, BGP_PATH_SELECTED
						      | BGP_PATH_MULTIPATH)) {

			bmp_monitor(bmp, peer, 0, BMP_PEER_TYPE_LOC_RIB_INSTANCE,
				    &bqe->p, prd, bpi->attr, afi, safi, addpath_rx_id,
				    bpi->extra ? bpi->extra->bgp_rib_uptime
					       : (time_t)(-1L),
				    bmp_make_path_status(bpi, bn));
			locrib = bpi;
			written = true;
		}

		if (locrib && ribin) /* early out when we've sent both messages */
			goto out;
	}

	/* rib-in post-policy path not found, send withdraw */
	if (CHECK_FLAG(flags, BMP_MON_IN_POSTPOLICY)
	    && !ribin) {
		bmp_monitor(bmp, peer, BMP_PEER_FLAG_L,
			    BMP_PEER_TYPE_GLOBAL_INSTANCE, &bqe->p, prd, NULL,
			    afi, safi, addpath_rx_id, (time_t)(-1),
			    bmp_make_path_status(ribin, bn));
		written = true;
	}

	/* loc-rib path not found, send withdraw */
	if (CHECK_FLAG(flags, BMP_MON_LOC_RIB)
	    && !locrib) {
		bmp_monitor(bmp, peer, 0, BMP_PEER_TYPE_LOC_RIB_INSTANCE,
			    &bqe->p, prd, NULL, afi, safi, addpath_rx_id,
			    (time_t)(-1L),
			    bmp_make_path_status(locrib, bn));
		written = true;
	}

out:
	if (!bqe->refcount)
		bmp_bqe_free(bqe);

	if (bn)
		bgp_dest_unlock_node(bn);

	return written;
}

/* gets a bqe from the rib-in pre-policy queue and sends a bmp monitoring
 * message to the peer if configured about the
 * first valid path found in adj-rib-in for this prefix
 */
static bool bmp_wrqueue_ribin(struct bmp *bmp, struct pullwr *pullwr)
{
	struct bmp_queue_entry *bqe;
	struct peer *peer;
	struct bgp_dest *bn = NULL;
	bool written = false;

	bqe = bmp_pull_ribin(bmp);
	if (!bqe)
		return false;

	afi_t afi = bqe->afi;
	safi_t safi = bqe->safi;
	uint32_t addpath_rx_id = bqe->addpath_id;

	if (bmp_prefix_will_sync(bmp, afi, safi, &bqe->p)) {
		goto out;
	}

	peer = QOBJ_GET_TYPESAFE(bqe->peerid, peer);
	if (!peer) {
		zlog_info("bmp: skipping queued item for deleted peer");
		goto out;
	}
	if (!peer_established(peer))
		goto out;

	if (!CHECK_FLAG(bmp->targets->afimon[afi][safi], BMP_MON_IN_PREPOLICY) ||
		    !CHECK_FLAG(bqe->flags, BMP_MON_IN_PREPOLICY))
		    goto out;

	bool is_vpn = (bqe->afi == AFI_L2VPN && bqe->safi == SAFI_EVPN) ||
		      (bqe->safi == SAFI_MPLS_VPN);

	struct prefix_rd *prd = is_vpn ? &bqe->rd : NULL;
	bn = bgp_afi_node_get(bmp->targets->bgp->rib[afi][safi], afi, safi,
				 &bqe->p, prd);

	struct bgp_adj_in *adjin;

	/* lookup adjin of this destination */
	for (adjin = bn ? bn->adj_in : NULL; adjin;
	     adjin = adjin->next) {
		/* match right path */
		if (adjin->peer == peer && adjin->addpath_rx_id == addpath_rx_id)
			break;
	}

	zlog_info("%s: adjin is %p", __func__, adjin);
	if (adjin)
		zlog_info("%s: adjin from peer %pBP rxid %"PRIu32" filtered=%d, reason=%s",
			  __func__,
			  adjin->peer,
			  adjin->addpath_rx_id,
			  adjin->filtered,
			  bgp_inbound_filtered_reason_str(adjin->reason));

	bmp_monitor(bmp, peer, 0, BMP_PEER_TYPE_GLOBAL_INSTANCE, &bqe->p, prd,
		    adjin ? adjin->attr : NULL, afi, safi, addpath_rx_id,
		    adjin ? adjin->uptime : monotime(NULL),
		    bmp_make_path_status_adjin(adjin));

	written = true;

out:
	if (!bqe->refcount)
		bmp_bqe_free(bqe);

	if (bn)
		bgp_dest_unlock_node(bn);

	return written;
}

/* gets a bqe from the rib-out pre/post-policy queue and sends a bmp
 * rib-out pre/post-policy monitoring message to the peer
 */
static bool bmp_wrqueue_ribout(struct bmp *bmp, struct pullwr *pullwr)
{
	struct bmp_queue_entry *bqe;
	struct peer *peer;
	struct bgp_dest *bn = NULL;
	bool written = false;

	bqe = bmp_pull_ribout(bmp);
	if (!bqe)
		return false;

	afi_t afi = bqe->afi;
	safi_t safi = bqe->safi;
	uint32_t addpath_tx_id = bqe->addpath_id;

	if (!CHECK_FLAG(bmp->targets->afimon[afi][safi], BMP_MON_OUT_POSTPOLICY
			| BMP_MON_OUT_PREPOLICY)) {
		goto out;
	}

	if (bmp_prefix_will_sync(bmp, afi, safi, &bqe->p)) {
		goto out;
	}

	peer = QOBJ_GET_TYPESAFE(bqe->peerid, peer);
	if (!peer) {
		zlog_info("bmp: skipping queued item for deleted peer");
		goto out;
	}

	bool is_vpn = (bqe->afi == AFI_L2VPN && bqe->safi == SAFI_EVPN) ||
		      (bqe->safi == SAFI_MPLS_VPN);

	struct prefix_rd *prd = is_vpn ? &bqe->rd : NULL;

	bn = bgp_afi_node_lookup(bmp->targets->bgp->rib[afi][safi], afi, safi,
				 &bqe->p, prd);

	if (CHECK_FLAG(bmp->targets->afimon[afi][safi], BMP_MON_OUT_PREPOLICY)
	    && CHECK_FLAG(bqe->flags, BMP_MON_OUT_PREPOLICY)) {

		/* lookup path in rib */
		struct bgp_path_info *bpi;
		for (bpi = bn ? bgp_dest_get_bgp_path_info(bn) : NULL;
		     bpi; bpi = bpi->next) {

			if (addpath_tx_id !=
			    bgp_addpath_id_for_peer(peer, afi, safi,
						    &bpi->tx_addpath))
				continue;

			if (CHECK_FLAG(bpi->flags, BGP_PATH_SELECTED
				       | BGP_PATH_MULTIPATH))
				break;
		}

		zlog_info("%s using bpi %p", __func__, bpi);
		bmp_monitor(bmp, peer, BMP_PEER_FLAG_O,
			    BMP_PEER_TYPE_GLOBAL_INSTANCE, &bqe->p, prd,
			    bpi ? bpi->attr : NULL, afi,
			    safi, addpath_tx_id, monotime(NULL),
			    bmp_make_path_status_adjout(bpi, !bn && bpi ? bpi->net : bn, peer, addpath_tx_id));

		written = true;
	}

	if (CHECK_FLAG(bmp->targets->afimon[afi][safi], BMP_MON_OUT_POSTPOLICY)
	    && CHECK_FLAG(bqe->flags, BMP_MON_OUT_POSTPOLICY)) {
		struct bgp_adj_out *adj;
		struct attr *advertised_attr;

		/* lookup path in adj-rib-out */
		adj = adj_lookup(bn, peer_subgroup(peer, afi, safi), addpath_tx_id);

		/* advertised attributes (NULL if withdrawn) */
		advertised_attr =
			adj ?
			    !adj->adv ? adj->attr
				      : adj->adv->baa ?
					    adj->adv->baa->attr : NULL
			: NULL;

		bmp_monitor(bmp, peer, BMP_PEER_FLAG_L | BMP_PEER_FLAG_O,
			    BMP_PEER_TYPE_GLOBAL_INSTANCE, &bqe->p, prd,
			    advertised_attr, afi, safi, addpath_tx_id,
			    monotime(NULL),
			    bmp_make_path_status(adj && adj->adv && adj->adv->pathi ? adj->adv->pathi : NULL, bn));

		written = true;
	}

out:
	if (!bqe->refcount)
		bmp_bqe_free(bqe);

	if (bn)
		bgp_dest_unlock_node(bn);

	return written;
}

/* called, when the socket is available, to retrieve data to send
 */
static void bmp_wrfill(struct bmp *bmp, struct pullwr *pullwr)
{

	uint32_t timeout_ms;
	uint32_t startup_delay = bmp->targets->bmpbgp->startup_delay_ms;
	switch(bmp->state) {
	case BMP_StartupIdle:
		if ((timeout_ms = bmp_time_since_startup(NULL)) < startup_delay) {
			pullwr_timeout(pullwr, startup_delay - timeout_ms);
			return;
		}

		uint32_t micros_since = (uint32_t) (monotime_since(&bmp_startup_time, NULL));
		zlog_info("bmp: Startup timeout expired, time since startup is %"PRIu32"ms", micros_since / 1000);
		bmp->state = BMP_PeerUp;
		// fall through
	case BMP_PeerUp:
		bmp_send_peerup(bmp);
		bmp->state = BMP_Run;
		break;

	case BMP_Run:
		if (bmp_wrmirror(bmp, pullwr))
			break;
		if (bmp_wrqueue_ribin(bmp, pullwr))
			break;
		if (bmp_wrqueue_locrib(bmp, pullwr))
			break;
		if (bmp_wrqueue_ribout(bmp, pullwr))
			break;
		if (bmp_wrsync(bmp, pullwr))
			break;
		break;
	}
}

static void bmp_wrerr(struct bmp *bmp, struct pullwr *pullwr, bool eof)
{
	if (eof)
		zlog_info("bmp[%s] disconnected", bmp->remote);
	else
		flog_warn(EC_LIB_SYSTEM_CALL, "bmp[%s] connection error: %s",
				bmp->remote, strerror(errno));

	bmp_close(bmp);
	bmp_free(bmp);
}

/* inserts a bmp_queue_entry in the updlist. overwrites any similar
 * bmp_queue_entry in the list.
 *
 * returns the bqe inserted or NULL if updated an already existing bqe
 *
 * need to update correct queue pos for all sessions of the target after
 * a call to this function
 */
static struct bmp_queue_entry *
bmp_process_one(struct bmp_targets *bt, struct bmp_qhash_head *updhash,
		struct bmp_qlist_head *updlist, struct bgp *bgp, afi_t afi,
		safi_t safi, struct bgp_dest *bn, uint32_t addpath_id,
		struct peer *peer, uint8_t mon_flag)
{
	struct bmp_queue_entry *bqe, bqeref;
	size_t refcount;

	refcount = bmp_session_count(&bt->sessions);
	if (refcount == 0)
		return NULL;

	memset(&bqeref, 0, sizeof(bqeref));
	prefix_copy(&bqeref.p, bgp_dest_get_prefix(bn));
	bqeref.peerid = peer->qobj_node.nid;
	bqeref.afi = afi;
	bqeref.safi = safi;
	bqeref.flags = mon_flag;
	bqeref.addpath_id = addpath_id;

	if ((afi == AFI_L2VPN && safi == SAFI_EVPN && bn->pdest) ||
	    (safi == SAFI_MPLS_VPN))
		prefix_copy(&bqeref.rd,
			    (struct prefix_rd *)bgp_dest_get_prefix(bn->pdest));

	bqe = bmp_qhash_find(updhash, &bqeref);
	if (bqe) {
		SET_FLAG(bqe->flags, mon_flag);

		if (bqe->refcount >= refcount) {
			/* same update, not sent to anyone yet,
			 * nothing to do here */
			return NULL;
		}

		bmp_qlist_del(updlist, bqe);
	} else {
		bqe = XMALLOC(MTYPE_BMP_QUEUE, sizeof(*bqe));
		memcpy(bqe, &bqeref, sizeof(*bqe));

		bmp_qhash_add(updhash, bqe);
	}

	bqe->refcount = refcount;
	bmp_qlist_add_tail(updlist, bqe);

	return bqe;
}


/* triggered when a change in adj-rib-in is detected.
 * if enabled in config, inserts a bqe to the adj-rib-in pre monitoring queue
 * which will trigger a bmp monitoring message to be sent for adj-rib-in pre
 */
static int bmp_process_ribinpre(struct bgp *bgp, afi_t afi, safi_t safi,
			     struct bgp_dest *bn, uint32_t addpath_id,
			     struct peer *peer, bool post)
{
	struct bmp_bgp *bmpbgp = bmp_bgp_find(peer->bgp);
	struct bmp_targets *bt;
	struct bmp *bmp;

	if (frrtrace_enabled(frr_bgp, bmp_process_ribinpre)) {
		char pfxprint[PREFIX2STR_BUFFER];

		prefix2str(&bn->p, pfxprint, sizeof(pfxprint));
		frrtrace(5, frr_bgp, bmp_process_ribinpre, peer, pfxprint, afi, safi,
			 withdraw);
	}

	if (!bmpbgp)
		return 0;

	/* mark paths that changed in adj-rib-in pre-policy
	 * allows rib-in post to know which path has changed in adj-in and needs
	 * a bmp update
	 *
	 * (trigger is called later for VALID state to be evaluated)
	 * this is needed because where the trigger is called (bgp_process)
	 * we don't know because of which adj-in update we recompute the
	 * best-path
	 */
	if (post) {
		for (struct bgp_path_info *bpi = bgp_dest_get_bgp_path_info(bn); bpi;
		     bpi = bpi->next)
			if (bpi->peer == peer &&
			    bpi->addpath_rx_id == addpath_id)
				SET_FLAG(bpi->flags, BGP_PATH_BMP_ADJIN_CHG);
	}

	zlog_info("%s: for afi/safi %s dest %pRN addpath_id %"PRIu32" peer %pBP post %d",
		  __func__, get_afi_safi_str(afi, safi, false), bn, addpath_id, peer, post);

	frr_each(bmp_targets, &bmpbgp->targets, bt) {
		if (!CHECK_FLAG(bt->afimon[afi][safi], BMP_MON_IN_PREPOLICY))
			continue;

		struct bmp_queue_entry *new_item = bmp_process_one(
			bt, &bt->mon_in_updhash, &bt->mon_in_updlist, bgp, afi,
			safi, bn, addpath_id, peer, BMP_MON_IN_PREPOLICY);

		// if bmp_process_one returns NULL
		// we don't have anything to do next
		if (!new_item)
			continue;

		frr_each(bmp_session, &bt->sessions, bmp) {
			if (!bmp->mon_in_queuepos)
				bmp->mon_in_queuepos = new_item;

			pullwr_bump(bmp->pullwr);
		}
	}
	return 0;
}

/* triggered when a change in adj-rib-in post-policy is detected.
 * if enabled in config, inserts a bqe to the loc-rib / rib-in-post
 * monitoring queue which will trigger a bmp monitoring message to be sent
 * for adj-rib-in post-policy
 */
static int bmp_process_ribinpost(struct bgp *bgp, afi_t afi, safi_t safi,
				  struct bgp_dest *bn)
{
	struct bmp_bgp *bmpbgp = bmp_bgp_find(bgp);
	struct bmp_targets *bt;
	struct bmp *bmp;

	if (frrtrace_enabled(frr_bgp, bmp_process_ribinpre)) {
		char pfxprint[PREFIX2STR_BUFFER];

		prefix2str(&bn->p, pfxprint, sizeof(pfxprint));
		frrtrace(5, frr_bgp, bmp_process_ribinpre, peer, pfxprint, afi, safi,
			 withdraw);
	}

	if (!bmpbgp)
		return 0;


	frr_each(bmp_targets, &bmpbgp->targets, bt) {
		if (!CHECK_FLAG(bt->afimon[afi][safi], BMP_MON_IN_POSTPOLICY))
			continue;

		struct bmp_queue_entry *new_head = NULL, *new_item = NULL;

		/* find paths that changed in pre-policy and need update */
		for (struct bgp_path_info *bpi = bgp_dest_get_bgp_path_info(bn); bpi; bpi = bpi->next) {
			if (CHECK_FLAG(bpi->flags, BGP_PATH_BMP_ADJIN_CHG)) {
				new_item = bmp_process_one(
					bt, &bt->mon_loc_updhash,
					&bt->mon_loc_updlist, bgp, afi, safi,
					bn, bpi->addpath_rx_id, bpi->peer,
					BMP_MON_IN_POSTPOLICY);

				new_head = !new_head ? new_item : new_head;

				UNSET_FLAG(bpi->flags, BGP_PATH_BMP_ADJIN_CHG);
			}
		}

		// if bmp_process_one returns NULL
		// we don't have anything to do next
		if (!new_head)
			continue;

		frr_each(bmp_session, &bt->sessions, bmp) {
			if (!bmp->mon_loc_queuepos)
				bmp->mon_loc_queuepos = new_head;

			pullwr_bump(bmp->pullwr);
		}
	}

	return 0;
}

/* put a u32 stat to the stream */
static void bmp_stat_put_u32(struct stream *s, size_t *cnt, uint16_t type,
		uint32_t value)
{
	stream_putw(s, type);
	stream_putw(s, 4);
	stream_putl(s, value);
	(*cnt)++;
}

/* put a u64 stat to the stream */
static void bmp_stat_put_u64(struct stream *s, size_t *cnt, uint16_t type,
		uint64_t value)
{
	stream_putw(s, type);
	stream_putw(s, 8);
	stream_putq(s, value);
	(*cnt)++;
}

/* put a per-AFI/SAFI u64 stat to the stream */
static void bmp_stat_put_af_u64(struct stream *s, size_t *cnt, uint16_t type,
				afi_t afi, safi_t safi, uint64_t value)
{
	stream_putw(s, type);
	stream_putw(s, 2 + 1 + 8);
	stream_put3(s, (afi_int2iana(afi) << 8) + safi_int2iana(safi));
	stream_putq(s, value);
	(*cnt)++;
}

/* send stats and reschedule event */
static void bmp_stats(struct thread *thread)
{
	struct bmp_targets *bt = THREAD_ARG(thread);
	struct stream *s;
	struct peer *peer;
	struct listnode *node;
	struct timeval tv;
	afi_t afi;
	safi_t safi;
	uint64_t af_stat[AFI_MAX][SAFI_MAX];
	struct update_subgroup *subgrp;


	if (bt->stat_msec)
		thread_add_timer_msec(bm->master, bmp_stats, bt, bt->stat_msec,
				&bt->t_stats);

	gettimeofday(&tv, NULL);

	/* Walk down all peers */
	for (ALL_LIST_ELEMENTS_RO(bt->bgp->peer, node, peer)) {
		size_t count = 0, count_pos, len;
		uint64_t per_af_sum = 0;

		if (!peer_established(peer))
			continue;

		s = stream_new(BGP_MAX_PACKET_SIZE);
		bmp_common_hdr(s, BMP_VERSION_3, BMP_TYPE_STATISTICS_REPORT);
		bmp_per_peer_hdr(s, bt->bgp, peer, 0,
				 BMP_PEER_TYPE_GLOBAL_INSTANCE, 0, &tv);

		count_pos = stream_get_endp(s);
		stream_putl(s, 0);

		bmp_stat_put_u32(s, &count, BMP_STATS_PFX_REJECTED,
				peer->stat_pfx_filter);
		bmp_stat_put_u32(s, &count, BMP_STATS_PFX_DUP_WITHDRAW,
				 peer->stat_pfx_dup_withdraw);
		bmp_stat_put_u32(s, &count, BMP_STATS_UPD_LOOP_CLUSTER,
				 peer->stat_pfx_cluster_loop);
		bmp_stat_put_u32(s, &count, BMP_STATS_UPD_LOOP_ASPATH,
				peer->stat_pfx_aspath_loop);
		bmp_stat_put_u32(s, &count, BMP_STATS_UPD_LOOP_ORIGINATOR,
				peer->stat_pfx_originator_loop);
		bmp_stat_put_u32(s, &count, BMP_STATS_UPD_7606_WITHDRAW,
				peer->stat_upd_7606);

/* macro that sends a stat per AFI/SAFI and the total sum of sub-stats
 * per AFI/SAFI is not sent if and force_zero == false */
# define BMP_PER_AF_STAT(afi_var, safi_var, af_stat_arr, sum_var, safi_stat, 			\
		    safi_stat_call, sum_stat_call, force_zero) 					\
		do {                                                                      	\
			(sum_var) = 0;								\
			FOREACH_AFI_SAFI ((afi_var), (safi_var)) {				\
			(sum_var) += ((af_stat_arr)[(afi_var)][(safi_var)] = (safi_stat));  	\
			if ((force_zero) || (af_stat_arr)[(afi_var)][(safi_var)])		\
				(safi_stat_call);						\
			};									\
			(sum_stat_call);							\
		} while (0);

		BMP_PER_AF_STAT(afi, safi, af_stat, per_af_sum,
			    peer->stat_adj_in_count[afi][safi],
			    bmp_stat_put_af_u64(
				    s, &count,
				    BMP_STATS_SIZE_ADJ_RIB_IN_SAFI, afi, safi,
				    af_stat[afi][safi]),
			    bmp_stat_put_u64(
				    s, &count,
				    BMP_STATS_SIZE_ADJ_RIB_IN,
				    per_af_sum),
			    false
		);

		BMP_PER_AF_STAT(afi, safi, af_stat, per_af_sum,
			    peer->stat_loc_rib_count[afi][safi],
			    bmp_stat_put_af_u64(
				    s, &count,
				    BMP_STATS_SIZE_LOC_RIB_SAFI, afi, safi,
				    af_stat[afi][safi]),
			    bmp_stat_put_u64(
				    s, &count,
				    BMP_STATS_SIZE_LOC_RIB,
				    per_af_sum),
			    false
		);

		BMP_PER_AF_STAT(afi, safi, af_stat, per_af_sum,
			    ((subgrp = peer_subgroup(peer, afi, safi)) ?
			     subgrp->pscount : 0),
			    bmp_stat_put_af_u64(
				    s, &count,
				    BMP_STATS_SIZE_ADJ_RIB_OUT_POST_SAFI,
				    afi, safi, af_stat[afi][safi]),
			    bmp_stat_put_u64(
				    s, &count,
				    BMP_STATS_SIZE_ADJ_RIB_OUT_POST,
				    per_af_sum),
			    false
		);

		bmp_stat_put_u32(s, &count, BMP_STATS_FRR_NH_INVALID,
				peer->stat_pfx_nh_invalid);

		stream_putl_at(s, count_pos, count);

		len = stream_get_endp(s);
		stream_putl_at(s, BMP_LENGTH_POS, len);

		bmp_send_all(bt->bmpbgp, s);
	}
}

/* read from the BMP socket to detect session termination */
static void bmp_read(struct thread *t)
{
	struct bmp *bmp = THREAD_ARG(t);
	char buf[1024];
	ssize_t n;

	bmp->t_read = NULL;

	n = read(bmp->socket, buf, sizeof(buf));
	if (n >= 1) {
		zlog_info("bmp[%s]: unexpectedly received %zu bytes", bmp->remote, n);
	} else if (n == 0) {
		/* the TCP session was terminated by the far end */
		bmp_wrerr(bmp, NULL, true);
		return;
	} else if (!(errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
		/* the TCP session experienced a fatal error, likely a timeout */
		bmp_wrerr(bmp, NULL, false);
		return;
	}

	thread_add_read(bm->master, bmp_read, bmp, bmp->socket, &bmp->t_read);
}

static struct bmp *bmp_open(struct bmp_targets *bt, int bmp_sock)
{
	union sockunion su, *sumem;
	struct prefix p;
	int on = 1;
	struct access_list *acl = NULL;
	enum filter_type ret;
	char buf[SU_ADDRSTRLEN];
	struct bmp *bmp;

	sumem = sockunion_getpeername(bmp_sock);
	if (!sumem) {
		close(bmp_sock);
		return NULL;
	}
	memcpy(&su, sumem, sizeof(su));
	sockunion_free(sumem);

	set_nonblocking(bmp_sock);
	set_cloexec(bmp_sock);

	if (!sockunion2hostprefix(&su, &p)) {
		close(bmp_sock);
		return NULL;
	}

	acl = NULL;
	switch (p.family) {
	case AF_INET:
		acl = access_list_lookup(AFI_IP, bt->acl_name);
		break;
	case AF_INET6:
		acl = access_list_lookup(AFI_IP6, bt->acl6_name);
		break;
	default:
		break;
	}

	ret = FILTER_PERMIT;
	if (acl) {
		ret = access_list_apply(acl, &p);
	}

	sockunion2str(&su, buf, SU_ADDRSTRLEN);
	snprintf(buf + strlen(buf), sizeof(buf) - strlen(buf), ":%u",
			su.sa.sa_family == AF_INET
				? ntohs(su.sin.sin_port)
				: ntohs(su.sin6.sin6_port));

	if (ret == FILTER_DENY) {
		bt->cnt_aclrefused++;
		zlog_info("bmp[%s] connection refused by access-list", buf);
		close(bmp_sock);
		return NULL;
	}
	bt->cnt_accept++;

	if (setsockopt(bmp_sock, SOL_SOCKET, SO_KEEPALIVE, &on, sizeof(on)) < 0)
		flog_err(EC_LIB_SOCKET, "bmp: %d can't setsockopt SO_KEEPALIVE: %s(%d)",
			 bmp_sock, safe_strerror(errno), errno);
	if (setsockopt(bmp_sock, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on)) < 0)
		flog_err(EC_LIB_SOCKET, "bmp: %d can't setsockopt TCP_NODELAY: %s(%d)",
			 bmp_sock, safe_strerror(errno), errno);

	zlog_info("bmp[%s] connection established", buf);

	/* Allocate new BMP structure and set up default values. */
	bmp = bmp_new(bt, bmp_sock);
	strlcpy(bmp->remote, buf, sizeof(bmp->remote));

	bmp->state = BMP_StartupIdle;
	bmp->pullwr = pullwr_new(bm->master, bmp_sock, bmp, bmp_wrfill,
			bmp_wrerr);
	thread_add_read(bm->master, bmp_read, bmp, bmp_sock, &bmp->t_read);
	bmp_send_initiation(bmp);

	return bmp;
}

/* Accept BMP connection. */
static void bmp_accept(struct thread *thread)
{
	union sockunion su;
	struct bmp_listener *bl = THREAD_ARG(thread);
	int bmp_sock;

	/* We continue hearing BMP socket. */
	thread_add_read(bm->master, bmp_accept, bl, bl->sock, &bl->t_accept);

	memset(&su, 0, sizeof(union sockunion));

	/* We can handle IPv4 or IPv6 socket. */
	bmp_sock = sockunion_accept(bl->sock, &su);
	if (bmp_sock < 0) {
		zlog_info("bmp: accept_sock failed: %s", safe_strerror(errno));
		return;
	}
	bmp_open(bl->targets, bmp_sock);
}

static void bmp_close(struct bmp *bmp)
{
	struct bmp_queue_entry *bqe;
	struct bmp_mirrorq *bmq;

	THREAD_OFF(bmp->t_read);

	if (bmp->active)
		bmp_active_disconnected(bmp->active);

	while ((bmq = bmp_pull_mirror(bmp)))
		if (!bmq->refcount)
			XFREE(MTYPE_BMP_MIRRORQ, bmq);
	while ((bqe = bmp_pull_ribin(bmp)))
		if (!bqe->refcount)
			bmp_bqe_free(bqe);
	while ((bqe = bmp_pull_locrib(bmp)))
		if (!bqe->refcount)
			bmp_bqe_free(bqe);
	while ((bqe = bmp_pull_ribout(bmp)))
		if (!bqe->refcount)
			bmp_bqe_free(bqe);

	THREAD_OFF(bmp->t_read);
	pullwr_del(bmp->pullwr);
	close(bmp->socket);
}

static struct bmp_bgp *bmp_bgp_find(struct bgp *bgp)
{
	struct bmp_bgp dummy = { .bgp = bgp };
	return bmp_bgph_find(&bmp_bgph, &dummy);
}

static struct bmp_bgp *bmp_bgp_get(struct bgp *bgp)
{
	struct bmp_bgp *bmpbgp;

	bmpbgp = bmp_bgp_find(bgp);
	if (bmpbgp)
		return bmpbgp;

	bmpbgp = XCALLOC(MTYPE_BMP, sizeof(*bmpbgp));
	bmpbgp->bgp = bgp;
	bmpbgp->mirror_qsizelimit = ~0UL;
	bmpbgp->startup_delay_ms = 0;
	bmp_mirrorq_init(&bmpbgp->mirrorq);
	bmp_bgph_add(&bmp_bgph, bmpbgp);

	return bmpbgp;
}

static void bmp_bgp_put(struct bmp_bgp *bmpbgp)
{
	struct bmp_targets *bt;
	struct bmp_listener *bl;

	bmp_bgph_del(&bmp_bgph, bmpbgp);

	frr_each_safe (bmp_targets, &bmpbgp->targets, bt) {
		frr_each_safe (bmp_listeners, &bt->listeners, bl)
			bmp_listener_put(bl);

		bmp_targets_put(bt);
	}

	bmp_mirrorq_fini(&bmpbgp->mirrorq);
	XFREE(MTYPE_BMP, bmpbgp);
}

static int bmp_bgp_del(struct bgp *bgp)
{
	struct bmp_bgp *bmpbgp = bmp_bgp_find(bgp);

	if (bmpbgp)
		bmp_bgp_put(bmpbgp);
	return 0;
}

static struct bmp_bgp_peer *bmp_bgp_peer_find(uint64_t peerid)
{
	struct bmp_bgp_peer dummy = { .peerid = peerid };
	return bmp_peerh_find(&bmp_peerh, &dummy);
}

static struct bmp_bgp_peer *bmp_bgp_peer_get(struct peer *peer)
{
	struct bmp_bgp_peer *bbpeer;

	bbpeer = bmp_bgp_peer_find(peer->qobj_node.nid);
	if (bbpeer)
		return bbpeer;

	bbpeer = XCALLOC(MTYPE_BMP_PEER, sizeof(*bbpeer));
	bbpeer->peerid = peer->qobj_node.nid;
	bmp_peerh_add(&bmp_peerh, bbpeer);

	return bbpeer;
}

static struct bmp_targets *bmp_targets_find1(struct bgp *bgp, const char *name)
{
	struct bmp_bgp *bmpbgp = bmp_bgp_find(bgp);
	struct bmp_targets dummy;

	if (!bmpbgp)
		return NULL;
	dummy.name = (char *)name;
	return bmp_targets_find(&bmpbgp->targets, &dummy);
}

static struct bmp_targets *bmp_targets_get(struct bgp *bgp, const char *name)
{
	struct bmp_targets *bt;

	bt = bmp_targets_find1(bgp, name);
	if (bt)
		return bt;

	bt = XCALLOC(MTYPE_BMP_TARGETS, sizeof(*bt));
	bt->name = XSTRDUP(MTYPE_BMP_TARGETSNAME, name);
	bt->bgp = bgp;
	bt->bmpbgp = bmp_bgp_get(bgp);
	bmp_session_init(&bt->sessions);
	bmp_qhash_init(&bt->mon_in_updhash);
	bmp_qlist_init(&bt->mon_in_updlist);
	bmp_qhash_init(&bt->mon_loc_updhash);
	bmp_qlist_init(&bt->mon_loc_updlist);
	bmp_qhash_init(&bt->mon_out_updhash);
	bmp_qlist_init(&bt->mon_out_updlist);
	bmp_actives_init(&bt->actives);
	bmp_listeners_init(&bt->listeners);

	QOBJ_REG(bt, bmp_targets);
	bmp_targets_add(&bt->bmpbgp->targets, bt);
	return bt;
}

static void bmp_targets_put(struct bmp_targets *bt)
{
	struct bmp *bmp;
	struct bmp_active *ba;

	THREAD_OFF(bt->t_stats);

	frr_each_safe (bmp_actives, &bt->actives, ba)
		bmp_active_put(ba);

	frr_each_safe(bmp_session, &bt->sessions, bmp) {
		bmp_close(bmp);
		bmp_free(bmp);
	}

	bmp_targets_del(&bt->bmpbgp->targets, bt);
	QOBJ_UNREG(bt);

	bmp_listeners_fini(&bt->listeners);
	bmp_actives_fini(&bt->actives);
	bmp_qhash_fini(&bt->mon_in_updhash);
	bmp_qlist_fini(&bt->mon_in_updlist);
	bmp_qhash_fini(&bt->mon_loc_updhash);
	bmp_qlist_fini(&bt->mon_loc_updlist);
	bmp_qhash_fini(&bt->mon_out_updhash);
	bmp_qlist_fini(&bt->mon_out_updlist);

	XFREE(MTYPE_BMP_ACLNAME, bt->acl_name);
	XFREE(MTYPE_BMP_ACLNAME, bt->acl6_name);
	bmp_session_fini(&bt->sessions);

	XFREE(MTYPE_BMP_TARGETSNAME, bt->name);
	XFREE(MTYPE_BMP_TARGETS, bt);
}

static struct bmp_listener *bmp_listener_find(struct bmp_targets *bt,
					      const union sockunion *su,
					      int port)
{
	struct bmp_listener dummy;
	dummy.addr = *su;
	dummy.port = port;
	return bmp_listeners_find(&bt->listeners, &dummy);
}

static struct bmp_listener *bmp_listener_get(struct bmp_targets *bt,
					     const union sockunion *su,
					     int port)
{
	struct bmp_listener *bl = bmp_listener_find(bt, su, port);

	if (bl)
		return bl;

	bl = XCALLOC(MTYPE_BMP_LISTENER, sizeof(*bl));
	bl->targets = bt;
	bl->addr = *su;
	bl->port = port;
	bl->sock = -1;

	bmp_listeners_add(&bt->listeners, bl);
	return bl;
}

static void bmp_listener_start(struct bmp_listener *bl)
{
	int sock, ret;

	sock = socket(bl->addr.sa.sa_family, SOCK_STREAM, 0);
	if (sock < 0)
		return;

	sockopt_reuseaddr(sock);
	sockopt_reuseport(sock);
	sockopt_v6only(bl->addr.sa.sa_family, sock);
	set_cloexec(sock);

	ret = sockunion_bind(sock, &bl->addr, bl->port, &bl->addr);
	if (ret < 0)
		goto out_sock;

	ret = listen(sock, 3);
	if (ret < 0)
		goto out_sock;

	bl->sock = sock;
	thread_add_read(bm->master, bmp_accept, bl, sock, &bl->t_accept);
	return;
out_sock:
	close(sock);
}

static void bmp_listener_stop(struct bmp_listener *bl)
{
	THREAD_OFF(bl->t_accept);

	if (bl->sock != -1)
		close(bl->sock);
	bl->sock = -1;
}

static struct bmp_active *bmp_active_find(struct bmp_targets *bt,
					  const char *hostname, int port)
{
	struct bmp_active dummy;
	dummy.hostname = (char *)hostname;
	dummy.port = port;
	return bmp_actives_find(&bt->actives, &dummy);
}

static struct bmp_active *bmp_active_get(struct bmp_targets *bt,
					 const char *hostname, int port)
{
	struct bmp_active *ba;

	ba = bmp_active_find(bt, hostname, port);
	if (ba)
		return ba;

	ba = XCALLOC(MTYPE_BMP_ACTIVE, sizeof(*ba));
	ba->targets = bt;
	ba->hostname = XSTRDUP(MTYPE_TMP, hostname);
	ba->port = port;
	ba->minretry = BMP_DFLT_MINRETRY;
	ba->maxretry = BMP_DFLT_MAXRETRY;
	ba->socket = -1;

	bmp_actives_add(&bt->actives, ba);
	return ba;
}

static void bmp_active_put(struct bmp_active *ba)
{
	THREAD_OFF(ba->t_timer);
	THREAD_OFF(ba->t_read);
	THREAD_OFF(ba->t_write);

	bmp_actives_del(&ba->targets->actives, ba);

	if (ba->bmp) {
		ba->bmp->active = NULL;
		bmp_close(ba->bmp);
		bmp_free(ba->bmp);
	}
	if (ba->socket != -1)
		close(ba->socket);

	XFREE(MTYPE_TMP, ba->ifsrc);
	XFREE(MTYPE_TMP, ba->hostname);
	XFREE(MTYPE_BMP_ACTIVE, ba);
}

static void bmp_active_setup(struct bmp_active *ba);

static void bmp_active_connect(struct bmp_active *ba)
{
	enum connect_result res;
	struct interface *ifp;
	vrf_id_t vrf_id = VRF_DEFAULT;
	int res_bind;

	for (; ba->addrpos < ba->addrtotal; ba->addrpos++) {
		if (ba->ifsrc) {
			if (ba->targets && ba->targets->bgp)
				vrf_id = ba->targets->bgp->vrf_id;

			/* find interface and related */
			/* address with same family   */
			ifp = if_lookup_by_name(ba->ifsrc, vrf_id);
			if (!ifp) {
				zlog_warn("bmp[%s]: failed to find interface",
					  ba->ifsrc);
				continue;
			}

			if (bgp_update_address(ifp, &ba->addrs[ba->addrpos],
					       &ba->addrsrc)){
				zlog_warn("bmp[%s]: failed to find matching address",
					  ba->ifsrc);
				continue;
			}
			zlog_info("bmp[%s]: selected source address : %pSU",
				  ba->ifsrc, &ba->addrsrc);
		}

		ba->socket = sockunion_socket(&ba->addrs[ba->addrpos]);
		if (ba->socket < 0) {
			zlog_warn("bmp[%s]: failed to create socket",
				  ba->hostname);
			continue;
		}

		set_nonblocking(ba->socket);

		if (!sockunion_is_null(&ba->addrsrc)) {
			res_bind = sockunion_bind(ba->socket, &ba->addrsrc, 0,
						  &ba->addrsrc);
			if (res_bind < 0) {
				zlog_warn(
					"bmp[%s]: no bind currently to source address %pSU:%d",
					ba->hostname, &ba->addrsrc, ba->port);
				close(ba->socket);
				ba->socket = -1;
				sockunion_init(&ba->addrsrc);
				continue;
			}
		}


		res = sockunion_connect(ba->socket, &ba->addrs[ba->addrpos],
				      htons(ba->port), 0);
		switch (res) {
		case connect_error:
			zlog_warn("bmp[%s]: failed to connect to %pSU:%d",
				  ba->hostname, &ba->addrs[ba->addrpos],
				  ba->port);
			close(ba->socket);
			ba->socket = -1;
			sockunion_init(&ba->addrsrc);
			continue;
		case connect_success:
			zlog_info("bmp[%s]: connected to  %pSU:%d",
				  ba->hostname, &ba->addrs[ba->addrpos],
				  ba->port);
			break;
		case connect_in_progress:
			zlog_warn("bmp[%s]: connect in progress  %pSU:%d",
				  ba->hostname, &ba->addrs[ba->addrpos],
				  ba->port);
			bmp_active_setup(ba);
			return;
		}
	}

	/* exhausted all addresses */
	ba->curretry += ba->curretry / 2;
	bmp_active_setup(ba);
}

static void bmp_active_resolved(struct resolver_query *resq, const char *errstr,
				int numaddrs, union sockunion *addr)
{
	struct bmp_active *ba = container_of(resq, struct bmp_active, resq);
	unsigned i;

	if (numaddrs <= 0) {
		zlog_warn("bmp[%s]: hostname resolution failed: %s",
			  ba->hostname, errstr);
		ba->last_err = errstr;
		ba->curretry += ba->curretry / 2;
		ba->addrpos = 0;
		ba->addrtotal = 0;
		bmp_active_setup(ba);
		return;
	}

	if (numaddrs > (int)array_size(ba->addrs))
		numaddrs = array_size(ba->addrs);

	ba->addrpos = 0;
	ba->addrtotal = numaddrs;
	for (i = 0; i < ba->addrtotal; i++)
		memcpy(&ba->addrs[i], &addr[i], sizeof(ba->addrs[0]));

	bmp_active_connect(ba);
}

static void bmp_active_thread(struct thread *t)
{
	struct bmp_active *ba = THREAD_ARG(t);
	socklen_t slen;
	int status, ret;
	vrf_id_t vrf_id;

	/* all 3 end up here, though only timer or read+write are active
	 * at a time */
	THREAD_OFF(ba->t_timer);
	THREAD_OFF(ba->t_read);
	THREAD_OFF(ba->t_write);

	ba->last_err = NULL;

	if (ba->socket == -1) {
		/* get vrf_id */
		if (!ba->targets || !ba->targets->bgp)
			vrf_id = VRF_DEFAULT;
		else
			vrf_id = ba->targets->bgp->vrf_id;
		resolver_resolve(&ba->resq, AF_UNSPEC, vrf_id, ba->hostname,
				 bmp_active_resolved);
		return;
	}

	slen = sizeof(status);
	ret = getsockopt(ba->socket, SOL_SOCKET, SO_ERROR, (void *)&status,
			 &slen);

	if (ret < 0 || status != 0) {
		ba->last_err = strerror(status);
		zlog_warn("bmp[%s]: failed to connect to %pSU:%d: %s",
			  ba->hostname, &ba->addrs[ba->addrpos], ba->port,
			  ba->last_err);
		goto out_next;
	}

	zlog_warn("bmp[%s]: outbound connection to %pSU:%d", ba->hostname,
		  &ba->addrs[ba->addrpos], ba->port);

	ba->bmp = bmp_open(ba->targets, ba->socket);
	if (!ba->bmp)
		goto out_next;

	ba->bmp->active = ba;
	ba->socket = -1;
	ba->curretry = ba->minretry;
	return;

out_next:
	close(ba->socket);
	ba->socket = -1;
	ba->addrpos++;
	bmp_active_connect(ba);
}

static void bmp_active_disconnected(struct bmp_active *ba)
{
	ba->bmp = NULL;
	bmp_active_setup(ba);
}

static void bmp_active_setup(struct bmp_active *ba)
{
	THREAD_OFF(ba->t_timer);
	THREAD_OFF(ba->t_read);
	THREAD_OFF(ba->t_write);

	if (ba->bmp)
		return;
	if (ba->resq.callback)
		return;

	if (ba->curretry > ba->maxretry)
		ba->curretry = ba->maxretry;

	if (ba->socket == -1)
		thread_add_timer_msec(bm->master, bmp_active_thread, ba,
				      ba->curretry, &ba->t_timer);
	else {
		thread_add_read(bm->master, bmp_active_thread, ba, ba->socket,
				&ba->t_read);
		thread_add_write(bm->master, bmp_active_thread, ba, ba->socket,
				&ba->t_write);
	}
}

static struct cmd_node bmp_node = {
	.name = "bmp",
	.node = BMP_NODE,
	.parent_node = BGP_NODE,
	.prompt = "%s(config-bgp-bmp)# "
};

static void bmp_targets_autocomplete(vector comps, struct cmd_token *token)
{
	struct bgp *bgp;
	struct bmp_targets *target;
	struct listnode *node;

	for (ALL_LIST_ELEMENTS_RO(bm->bgp, node, bgp)) {
		struct bmp_bgp *bmpbgp = bmp_bgp_find(bgp);

		if (!bmpbgp)
			continue;

		frr_each_safe (bmp_targets, &bmpbgp->targets, target)
			vector_set(comps,
				   XSTRDUP(MTYPE_COMPLETION, target->name));
	}
}

static const struct cmd_variable_handler bmp_targets_var_handlers[] = {
	{.tokenname = "BMPTARGETS", .completions = bmp_targets_autocomplete},
	{.completions = NULL}};

#define BMP_STR "BGP Monitoring Protocol\n"

#include "bgpd/bgp_bmp_clippy.c"

DEFPY_NOSH(bmp_targets_main,
      bmp_targets_cmd,
      "bmp targets BMPTARGETS",
      BMP_STR
      "Create BMP target group\n"
      "Name of the BMP target group\n")
{
	VTY_DECLVAR_CONTEXT(bgp, bgp);
	struct bmp_targets *bt;

	bt = bmp_targets_get(bgp, bmptargets);

	VTY_PUSH_CONTEXT_SUB(BMP_NODE, bt);
	return CMD_SUCCESS;
}

DEFPY(no_bmp_targets_main,
      no_bmp_targets_cmd,
      "no bmp targets BMPTARGETS",
      NO_STR
      BMP_STR
      "Delete BMP target group\n"
      "Name of the BMP target group\n")
{
	VTY_DECLVAR_CONTEXT(bgp, bgp);
	struct bmp_targets *bt;

	bt = bmp_targets_find1(bgp, bmptargets);
	if (!bt) {
		vty_out(vty, "%% BMP target group not found\n");
		return CMD_WARNING;
	}
	bmp_targets_put(bt);
	return CMD_SUCCESS;
}

DEFPY(bmp_listener_main,
      bmp_listener_cmd,
      "bmp listener <X:X::X:X|A.B.C.D> port (1-65535)",
      BMP_STR
      "Listen for inbound BMP connections\n"
      "IPv6 address to listen on\n"
      "IPv4 address to listen on\n"
      "TCP Port number\n"
      "TCP Port number\n")
{
	VTY_DECLVAR_CONTEXT_SUB(bmp_targets, bt);
	struct bmp_listener *bl;

	bl = bmp_listener_get(bt, listener, port);
	if (bl->sock == -1)
		bmp_listener_start(bl);

	return CMD_SUCCESS;
}

DEFPY(no_bmp_listener_main,
      no_bmp_listener_cmd,
      "no bmp listener <X:X::X:X|A.B.C.D> port (1-65535)",
      NO_STR
      BMP_STR
      "Create BMP listener\n"
      "IPv6 address to listen on\n"
      "IPv4 address to listen on\n"
      "TCP Port number\n"
      "TCP Port number\n")
{
	VTY_DECLVAR_CONTEXT_SUB(bmp_targets, bt);
	struct bmp_listener *bl;

	bl = bmp_listener_find(bt, listener, port);
	if (!bl) {
		vty_out(vty, "%% BMP listener not found\n");
		return CMD_WARNING;
	}
	bmp_listener_stop(bl);
	bmp_listener_put(bl);
	return CMD_SUCCESS;
}

DEFPY(bmp_connect,
      bmp_connect_cmd,
      "[no] bmp connect HOSTNAME port (1-65535) {min-retry (100-86400000)|max-retry (100-86400000)} [source-interface <WORD$srcif>]",
      NO_STR
      BMP_STR
      "Actively establish connection to monitoring station\n"
      "Monitoring station hostname or address\n"
      "TCP port\n"
      "TCP port\n"
      "Minimum connection retry interval\n"
      "Minimum connection retry interval (milliseconds)\n"
      "Maximum connection retry interval\n"
      "Maximum connection retry interval (milliseconds)\n"
      "Source interface to use\n"
      "Define an interface\n")
{
	VTY_DECLVAR_CONTEXT_SUB(bmp_targets, bt);
	struct bmp_active *ba;

	if (no) {
		ba = bmp_active_find(bt, hostname, port);
		if (!ba) {
			vty_out(vty, "%% No such active connection found\n");
			return CMD_WARNING;
		}
		/* connection deletion need same hostname port and interface */
		if (ba->ifsrc || srcif)
			if ((!ba->ifsrc) || (!srcif) ||
			    !strcmp(ba->ifsrc, srcif)) {
				vty_out(vty,
					"%% No such active connection found\n");
				return CMD_WARNING;
			}
		bmp_active_put(ba);
		return CMD_SUCCESS;
	}

	ba = bmp_active_get(bt, hostname, port);
	if (srcif)
		ba->ifsrc = XSTRDUP(MTYPE_TMP, srcif);
	if (min_retry_str)
		ba->minretry = min_retry;
	if (max_retry_str)
		ba->maxretry = max_retry;
	ba->curretry = ba->minretry;
	bmp_active_setup(ba);

	return CMD_SUCCESS;
}

DEFPY(bmp_acl,
      bmp_acl_cmd,
      "[no] <ip|ipv6>$af access-list ACCESSLIST_NAME$access_list",
      NO_STR
      IP_STR
      IPV6_STR
      "Access list to restrict BMP sessions\n"
      "Access list name\n")
{
	VTY_DECLVAR_CONTEXT_SUB(bmp_targets, bt);
	char **what;

	if (no)
		access_list = NULL;
	if (!strcmp(af, "ipv6"))
		what = &bt->acl6_name;
	else
		what = &bt->acl_name;

	XFREE(MTYPE_BMP_ACLNAME, *what);
	if (access_list)
		*what = XSTRDUP(MTYPE_BMP_ACLNAME, access_list);

	return CMD_SUCCESS;
}

DEFPY(bmp_stats_cfg,
      bmp_stats_cmd,
      "[no] bmp stats [interval (100-86400000)]",
      NO_STR
      BMP_STR
      "Send BMP statistics messages\n"
      "Specify BMP stats interval\n"
      "Interval (milliseconds) to send BMP Stats in\n")
{
	VTY_DECLVAR_CONTEXT_SUB(bmp_targets, bt);

	THREAD_OFF(bt->t_stats);
	if (no)
		bt->stat_msec = 0;
	else if (interval_str)
		bt->stat_msec = interval;
	else
		bt->stat_msec = BMP_STAT_DEFAULT_TIMER;

	if (bt->stat_msec)
		thread_add_timer_msec(bm->master, bmp_stats, bt, bt->stat_msec,
				      &bt->t_stats);
	return CMD_SUCCESS;
}

DEFPY(bmp_monitor_cfg, bmp_monitor_cmd,
      "[no] bmp monitor <ipv4|ipv6|l2vpn> <unicast|multicast|evpn|vpn> <rib-in|loc-rib|rib-out>$rib [pre-policy|post-policy]$policy",
      NO_STR BMP_STR
      "Send BMP route monitoring messages\n" BGP_AF_STR BGP_AF_STR BGP_AF_STR
	      BGP_AF_MODIFIER_STR BGP_AF_MODIFIER_STR BGP_AF_MODIFIER_STR
		      BGP_AF_MODIFIER_STR
      "Monitor BGP Adj-RIB-In\n"
      "Monitor BGP Local-RIB\n"
      "Monitor BGP Adj-RIB-Out\n"
      "Send state of Adj-RIB-In/out before in/outbound policy is applied\n"
      "Send state of Adj-RIB-In/out after in/outbound policy is applied\n"
      )
{
	int index = 0;
	uint8_t flag, prev;
	afi_t afi;
	safi_t safi;

	VTY_DECLVAR_CONTEXT_SUB(bmp_targets, bt);
	struct bmp *bmp;

	argv_find_and_parse_afi(argv, argc, &index, &afi);
	argv_find_and_parse_safi(argv, argc, &index, &safi);

	if (!policy && rib[0] == 'l') {
		flag = BMP_MON_LOC_RIB;
	} else if (policy && rib[4] == 'i') {
		flag = policy[1] == 'r' ? BMP_MON_IN_PREPOLICY
						  : BMP_MON_IN_POSTPOLICY;
	} else if (policy && rib[4] == 'o') {
		flag = policy[1] == 'r' ? BMP_MON_OUT_PREPOLICY
						  : BMP_MON_OUT_POSTPOLICY;
	} else {
		vty_out(vty, "%% Target RIB doesn't exist\n");
		return CMD_WARNING;
	}

	prev = bt->afimon[afi][safi];
	if (no)
		UNSET_FLAG(bt->afimon[afi][safi], flag);
	else
		SET_FLAG(bt->afimon[afi][safi], flag);

	if (prev == bt->afimon[afi][safi])
		return CMD_SUCCESS;

	frr_each (bmp_session, &bt->sessions, bmp) {
		if (bmp->syncafi == afi && bmp->syncsafi == safi) {
			bmp->syncafi = AFI_MAX;
			bmp->syncsafi = SAFI_MAX;
		}

		if (!bt->afimon[afi][safi]) {
			bmp->afistate[afi][safi] = BMP_AFI_INACTIVE;
			continue;
		}

		bmp->afistate[afi][safi] = BMP_AFI_NEEDSYNC;

		pullwr_bump(bmp->pullwr);
	}

	return CMD_SUCCESS;
}

DEFPY(bmp_mirror_cfg,
      bmp_mirror_cmd,
      "[no] bmp mirror",
      NO_STR
      BMP_STR
      "Send BMP route mirroring messages\n")
{
	VTY_DECLVAR_CONTEXT_SUB(bmp_targets, bt);
	struct bmp *bmp;

	if (bt->mirror == !no)
		return CMD_SUCCESS;

	bt->mirror = !no;
	if (bt->mirror)
		return CMD_SUCCESS;

	frr_each (bmp_session, &bt->sessions, bmp) {
		struct bmp_mirrorq *bmq;

		while ((bmq = bmp_pull_mirror(bmp)))
			if (!bmq->refcount)
				XFREE(MTYPE_BMP_MIRRORQ, bmq);
	}
	return CMD_SUCCESS;
}

DEFPY(bmp_mirror_limit_cfg,
      bmp_mirror_limit_cmd,
      "bmp mirror buffer-limit (0-4294967294)",
      BMP_STR
      "Route Mirroring settings\n"
      "Configure maximum memory used for buffered mirroring messages\n"
      "Limit in bytes\n")
{
	VTY_DECLVAR_CONTEXT(bgp, bgp);
	struct bmp_bgp *bmpbgp;

	bmpbgp = bmp_bgp_get(bgp);
	bmpbgp->mirror_qsizelimit = buffer_limit;

	return CMD_SUCCESS;
}

DEFPY(no_bmp_mirror_limit_cfg,
      no_bmp_mirror_limit_cmd,
      "no bmp mirror buffer-limit [(0-4294967294)]",
      NO_STR
      BMP_STR
      "Route Mirroring settings\n"
      "Configure maximum memory used for buffered mirroring messages\n"
      "Limit in bytes\n")
{
	VTY_DECLVAR_CONTEXT(bgp, bgp);
	struct bmp_bgp *bmpbgp;

	bmpbgp = bmp_bgp_get(bgp);
	bmpbgp->mirror_qsizelimit = ~0UL;

	return CMD_SUCCESS;
}

DEFPY(bmp_startup_delay_cfg,
      bmp_startup_delay_cmd,
      "[no] bmp startup-delay [(0-4294967294)]$startup_delay",
      NO_STR BMP_STR
      "Configure delay before BMP starts sending monitoring and mirroring messages\n"
      "Time in milliseconds\n")
{
	VTY_DECLVAR_CONTEXT(bgp, bgp);
	struct bmp_bgp *bmpbgp;

	if (!no && startup_delay < 0) {
		vty_out(vty, "Missing startup delay parameter\n");
		return CMD_ERR_INCOMPLETE;
	}

	bmpbgp = bmp_bgp_get(bgp);
	bmpbgp->startup_delay_ms = !no ? startup_delay : 0;

	return CMD_SUCCESS;
}

/* show the classic 'show bmp' information */
static void bmp_show_bmp(struct vty *vty) {

	struct bmp_bgp *bmpbgp;
	struct bmp_targets *bt;
	struct bmp_listener *bl;
	struct bmp_active *ba;
	struct bmp *bmp;
	struct ttable *tt;
	char uptime[BGP_UPTIME_LEN];
	char *out;

	vty_out(vty, "BMP Module started at %pTVM\n\n", &bmp_startup_time);
	frr_each(bmp_bgph, &bmp_bgph, bmpbgp) {
		vty_out(vty, "BMP state for BGP %s:\n\n",
			bmpbgp->bgp->name_pretty);
		vty_out(vty, "  Route Mirroring %9zu bytes (%zu messages) pending\n",
			bmpbgp->mirror_qsize,
			bmp_mirrorq_count(&bmpbgp->mirrorq));
		vty_out(vty, "                  %9zu bytes maximum buffer used\n",
			bmpbgp->mirror_qsizemax);
		if (bmpbgp->mirror_qsizelimit != ~0UL)
			vty_out(vty, "                  %9zu bytes buffer size limit\n",
				bmpbgp->mirror_qsizelimit);
		vty_out(vty, "\n");

		vty_out(vty, "  Startup delay : %s",
			bmpbgp->startup_delay_ms == 0 ? "Immediate\n\n" : "");
		if (bmpbgp->startup_delay_ms != 0)
			vty_out(vty, "%"PRIu32"ms\n\n", bmpbgp->startup_delay_ms);

		frr_each(bmp_targets, &bmpbgp->targets, bt) {
			vty_out(vty, "  Targets \"%s\":\n", bt->name);
			vty_out(vty, "    Route Mirroring %sabled\n",
				bt->mirror ? "en" : "dis");

			afi_t afi;
			safi_t safi;

			FOREACH_AFI_SAFI (afi, safi) {

				uint8_t afimon_flag = bt->afimon[afi][safi];

				if (!afimon_flag)
					continue;

				const char *in_pre_str =
					CHECK_FLAG(afimon_flag, BMP_MON_IN_PREPOLICY)
						? "rib-in pre-policy "
						: "";
				const char *in_post_str =
					CHECK_FLAG(afimon_flag, BMP_MON_IN_POSTPOLICY)
						? "rib-in post-policy "
						: "";
				const char *locrib_str =
					CHECK_FLAG(afimon_flag, BMP_MON_LOC_RIB)
						? "loc-rib "
						: "";
				const char *out_pre_str =
					CHECK_FLAG(afimon_flag, BMP_MON_OUT_PREPOLICY)
						? "rib-out pre-policy "
						: "";
				const char *out_post_str =
					CHECK_FLAG(afimon_flag, BMP_MON_OUT_POSTPOLICY)
						? "rib-out post-policy"
						: "";

				vty_out(vty,
					"    Route Monitoring %s %s %s%s%s%s%s\n",
					afi2str(afi), safi2str(safi), in_pre_str,
					in_post_str, locrib_str, out_pre_str, out_post_str);
			}

			vty_out(vty, "    Listeners:\n");
			frr_each (bmp_listeners, &bt->listeners, bl)
				vty_out(vty, "      %pSU:%d\n", &bl->addr,
					bl->port);

			vty_out(vty, "\n    Outbound connections:\n");
			tt = ttable_new(&ttable_styles[TTSTYLE_BLANK]);
			ttable_add_row(tt, "remote|state||timer|local");
			ttable_rowseps(tt, 0, BOTTOM, true, '-');
			frr_each (bmp_actives, &bt->actives, ba) {
				const char *state_str = "?";

				if (ba->bmp) {
					peer_uptime(ba->bmp->t_up.tv_sec,
						    uptime, sizeof(uptime),
						    false, NULL);
					ttable_add_row(tt,
						       "%s:%d|Up|%s|%s|%pSU",
						       ba->hostname, ba->port,
						       ba->bmp->remote, uptime,
						       &ba->addrsrc);
					continue;
				}

				uptime[0] = '\0';

				if (ba->t_timer) {
					long trem = thread_timer_remain_second(
						ba->t_timer);

					peer_uptime(monotime(NULL) - trem,
						    uptime, sizeof(uptime),
						    false, NULL);
					state_str = "RetryWait";
				} else if (ba->t_read) {
					state_str = "Connecting";
				} else if (ba->resq.callback) {
					state_str = "Resolving";
				}

				ttable_add_row(tt, "%s:%d|%s|%s|%s|%pSU",
					       ba->hostname, ba->port,
					       state_str,
					       ba->last_err ? ba->last_err : "",
					       uptime, &ba->addrsrc);
			}
			out = ttable_dump(tt, "\n");
			vty_out(vty, "%s", out);
			XFREE(MTYPE_TMP, out);
			ttable_del(tt);

			vty_out(vty, "\n    %zu connected clients:\n",
					bmp_session_count(&bt->sessions));
			tt = ttable_new(&ttable_styles[TTSTYLE_BLANK]);
			ttable_add_row(tt, "remote|uptime|state|MonSent|MirrSent|MirrLost|ByteSent|ByteQ|ByteQKernel");
			ttable_rowseps(tt, 0, BOTTOM, true, '-');

			frr_each (bmp_session, &bt->sessions, bmp) {
				uint64_t total;
				size_t q, kq;

				pullwr_stats(bmp->pullwr, &total, &q, &kq);

				peer_uptime(bmp->t_up.tv_sec, uptime,
					    sizeof(uptime), false, NULL);

				ttable_add_row(tt, "%s|%s|%s|%Lu|%Lu|%Lu|%Lu|%zu|%zu",
					       bmp->remote, uptime,
					       bmp_state_str(bmp->state),
					       bmp->cnt_update,
					       bmp->cnt_mirror,
					       bmp->cnt_mirror_overruns,
					       total, q, kq);
			}
			out = ttable_dump(tt, "\n");
			vty_out(vty, "%s", out);
			XFREE(MTYPE_TMP, out);
			ttable_del(tt);
			vty_out(vty, "\n");
		}
	}

}

/* show the bpi locked by bmp with 'show bmp locked' */
static void bmp_show_locked(struct vty *vty) {

	vty_out(vty, "BMP: BGP Paths locked for use in the Monitoring\n");
	struct bmp_bpi_lock *lbpi_iter;
	frr_each(bmp_lbpi_h, &bmp_lbpi, lbpi_iter) {
		if (!lbpi_iter)
			continue;

		vty_out(vty, "Bucket:\n");

		int n = 0;
		struct bmp_bpi_lock *lbpi_curr = lbpi_iter;
		do {
			if (!lbpi_curr->locked) {
				vty_out(vty, " [%d] Empty node\n", n);
				continue;
			}

			vty_out(vty, " [#%d][lock=%d] %s: bgp id=%"PRId64" dest=%pRN rx_id=%"PRIu32" from peer=%pBP\n",
				n, lbpi_curr->lock, n == 0 ? "head" : "node",
				lbpi_curr->bgp ? (int64_t)lbpi_curr->bgp->vrf_id : -1,
				lbpi_curr->locked->net,
				lbpi_curr->locked->addpath_rx_id,
				lbpi_curr->locked->peer);
			n++;
		} while ((lbpi_curr = lbpi_curr->next));
	}
}

/* command router for 'show bmp' command */
DEFPY(show_bmp,
      show_bmp_cmd,
      "show bmp [locked]$locked",
      SHOW_STR
      BMP_STR
      "Display information specific to paths locked by BMP\n")
{
	if (locked)
		bmp_show_locked(vty);
	else
		bmp_show_bmp(vty);

	return CMD_SUCCESS;
}

static int bmp_config_write(struct bgp *bgp, struct vty *vty)
{
	struct bmp_bgp *bmpbgp = bmp_bgp_find(bgp);
	struct bmp_targets *bt;
	struct bmp_listener *bl;
	struct bmp_active *ba;
	afi_t afi;
	safi_t safi;

	if (!bmpbgp)
		return 0;

	if (bmpbgp->mirror_qsizelimit != ~0UL)
		vty_out(vty, " !\n bmp mirror buffer-limit %zu\n",
			bmpbgp->mirror_qsizelimit);

	if (bmpbgp->startup_delay_ms != 0)
		vty_out(vty, " !\n bmp startup-delay %"PRIu32"\n",
			bmpbgp->startup_delay_ms);

	frr_each(bmp_targets, &bmpbgp->targets, bt) {
		vty_out(vty, " !\n bmp targets %s\n", bt->name);

		if (bt->acl6_name)
			vty_out(vty, "  ipv6 access-list %s\n", bt->acl6_name);
		if (bt->acl_name)
			vty_out(vty, "  ip access-list %s\n", bt->acl_name);

		if (bt->stat_msec)
			vty_out(vty, "  bmp stats interval %d\n",
					bt->stat_msec);

		if (bt->mirror)
			vty_out(vty, "  bmp mirror\n");

		FOREACH_AFI_SAFI (afi, safi) {
			const char *afi_str = (afi == AFI_IP) ? "ipv4" : "ipv6";

			if (CHECK_FLAG(bt->afimon[afi][safi], BMP_MON_IN_PREPOLICY))
				vty_out(vty, "  bmp monitor %s %s rib-in pre-policy\n",
					afi_str, safi2str(safi));
			if (CHECK_FLAG(bt->afimon[afi][safi], BMP_MON_IN_POSTPOLICY))
				vty_out(vty, "  bmp monitor %s %s rib-in post-policy\n",
					afi_str, safi2str(safi));
			if (CHECK_FLAG(bt->afimon[afi][safi], BMP_MON_LOC_RIB))
				vty_out(vty, "  bmp monitor %s %s loc-rib\n",
					afi_str, safi2str(safi));
			if (CHECK_FLAG(bt->afimon[afi][safi], BMP_MON_OUT_PREPOLICY))
				vty_out(vty, "  bmp monitor %s %s rib-out pre-policy\n",
					afi_str, safi2str(safi));
			if (CHECK_FLAG(bt->afimon[afi][safi], BMP_MON_OUT_POSTPOLICY))
				vty_out(vty, "  bmp monitor %s %s rib-out post-policy\n",
					afi_str, safi2str(safi));
		}
		frr_each (bmp_listeners, &bt->listeners, bl)
			vty_out(vty, " \n  bmp listener %pSU port %d\n",
				&bl->addr, bl->port);

		frr_each (bmp_actives, &bt->actives, ba) {
			vty_out(vty, "  bmp connect %s port %u min-retry %u max-retry %u",
				ba->hostname, ba->port,
				ba->minretry, ba->maxretry);

			if (ba->ifsrc)
				vty_out(vty, " source-interface %s\n", ba->ifsrc);
			else
				vty_out(vty, "\n");
		}
		vty_out(vty, " exit\n");
	}

	return 0;
}

static int bgp_bmp_init(struct thread_master *tm)
{
	install_node(&bmp_node);
	install_default(BMP_NODE);

	cmd_variable_handler_register(bmp_targets_var_handlers);

	install_element(BGP_NODE, &bmp_targets_cmd);
	install_element(BGP_NODE, &no_bmp_targets_cmd);

	install_element(BMP_NODE, &bmp_listener_cmd);
	install_element(BMP_NODE, &no_bmp_listener_cmd);
	install_element(BMP_NODE, &bmp_connect_cmd);
	install_element(BMP_NODE, &bmp_acl_cmd);
	install_element(BMP_NODE, &bmp_stats_cmd);
	install_element(BMP_NODE, &bmp_monitor_cmd);
	install_element(BMP_NODE, &bmp_mirror_cmd);

	install_element(BGP_NODE, &bmp_mirror_limit_cmd);
	install_element(BGP_NODE, &no_bmp_mirror_limit_cmd);
	install_element(BGP_NODE, &bmp_startup_delay_cmd);

	install_element(VIEW_NODE, &show_bmp_cmd);

	resolver_init(tm);

	monotime(&bmp_startup_time);

	return 0;
}

/* this function is triggered when a route is updated is the BGP RIB
 * it puts a bmp_queue_entry in the loc-rib queue which will trigger a bmp
 * monitoring message for loc-rib based on config
 */
static int bmp_route_update(struct bgp *bgp, afi_t afi, safi_t safi,
			    struct bgp_dest *bn,
			    struct bgp_path_info *old_route,
			    struct bgp_path_info *new_route)
{
	bool is_locribmon_enabled = false;
	bool is_withdraw = old_route && !new_route;
	struct bgp_path_info *updated_route =
		is_withdraw ? old_route : new_route;
	struct bmp_bgp *bmpbgp = bmp_bgp_get(bgp);
	struct bmp_targets *bt;
	struct bmp *bmp;

	/* lock the bpi in case of withdraw for rib-out pre-policy
	 * do this unconditionally because bmp_path_unlock hook will always be
	 * called whether rib-out mon is configured or not and this avoids problems
	 * in case of configuration changes between lock and unlock calls
	 */
	if (is_withdraw)
		bmp_lock_bpi(bgp, updated_route);

	frr_each (bmp_targets, &bmpbgp->targets, bt) {
		if ((is_locribmon_enabled |=
		     (CHECK_FLAG(bt->afimon[afi][safi], BMP_MON_LOC_RIB))))
			break;
	}

	/* dont waste time recording the loc-rib install time if not configured
	 */
	if (!is_locribmon_enabled)
		return 0;

	/* route is not installed in loc-rib anymore and rib uptime was saved */
	if (old_route && old_route->extra)
		bgp_path_info_extra_get(old_route)->bgp_rib_uptime =
			(time_t)(-1L);

	/* route is installed in loc-rib from now on so
	 * save rib uptime in bgp_path_info_extra
	 */
	if (new_route)
		bgp_path_info_extra_get(new_route)->bgp_rib_uptime =
			monotime(NULL);


	frr_each (bmp_targets, &bmpbgp->targets, bt) {
		if (!CHECK_FLAG(bt->afimon[afi][safi],
			       BMP_MON_LOC_RIB))
			continue;

		struct bmp_queue_entry *new_head = NULL;

		/* send withdraw for previously selected best-path in case of
		 * best path change
		 */
		if (old_route && new_route && old_route != new_route) {
			new_head = bmp_process_one(
				bt, &bt->mon_loc_updhash, &bt->mon_loc_updlist,
				bgp, afi, safi, bn, old_route->addpath_rx_id,
				old_route->peer, BMP_MON_LOC_RIB);
		}

		struct bmp_queue_entry *new_item = bmp_process_one(
			bt, &bt->mon_loc_updhash, &bt->mon_loc_updlist, bgp,
			afi, safi, bn, updated_route->addpath_rx_id,
			updated_route->peer, BMP_MON_LOC_RIB);
		new_head = !new_head ? new_item : new_head;

		/* if bmp_process_one returns NULL
		 * we don't have anything to do next
		 */
		if (!new_head)
			continue;

		frr_each (bmp_session, &bt->sessions, bmp) {
			if (!bmp->mon_loc_queuepos)
				bmp->mon_loc_queuepos = new_head;

			pullwr_bump(bmp->pullwr);
		};
	};

	return 0;
}

/* this function is triggered when a change has been registered in the
 * adj-rib-out. it puts a bmp_queue_entry in the rib-out queue
 * and which will trigger a bmp monitoring message for rib-out pre/post-policy
 * if either is configured.
 */
static int bmp_adj_out_changed(struct update_subgroup *subgrp,
			       struct bgp_dest *dest,
			       struct bgp_path_info *locked_path,
			       uint32_t addpath_id, struct attr *attr,
			       bool post_policy, bool withdraw) {

	if (!subgrp)
		return 0;

	/* for rib-out pre-policy we need to run bgp conditions check to know
	 * if a path is in adj-rib-out pre or not */
	if (!post_policy) {

		/* because of withdraw we don't have the path right away,
		 * look it up in bmp locked paths */
		if (withdraw && !locked_path) {
			{ /* scope lock the vars declared by lookup */
				BMP_LBPI_LOOKUP_DEST(
					head, prev, lbpi, dest,
					SUBGRP_INST(subgrp),
					bgp_addpath_id_for_peer(
						SUBGRP_PEER(subgrp),
						SUBGRP_AFI(subgrp),
						SUBGRP_SAFI(subgrp),
						&lbpi->locked->tx_addpath) ==
						addpath_id);

				if (!lbpi) {
					zlog_warn(
						"no locked path found for %pRN tx %" PRIu32,
						dest, addpath_id);
					return 0;
				}

				locked_path = lbpi->locked;
			}
		}

		struct attr dummy_attr = {0};
		/*
		 * withdraw 	| pre_check	| result
		 * true		| true		| withdraw
		 * true		| false		| nothing to do
		 * false	| true		| update
		 * false	| false		| nothing to do
		 */

		/* run bgp rib-out-pre check */
		if (locked_path &&
		    !subgroup_announce_check(dest, locked_path, subgrp,
					     &dest->p, &dummy_attr, NULL,
					     BGP_ANNCHK_SPECIAL_PREPOLICY)) {
			return 0;
		}
	}

	struct bmp_targets *bt;
	struct bgp *bgp = SUBGRP_INST(subgrp);
	struct bmp_bgp *bmpbgp = bmp_bgp_get(bgp);
	afi_t afi = SUBGRP_AFI(subgrp);
	safi_t safi = SUBGRP_SAFI(subgrp);

	uint8_t mon_flag =
		post_policy ?
			    BMP_MON_OUT_POSTPOLICY
			    : BMP_MON_OUT_PREPOLICY;

	struct peer_af *paf;
	struct peer *peer;
	struct bmp *bmp;
	frr_each (bmp_targets, &bmpbgp->targets, bt) {
		if (!CHECK_FLAG(bt->afimon[afi][safi], mon_flag))
			continue;

		SUBGRP_FOREACH_PEER (subgrp, paf) {
			peer = PAF_PEER(paf);

			struct bmp_queue_entry *new_item = bmp_process_one(
				bt, &bt->mon_out_updhash, &bt->mon_out_updlist,
				NULL, afi, safi, dest, addpath_id, peer,
				mon_flag);

			/* if bmp_process_one returns NULL
			 * we don't have anything to do next
			 */
			if (!new_item)
				continue;

			frr_each (bmp_session, &bt->sessions, bmp) {
				if (!bmp->mon_out_queuepos)
					bmp->mon_out_queuepos = new_item;

				// TODO avoid bumping for each update ?
				pullwr_bump(bmp->pullwr);
			};
		}
	};

	return 0;
}

/* called at the end of the bgp_process when we need to unlock the path */
static int bmp_path_unlock(struct bgp *bgp, struct bgp_path_info *path) {
	return bmp_unlock_bpi(bgp, path) == NULL;
}

static int bgp_bmp_module_init(void)
{
	hook_register(bgp_packet_dump, bmp_mirror_packet);
	hook_register(bgp_packet_send, bmp_outgoing_packet);
	hook_register(peer_status_changed, bmp_peer_status_changed);
	hook_register(peer_backward_transition, bmp_peer_backward);
	hook_register(bgp_process, bmp_process_ribinpre);
	hook_register(bgp_inst_config_write, bmp_config_write);
	hook_register(bgp_inst_delete, bmp_bgp_del);
	hook_register(frr_late_init, bgp_bmp_init);
	hook_register(bgp_process_main_one, bmp_process_ribinpost);
	hook_register(bgp_route_update, bmp_route_update);
	hook_register(bgp_adj_out_updated, bmp_adj_out_changed);
	hook_register(bgp_process_main_one_end, bmp_path_unlock);
	return 0;
}

FRR_MODULE_SETUP(.name = "bgpd_bmp", .version = FRR_VERSION,
		 .description = "bgpd BMP module",
		 .init = bgp_bmp_module_init,
);
