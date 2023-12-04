//
// Created by taayoma5 on 01.12.23.
//

#include <zebra.h>

#include "bgpd/bgpd.h"
#include "bgpd/bgp_attr.h"

DECLARE_MTYPE(LPID_BGP);

struct local_path_id *local_path_id_allocate_bgp(struct bgp *bgp, struct bgp_dest *dest) {

	uint8_t path_id = 1;

	struct bgp_adj_in *lookup_adjin = dest->adj_in;
	struct bgp_path_info *lookup_rib = bgp_dest_get_bgp_path_info(dest);
	while (lookup_adjin || lookup_rib) {
		if (path_id == 255) {
			zlog_info("path id range exhausted for %pBD", dest);
			return NULL;
		}

		zlog_info("allocate path id for prefix %pBD: lookup_adjin=%p lookup_rib=%p current=%"PRIu8, dest, lookup_adjin, lookup_rib, path_id);
		if (lookup_adjin && lookup_adjin->lpid)
			zlog_info("found adjin id %"PRIu8, lookup_adjin->lpid->path_id);

		if (lookup_rib && lookup_rib->lpid)
			zlog_info("found rib id %"PRIu8, lookup_rib->lpid->path_id);

		/* TODO O(N²), do better with sorted adj_in struct */

		/* if we find the same id in either rib, change lookup id and start over */
		if ((lookup_adjin && lookup_adjin->lpid && lookup_adjin->lpid->path_id == path_id)
		    || (lookup_rib && lookup_rib->lpid && lookup_rib->lpid->path_id == path_id)) {
			path_id++;
			lookup_adjin = dest->adj_in;
			lookup_rib = bgp_dest_get_bgp_path_info(dest);
			zlog_info("new %"PRIu8", back at lookup_adjin=%p lookup_rib=%p", path_id,
				  lookup_adjin, lookup_rib);
		} else {
			if (lookup_adjin) {
				lookup_adjin = lookup_adjin->next;
				zlog_info("next adjin %p", lookup_adjin);
			}

			if (lookup_rib) {
				lookup_rib = lookup_rib->next;
				zlog_info("next rib %p", lookup_rib);
			}
		}
	}

	zlog_info("selected path id %"PRIu8, path_id);
	struct local_path_id *lpid = XCALLOC(MTYPE_LPID_BGP, sizeof(struct local_path_id));
	if (lpid) {
		lpid->path_id = path_id;
		lpid->vrf_id = bgp->vrf_id;
		lpid->process_id = getpid();
	}

	return lpid;
};

struct local_path_id *local_path_id_lock(struct local_path_id *lpid) {
	if (lpid)
		lpid->lock++;

	return lpid;
}

void local_path_id_free(struct local_path_id *lpid) {
	XFREE(MTYPE_LPID_BGP, lpid);
}

void local_path_id_unlock(struct local_path_id *lpid) {

	assert(lpid && lpid->lock > 0);
	lpid->lock--;

	if (lpid->lock == 0)
		local_path_id_free(lpid);
}