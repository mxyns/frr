//
// Created by taayoma5 on 01.12.23.
//

#include <zebra.h>

#include "bgpd/bgpd.h"
#include "bgpd/bgp_attr.h"
#include "bgpd/bgp_lpid.h"

DEFINE_MGROUP(LPID, "Local Path-ID");
DEFINE_MTYPE(LPID, LPID_BGP, "BGP Local-Path ID");

struct local_path_id *local_path_id_allocate_bgp(struct bgp *bgp, struct bgp_dest *dest) {

	assert(bgp && dest);

	struct local_path_id_allocator *alloc = &dest->lpid_allocator;

	if (alloc->allocated == LPID_MAX - 1) {
		printf("local-path-id range exhausted for dest %pBD\n", dest);
		return 0;
	}

	for (size_t segment_id = 0; segment_id < LPID_NSEG; segment_id++) {
		path_id_alloc_segment free_bitmask = ~alloc->segments[segment_id];
		int offset = LPID_FFS(free_bitmask);

		/* no bit found in free_bitmask => no spot available in segment
		 * check the next one
		 */
		if (offset == 0)
			continue;
		else {
			/* mark id as in use */
			alloc->segments[segment_id] |= 1 << (offset - 1);
			alloc->allocated += 1;

			path_id id = offset - 1 + LPID_SEGSIZE * segment_id;

			printf("selected path id %"PRIu8"\n", id);
			struct local_path_id *lpid = XCALLOC(MTYPE_LPID_BGP, sizeof(struct local_path_id));
			if (lpid) {
				lpid->path_id = id;
				lpid->vrf_id = bgp->vrf_id;
				lpid->process_id = getpid();
			}
			return lpid;
		}
	}

	return NULL;
};

struct local_path_id *local_path_id_lock(struct local_path_id *lpid)
{
	if (lpid)
		lpid->lock++;

	return lpid;
}

void local_path_id_free(struct bgp_dest* dest, struct local_path_id *lpid) {

	path_id id = lpid->path_id;
	size_t segment_id = id / LPID_SEGSIZE;

	struct local_path_id_allocator *alloc = &dest->lpid_allocator;

	/* mark the id as unused */
	path_id_alloc_segment bitmask = ~(1 << (id - (segment_id * LPID_SEGSIZE)));
	assert(alloc->segments[segment_id] & ~bitmask);
	alloc->segments[segment_id] &= bitmask;
	alloc->allocated -= 1;

	XFREE(MTYPE_LPID_BGP, lpid);
}

void local_path_id_unlock(struct bgp_dest *dest, struct local_path_id *lpid)
{
	assert(dest && lpid && lpid->lock > 0);
	lpid->lock--;

	if (lpid->lock == 0)
		local_path_id_free(dest, lpid);
}
