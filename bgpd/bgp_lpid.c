//
// Created by taayoma5 on 01.12.23.
//

#include <zebra.h>

#include "bgpd.h"

struct local_path_id *allocate_local_path_id(struct bgp *bgp, struct bgp_dest *dest) {

	uint8_t path_id = 0;

	struct bgp_adj_in *lookup = dest->adj_in;
	while (lookup) {
		if (path_id == 255) {
			zlog_info("path id range exhausted for %pBD", dest);
			return NULL;
		}

		zlog_info("allocate path id for prefix %pBD: lookup=%p, current=%"PRIu8, dest, lookup, path_id);
		if (lookup->lpid) {
			zlog_info("found id %"PRIu8, lookup->lpid->path_id);
		}
		/* O(N²), do better with sorted adj_in struct */
		if (lookup->lpid && lookup->lpid->path_id == path_id) {
			path_id++;
			lookup = dest->adj_in;
			zlog_info("new %"PRIu8", back at lookup=%p", path_id, lookup);
		} else {
			lookup = lookup->next;
		}
	}

	zlog_info("selected path id %"PRIu8, path_id);
	struct local_path_id *lpid = XCALLOC(MTYPE_BGP_ROUTE_EXTRA, sizeof(struct local_path_id));
	if (lpid) {
		lpid->path_id = path_id;
		lpid->vrf_id = bgp->vrf_id;
		lpid->process_id = getpid();
	}

	return lpid;
};