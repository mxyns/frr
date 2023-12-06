//
// Created by taayoma5 on 01.12.23.
//

#ifndef FRR_BGP_LPID_H
#define FRR_BGP_LPID_H

#include "lib/typesafe.h"

DECLARE_MGROUP(LPID);
DECLARE_MTYPE(LPID_BGP);

/* TODO why not use this as universal bridge between ribs
 *  we can add backpointers to bgp_adj_in, bgp_path_info and bgp_adj_out
 *  and remove all lookups when trying to match adj-in paths
 *  with loc-rib or adj-out paths
 */
struct local_path_id {
	int lock;
	pid_t process_id;
	vrf_id_t vrf_id;
	uint8_t path_id;
};

struct bgp_dest;
struct bgp;

extern struct local_path_id *local_path_id_allocate_bgp(struct bgp* bgp, struct bgp_dest *dest);
extern struct local_path_id *local_path_id_lock(struct local_path_id *lpid);
extern void local_path_id_free(struct local_path_id *lpid);
extern void local_path_id_unlock(struct local_path_id *lpid);

#endif //FRR_BGP_LPID_H
