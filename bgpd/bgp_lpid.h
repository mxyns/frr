//
// Created by taayoma5 on 01.12.23.
//

#ifndef FRR_BGP_LPID_H
#define FRR_BGP_LPID_H

#include "lib/typesafe.h"

DEFINE_MGROUP(LPID, "Local Path-ID");
DEFINE_MTYPE(LPID, LPID_BGP, "BGP Local-Path ID");

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
