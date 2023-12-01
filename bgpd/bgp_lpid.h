//
// Created by taayoma5 on 01.12.23.
//

#ifndef FRR_BGP_LPID_H
#define FRR_BGP_LPID_H

#include "lib/typesafe.h"

struct local_path_id {
	pid_t process_id;
	vrf_id_t vrf_id;
	uint8_t path_id;
};

struct bgp_dest;
struct bgp;

extern struct local_path_id *allocate_local_path_id(struct bgp* bgp, struct bgp_dest *adj);

#endif //FRR_BGP_LPID_H
