//
// Created by taayoma5 on 01.12.23.
//

#ifndef FRR_BGP_LPID_H
#define FRR_BGP_LPID_H

#include "lib/typesafe.h"

DECLARE_MGROUP(LPID);
DECLARE_MTYPE(LPID_BGP);

typedef uint8_t path_id;

/* TODO why not use this as universal bridge between ribs
 *  we can add backpointers to bgp_adj_in, bgp_path_info and bgp_adj_out
 *  and remove all lookups when trying to match adj-in paths
 *  with loc-rib or adj-out paths
 */
struct local_path_id {
	int lock;
	pid_t process_id;
	vrf_id_t vrf_id;
	path_id path_id;
};

typedef uint32_t path_id_alloc_segment;

/* must be the first bit set function for the right type of path_id_alloc_segment */
#define LPID_FFS(x) (ffs((int)(x)))

/* maximum amount of LPID that can be allocated. biggest id is LPID_MAX - 1 */
#define LPID_MAX (1 << sizeof(path_id) * 8)

#define LPID_SEGSIZE (sizeof(path_id_alloc_segment) * 8)

/* amount of segments of size sizeof(path_id_alloc_segment) bytes to store LPID_MAX lpids */
#define LPID_NSEG (LPID_MAX / LPID_SEGSIZE)

struct local_path_id_allocator {
	path_id_alloc_segment segments[LPID_NSEG];
	path_id allocated;
};

struct bgp_dest;
struct bgp;

extern struct local_path_id *local_path_id_allocate_bgp(struct bgp* bgp, struct bgp_dest *dest);
extern struct local_path_id *local_path_id_lock(struct local_path_id *lpid);
extern void local_path_id_free(struct bgp_dest *dest, struct local_path_id *lpid);
extern void local_path_id_unlock(struct bgp_dest *dest, struct local_path_id *lpid);

#endif //FRR_BGP_LPID_H
