#ifndef _CSR_H_
#define _CSR_H_

#include "graph.h"

typedef unsigned long csr_offset_t;
typedef vertex_t csr_vertex_t;
typedef struct csr {

  csr_offset_t *oa;
  csr_vertex_t *na; 

} csr_t;

/*auxData for use during neighpop*/
//csr_offset_t CSR_offset_array[MAX_VTX];

/*auxData to serialize out at the end*/
//csr_offset_t CSR_offset_array_out[MAX_VTX];

//csr_vertex_t *CSR_neigh_array;


csr_offset_t *CSR_alloc_offset_array();

csr_t *CSR_alloc();
csr_offset_t *CSR_alloc_offset_array(unsigned long);
csr_vertex_t *CSR_alloc_neigh_array(unsigned long);

void CSR_count_neigh(void *);
void CSR_cumul_neigh_count();
void CSR_print_neigh_counts();
void CSR_alloc_neigh(unsigned long);
void CSR_neigh_pop(void *);
void CSR_out(char *,unsigned long,csr_t *);
#endif
