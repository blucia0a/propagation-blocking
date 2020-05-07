#ifndef _CSR_H_
#define _CSR_H_
/*auxData for use during neighpop*/
unsigned long CSR_offset_array[MAX_VTX];

/*auxData to serialize out at the end*/
unsigned long CSR_offset_array_out[MAX_VTX];

vertex_t *CSR_neigh_array;

void thd_CSR_count_neigh(void *);
void CSR_cumul_neigh_count();
void CSR_print_neigh_counts();
void CSR_alloc_neigh(unsigned long);
void thd_CSR_neigh_pop(void *);
void CSR_out(char *,unsigned long);
#endif
