#ifndef _CSR_H_
#define _CSR_H_
void thd_CSR_count_neigh(void *);
void CSR_cumul_neigh_count();
void CSR_print_neigh_counts();
void CSR_alloc_neigh(unsigned long);
void thd_CSR_neigh_pop(void *);
void CSR_out(char *,unsigned long);
#endif
