#include <stdio.h>
#include <stdlib.h>
#include "pb.h"
#include "csr.h"
#include "graph.h"

/*TODO: factor par_binread_{count | npop} into a generic binread that takes a
 * bin_ctx_t * and a function pointer to a function that works on a bin_ctx_t *
 * 
 * THen, move main to main.c and move all mention of CSR to csr.c or main.c
 * pb.c/.h is purely about EL and bins
 * */
int main(int argc, char *argv[]){

  el_t *el = init_el_file(argv[1]);


  printf("Binning...");
  bin_ctx_t *g_ctx = par_bin(el);
  printf("Done.\n");

  /*Done with binning. Now bin read*/


  /*This is an argument to CSR_count_neigh*/  
  csr_offset_t *g_oa = CSR_alloc_offset_array(MAX_VTX);
  g_ctx->data = (void*)g_oa;
  
  printf("Counting neighbors...");
  par_binread_generic(g_ctx, (void *(*)(void*))CSR_count_neigh);
  printf("Done.\n");

  /*This is returned by CSR_count_neigh via g_ctx->data*/  
  csr_offset_t *oa = (csr_offset_t *)g_ctx->data;


  /*Sequential accumulation round*/ 
  printf("Accumulating neighbor counts...");
  csr_t *csr = CSR_alloc();
  csr->na = CSR_alloc_neigh_array(el->num_edges);
  csr->oa = oa;

  /*Need a spare one of these for output later*/
  csr_offset_t *oa_out = CSR_alloc_offset_array(MAX_VTX);
  CSR_cumul_neigh_count(csr->oa, oa_out);
  printf("Done.\n");


  /*Done with bin read for neighbor count.  Now bin read for neighbor pop*/



  /*The CSR is an argument to CSR_count_neigh*/  
  g_ctx->data = (void*)csr;

  printf("Populating neighbors...");
  par_binread_generic(g_ctx, (void *(*)(void*))CSR_neigh_pop);
  printf("Done.\n");
 
 
  printf("Ejecting CSR...");
  /*Put the "output" OA into the CSR to use in output*/  
  csr->oa = oa_out;
  CSR_out(argv[2],el->num_edges,csr);
  printf("Done.\n");
 
}
