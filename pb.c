#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>

#include "pb.h"
#include "csr.h"
#include "graph.h"
#include "el.h"

/*Total number of edges in the graph*/
unsigned long num_edges;
/*Number of edges that a thread works on */
unsigned long thd_edges[NUM_THDS];

pthread_t thds[NUM_THDS];

int bin_sz[NUM_THDS][NUM_BINS];
bin_elem_t *bins[NUM_THDS][NUM_BINS];


/*Argument el is the thread's starting 
pointer into the global el array*/
void thd_bin_init(thd_binner_t *t){

  /*             cur           */ 
  /*        ...  \/            */
  /*el = [s,d,s,d,s,d,s,d,s...]*/
  /*     |------num_edges-----|*/
  int tid = t->tid;
  vertex_t *cur = (vertex_t *)(t->el_ptr);
  for(int i = 0; i < t->thd_edges; i++){ 

    vertex_t src = *cur;
    (bin_sz[tid][ src % NUM_BINS ]) ++;    
    cur = cur + 2; 

    /*
    vertex_t dst = *cur;
    cur++;
    */

  }

  for(int i = 0 ; i < NUM_BINS; i ++){

    /*An element of a bin contains a source key and an update value*/
    /*For now, these are both the size of a vertex_t*/
    bins[tid][i] = (bin_elem_t *)malloc( bin_sz[tid][i] * sizeof(bin_elem_t) );

  }

}


void thd_bin(void *v_thd_binner_t){

  thd_binner_t *t = (thd_binner_t*)v_thd_binner_t;

  /*
    Local counters for the bin this thread is currently
    processing
  */
  unsigned long bin_i[NUM_BINS];
  memset( bin_i, 0, NUM_BINS * sizeof(unsigned long));

  vertex_t *cur = (vertex_t *)t->el_ptr;
  for(int i = 0; i < t->thd_edges; i++){ 

    vertex_t src = *cur;
    cur++;  
   
    vertex_t dst = *cur;
    cur++;  
   
    int ind = bin_i[ e2bin(src,dst) ]; 
    (bin_i[ e2bin(src,dst) ])++;

    bins[t->tid][e2bin(src,dst)][ ind ].key = e2key(src,dst);
    bins[t->tid][e2bin(src,dst)][ ind ].val = e2val(src,dst);

  }

}

void thd_dump_bins(void *v_thd_binner_t){

  thd_binner_t *t = (thd_binner_t*)v_thd_binner_t;
  int tid = t->tid;
  for(int i = 0; i < NUM_BINS; i++){

    printf("Bin %d (%d edges)\n", i, bin_sz[tid][i]);

    
    for(int j = 0; j < bin_sz[tid][i]; j++){

      printf("\t%lu %lu\n", bins[tid][i][j].key, bins[tid][i][j].val);

    }

  }   

}


void* thd_main_bin (void *v_thd_binner_t){

  thd_binner_t *t = (thd_binner_t*)v_thd_binner_t;
  thd_bin_init(t);
  thd_bin(t);
  return NULL;
}

void* thd_main_binread_count (void *vtid){

  CSR_count_neigh(vtid);
  return NULL;
}

void* thd_main_binread_npop (void *vtid){

  CSR_neigh_pop(vtid);
  return NULL;
}


bin_ctx_t *par_bin(el_t *el){

  unsigned long norm_thd_edges = el->num_edges / NUM_THDS;
  unsigned long last_thd_edges = el->num_edges % NUM_THDS;

  bin_ctx_t *bin_ctx = (bin_ctx_t*)malloc(sizeof(bin_ctx));

  int **alloc_bin_sz = (int **)malloc(sizeof(int*) * NUM_THDS); 
  for(int i = 0; i < NUM_THDS; i++){
    alloc_bin_sz[i] = (int *)malloc(sizeof(int) * NUM_BINS);
  }
  bin_ctx->bin_sz = alloc_bin_sz;

  bin_elem_t ***alloc_bins = (bin_elem_t ***)malloc(sizeof(bin_elem_t**) * NUM_THDS);
  for(int i = 0; i < NUM_THDS; i++){
    alloc_bins[i] = (bin_elem_t **)malloc(sizeof(bin_elem_t *) * NUM_BINS);  
  }
  bin_ctx->bins = alloc_bins;

  thd_binner_t *binners[NUM_THDS];
  for(int i = 0; i < NUM_THDS - 1; i++){

    thd_binner_t *t = (thd_binner_t*)malloc(sizeof(thd_binner_t));
    t->tid = i;
    t->el_ptr = el->el + 2 * sizeof(vertex_t) * i;
    t->thd_edges = norm_thd_edges;
    t->el = el;
    binners[i] = t;
    pthread_create(thds + i,NULL,thd_main_bin,(void*)t);

  } 

  /*The edges won't evenly divide by thread count, so the last
    thread gets a little bit less work to do */
  thd_binner_t *t = (thd_binner_t*)malloc(sizeof(thd_binner_t));
  t->tid = NUM_THDS - 1;
  t->el = el;
  t->el_ptr = el->el + 2 * sizeof(vertex_t) * (NUM_THDS - 1);
  t->thd_edges = last_thd_edges;
  binners[NUM_THDS - 1] = t;
  pthread_create(thds + NUM_THDS - 1,NULL,thd_main_bin,(void*)t);


  for(int i = 0; i < NUM_THDS; i++){
    pthread_join(thds[i],NULL);
    free(binners[i]);
  }

  return bin_ctx;

}

csr_offset_t *par_binread_count(bin_ctx_t *g_ctx){
 
  csr_offset_t *g_oa = CSR_alloc_offset_array(MAX_VTX);

  bin_ctx_t *ctxs[NUM_THDS];
  for(int i = 0; i < NUM_THDS; i++){

    bin_ctx_t *ctx = (bin_ctx_t *)malloc(sizeof(bin_ctx_t)); 
    ctx->bin_sz = g_ctx->bin_sz;
    ctx->bins = g_ctx->bins;
    ctx->num_edges = g_ctx->num_edges;
    ctx->tid = i;
    ctx->data = (void*)g_oa;
    ctxs[i] = ctx;
    

  }


  for(int i = 0; i < NUM_THDS; i++){

    pthread_create(thds + i,NULL,thd_main_binread_count,(void*)ctxs[i]);

  } 

  for(int i = 0; i < NUM_THDS; i++){
    pthread_join(thds[i],NULL);
    free(ctxs[i]);
  }
  return g_oa;

}

void par_binread_npop(bin_ctx_t *g_ctx){
 
  csr_t *csr = (csr_t *)g_ctx->data;
  bin_ctx_t *ctxs[NUM_THDS];
  for(int i = 0; i < NUM_THDS; i++){

    bin_ctx_t *ctx = (bin_ctx_t *)malloc(sizeof(bin_ctx_t)); 
    ctx->bin_sz = g_ctx->bin_sz;
    ctx->bins = g_ctx->bins;
    ctx->num_edges = g_ctx->num_edges;
    ctx->tid = i;
    ctx->data = (void*)csr;
    ctxs[i] = ctx;
    

  }


  for(int i = 0; i < NUM_THDS; i++){

    pthread_create(thds + i,NULL,thd_main_binread_npop,(void*)ctxs[i]);

  } 

  for(int i = 0; i < NUM_THDS; i++){
    pthread_join(thds[i],NULL);
    free(ctxs[i]);
  }

}

int main(int argc, char *argv[]){

  el_t *el = init_el_file(argv[1]);


  printf("Binning...");
  bin_ctx_t *g_ctx = par_bin(el);
  printf("Done.\n");

  /*Done with binning. Now bin read*/
  
  printf("Counting neighbors...");
  csr_offset_t *oa = par_binread_count(g_ctx);
  printf("Done.\n");
 

  /*Sequential accumulation round*/ 
  printf("Accumulating neighbor counts...");
  csr_t *csr = CSR_alloc();
  csr->oa = oa;
  csr_offset_t *oa_out = CSR_alloc_offset_array(MAX_VTX);
  CSR_cumul_neigh_count(csr->oa, oa_out);
  printf("Done.\n");


 
  /*Done with bin read for neighbor count.  Now bin read for neighbor pop*/


  csr->na = CSR_alloc_neigh_array(el->num_edges);
  printf("Populating neighbors...");
  g_ctx->data = (void*)csr;
  par_binread_npop(g_ctx);
  printf("Done.\n");
 
 
  printf("Ejecting CSR...");
  csr->oa = oa_out;
  CSR_out(argv[2],el->num_edges,csr);
  printf("Done.\n");
 
}
