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
    t->bin_sz[tid][ src % NUM_BINS ]++;    
    cur = cur + 2; 

  }

  for(int i = 0 ; i < NUM_BINS; i ++){

    /*An element of a bin contains a source key and an update value*/
    /*For now, these are both the size of a vertex_t*/
    t->bins[tid][i] = (bin_elem_t *)malloc( t->bin_sz[tid][i] * sizeof(bin_elem_t) );

  }

}


void thd_bin(void *v_thd_binner_t){

  thd_binner_t *t = (thd_binner_t*)v_thd_binner_t;

  /* Local counters for the bin this thread is currently processing */
  unsigned long bin_i[NUM_BINS];
  memset( bin_i, 0, NUM_BINS * sizeof(unsigned long));

  vertex_t *cur = (vertex_t *)t->el_ptr;
  for(int i = 0; i < t->thd_edges; i++){ 

    vertex_t src = *cur;
    cur++;  
   
    vertex_t dst = *cur;
    cur++;  
   
    int ind = bin_i[ e2bin(src,dst) ]; 
    bin_i[ e2bin(src,dst) ]++;

    t->bins[t->tid][e2bin(src,dst)][ ind ].key = e2key(src,dst);
    t->bins[t->tid][e2bin(src,dst)][ ind ].val = e2val(src,dst);

  }

}


void thd_dump_bins(void *v_thd_binner_t){

  thd_binner_t *t = (thd_binner_t*)v_thd_binner_t;
  int tid = t->tid;
  for(int i = 0; i < NUM_BINS; i++){

    printf("Bin %d (%d edges)\n", i, t->bin_sz[tid][i]);
    
    for(int j = 0; j < t->bin_sz[tid][i]; j++){

      printf("\t%lu %lu\n", t->bins[tid][i][j].key, t->bins[tid][i][j].val);

    }

  }   

}


void* thd_main_bin (void *v_thd_binner_t){

  /*This runs in a thread responsible for binning part of the edgelist
    that is stored in the thd_binner_t context object*/
  thd_binner_t *t = (thd_binner_t*)v_thd_binner_t;

  thd_bin_init(t);

  thd_bin(t);

  return NULL;

}

thd_binner_t *init_binner(el_t *el, int tid, unsigned long num_edges, 
                             int **bin_sz, bin_elem_t ***bins){

    thd_binner_t *t = (thd_binner_t*)malloc(sizeof(thd_binner_t));
    t->tid = tid;
    t->el_ptr = el->el + 2 * sizeof(vertex_t) * tid;
    t->thd_edges = num_edges;
    t->el = el;
    t->bin_sz = bin_sz;
    t->bins = bins;
}

bin_ctx_t *par_bin(el_t *el){

  pthread_t thds[NUM_THDS];
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

    binners[i] = init_binner(el,i,norm_thd_edges,
                             bin_ctx->bin_sz,bin_ctx->bins);
    pthread_create(thds + i,NULL,thd_main_bin,(void*)binners[i]);

  } 

  binners[NUM_THDS - 1] = init_binner(el, NUM_THDS - 1, last_thd_edges,
                                      bin_ctx->bin_sz, bin_ctx->bins);
  pthread_create(thds + NUM_THDS - 1,NULL,thd_main_bin,(void*)binners[NUM_THDS-1]);


  for(int i = 0; i < NUM_THDS; i++){
    pthread_join(thds[i],NULL);
    free(binners[i]);
  }

  return bin_ctx;

}



void par_binread_generic(bin_ctx_t *g_ctx, void *(*binread_func)(void*)){

  pthread_t thds[NUM_THDS]; 
  bin_ctx_t *ctxs[NUM_THDS];
  for(int i = 0; i < NUM_THDS; i++){

    bin_ctx_t *ctx = (bin_ctx_t *)malloc(sizeof(bin_ctx_t)); 
    ctx->bin_sz = g_ctx->bin_sz;
    ctx->bins = g_ctx->bins;
    ctx->num_edges = g_ctx->num_edges;
    ctx->tid = i;
    ctx->data = g_ctx->data;
    ctxs[i] = ctx;

  }

  for(int i = 0; i < NUM_THDS; i++){

    pthread_create(thds + i,NULL,binread_func,(void*)ctxs[i]);

  } 

  for(int i = 0; i < NUM_THDS; i++){
    pthread_join(thds[i],NULL);
    free(ctxs[i]);
  }

}

