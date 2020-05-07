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
  vertex_t *cur = (vertex_t *)(t->el);
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

  vertex_t *cur = (vertex_t *)t->el;
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
  //CSR_alloc_neigh();
  //CSR_neigh_pop();
  //CSR_out(argv[2]);
  free(t);
  return NULL;
}

void* thd_main_binread_count (void *vtid){

  thd_CSR_count_neigh((int)(unsigned long)vtid);
  return NULL;
}

void* thd_main_binread_npop (void *vtid){

  thd_CSR_neigh_pop((int)(unsigned long)vtid);
  return NULL;
}

int main(int argc, char *argv[]){

  char *el = init_el_file(argv[1]);

  unsigned long norm_thd_edges = num_edges / NUM_THDS;
  unsigned long last_thd_edges = num_edges % NUM_THDS;

  printf("Binning...");
  for(int i = 0; i < NUM_THDS - 1; i++){

    thd_binner_t *t = (thd_binner_t*)malloc(sizeof(thd_binner_t));
    t->tid = i;
    t->el = el + 2 * sizeof(vertex_t) * i;
    t->thd_edges = norm_thd_edges;

    /*release thd_edges[i] to worker thread here*/
 
    pthread_create(thds + i,NULL,thd_main_bin,(void*)t);

  } 

  /*The edges won't evenly divide by thread count, so the last
    thread gets a little bit less work to do */
  thd_binner_t *t = (thd_binner_t*)malloc(sizeof(thd_binner_t));
  t->tid = NUM_THDS - 1;
  t->el = el + 2 * sizeof(vertex_t) * (NUM_THDS - 1);
  t->thd_edges = last_thd_edges;
  pthread_create(thds + NUM_THDS - 1,NULL,thd_main_bin,(void*)t);

  for(int i = 0; i < NUM_THDS; i++){
    pthread_join(thds[i],NULL);
  }
  printf("Done.\n");

  bin_ctx_t *ctx = (bin_ctx_t *)malloc(sizeof(bin_ctx_t)); 
  ctx->bin_sz = &bin_sz;
  ctx->bins = bins;
  /*Done with binning. Now bin read*/
 
  /*TODO: pass context into pthread create to pass into binread funcs*/ 
  printf("Counting neighbors...");
  for(int i = 0; i < NUM_THDS - 1; i++){

    pthread_create(thds + i,NULL,thd_main_binread_count,(void*)(unsigned long)i);

  } 

  /*The edges won't evenly divide by thread count, so the last
    thread gets a little bit less work to do */
  pthread_create(thds + NUM_THDS - 1,NULL,thd_main_binread_count,(void*)NUM_THDS-1);

  for(int i = 0; i < NUM_THDS; i++){
    pthread_join(thds[i],NULL);
  }
  printf("Done.\n");
 
  /*Sequential accumulation round*/ 
  printf("Accumulating neighbor counts...");
  CSR_cumul_neigh_count();
  printf("Done.\n");
  
  CSR_alloc_neigh();
 

 
  /*Done with bin read for neighbor count.  Now bin read for neighbor pop*/
  printf("Populating neighbors...");
  for(int i = 0; i < NUM_THDS - 1; i++){

    pthread_create(thds + i,NULL,thd_main_binread_npop,(void*)(unsigned long)i);

  } 

  /*The edges won't evenly divide by thread count, so the last
    thread gets a little bit less work to do */
  pthread_create(thds + NUM_THDS - 1,NULL,thd_main_binread_npop,(void*)NUM_THDS-1);

  for(int i = 0; i < NUM_THDS; i++){
    pthread_join(thds[i],NULL);
  }
  printf("Done.\n");
  
  printf("Ejecting CSR...");
  CSR_out(argv[2]);
  printf("Done.\n");
 
}
