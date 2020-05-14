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

csr_t *CSR_alloc(){
  csr_t *csr = (csr_t*)malloc(sizeof(csr_t));
  return csr;
}

csr_offset_t *CSR_alloc_offset_array(unsigned long size){

  csr_offset_t *oa = (csr_offset_t*)malloc(sizeof(csr_offset_t) * size);
  return oa;

}

csr_offset_t *CSR_alloc_neigh_array(unsigned long size){

  csr_vertex_t *na = (csr_vertex_t*)malloc(sizeof(csr_vertex_t) * size);
  return na;

}



void CSR_cumul_neigh_count(csr_offset_t *CSR_offset_array, csr_offset_t *CSR_offset_array_out){

  unsigned long sum_so_far = 0; 
  for(unsigned long i = 0; i < MAX_VTX; i++){

    unsigned long tmp = CSR_offset_array[i];
    CSR_offset_array[i] = sum_so_far;
    sum_so_far += tmp;

  }
  
  memcpy(CSR_offset_array_out, CSR_offset_array, MAX_VTX * sizeof(unsigned long));

}

void CSR_print_neigh_counts(csr_offset_t *CSR_offset_array){
  
  printf("Neighbor Counts\n---------------\n");
  for(unsigned long i = 0; i < MAX_VTX; i++){

    if( CSR_offset_array[i] > 0 ){

      printf("v%lu %lu\n",i,CSR_offset_array[i]);

    }

  } 

}


/*This is a binread implementation function.  It is currently
written with poor modularity to avoid doing a function call
in the inner loop of the bin reading traversal, in this case
to increment the CSR offset array*/
void *CSR_count_neigh(void *vctx){

  /*
    Bin Read Phase - Each thread processes a subset of bins to
    have a disjoint set of elements in the offset_array
  */

/*Generic*/
  bin_ctx_t *ctx = (bin_ctx_t*)vctx;
  int tid = ctx->tid;
  int bins_per_thd = NUM_BINS / NUM_THDS;


/*CSR - Specific*/
  csr_offset_t *CSR_offset_array = (csr_offset_t *)ctx->data;

  for(int h = 0; h < NUM_THDS; h++){
    for(int i = bins_per_thd * tid; i < bins_per_thd * (tid + 1); i++){
      for(int j = 0; j < (ctx->bin_sz)[h][i]; j++){

         /* Each thread is operating on bins with
            disjoint keys.  The elements of CSR_offset_array
            that each thread accesses will be disjoint
            avoiding the need for synchronization */
/*CSR - Specific*/
        unsigned long ind = (ctx->bins)[h][i][j].key;  
        CSR_offset_array[ind]++; 

      }

    }

  }
  return NULL;

}


/*This is another binread function, like the one that
  counts adjacencies above.  This is "inlined" like the other
  one at a cost in modularity, but at a benefit in performance,
  avoiding any abstraction/modularity overheads in the inner loop
*/
void *CSR_neigh_pop(void *vctx){


/*Generic - binread*/
  bin_ctx_t *ctx = (bin_ctx_t*)vctx;
  int tid = ctx->tid;
  int bins_per_thd = NUM_BINS / NUM_THDS;

/*CSR-specific*/
  csr_t *csr = (csr_t *)ctx->data;
  csr_offset_t *CSR_offset_array = csr->oa;
  csr_vertex_t *CSR_neigh_array = csr->na;

  for(int h = 0; h < NUM_THDS; h++){
    for(int i = bins_per_thd * tid; i < bins_per_thd * (tid + 1); i++){
      for(int j = 0; j < (ctx->bin_sz)[h][i]; j++){

/*CSR - Specific*/
        vertex_t key = (ctx->bins)[h][i][j].key;
        val_t val = (ctx->bins)[h][i][j].val;

        unsigned long neigh_ind = CSR_offset_array[key];
        CSR_neigh_array[ neigh_ind ] = val;
        CSR_offset_array[key] = CSR_offset_array[key] + 1;

      }
 
    }

  }
  return NULL;

}

void CSR_out(char *out, unsigned long num_edges, csr_t *csr_data){
 
  printf ("Writing out CSR data to [%s]...",out);

  csr_offset_t *CSR_offset_array_out = csr_data->oa;
  csr_vertex_t *CSR_neigh_array = csr_data->na;

  unsigned long outsize = MAX_VTX * sizeof(unsigned long) + num_edges * sizeof(vertex_t) + 2* sizeof(unsigned long);
  struct stat stat;
 
  /*open file*/
  int outfd = open(out,  O_CREAT | O_RDWR | O_TRUNC, (mode_t) 0x0777);
  if( outfd == -1 ){  

    fprintf(stderr,"Couldn't open CSR output file\n");
    exit(-1); 

  }

  /*Seek to end and write dummy byte to make filesize large enough*/
  if(lseek(outfd, outsize - 1, SEEK_SET) == -1){
    fprintf(stderr,"csr seek error\n");
    exit(-1);
  }
 
  if(write(outfd,"",1) == -1){
    fprintf(stderr,"csr write error\n");
    exit(-1);
  }

  /*Map big empty file into memory*/
  char *csr = 
    mmap(NULL, outsize, 
          PROT_READ | PROT_WRITE | PROT_EXEC, MAP_SHARED, outfd, 0);

  if( csr == MAP_FAILED ){
    fprintf(stderr,"%s\n",strerror(errno));
    fprintf(stderr,"Couldn't memory map CSR output file\n");
    exit(-1);
  }

  unsigned long num_vtx = MAX_VTX;
  memcpy(csr, &num_vtx, sizeof(unsigned long));
  memcpy(csr + sizeof(unsigned long), &num_edges, sizeof(unsigned long));

  /*Copy offsets from memory to file*/
  memcpy(csr + 2*sizeof(unsigned long), CSR_offset_array_out, MAX_VTX * sizeof(unsigned long));

  /*Copy neighs from memory to file*/
  memcpy(csr + 2*sizeof(unsigned long) + MAX_VTX * sizeof(unsigned long), CSR_neigh_array, 
         num_edges * sizeof(vertex_t));

  
  /*Sync to flush data*/
  msync(csr, outsize, MS_SYNC);
  /*unmap output file*/
  munmap(csr, outsize);
  /*close file*/
  close(outfd);
  printf("Done.\n");
  
}
