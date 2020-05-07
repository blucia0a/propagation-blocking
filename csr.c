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

/*auxData for use during neighpop*/
unsigned long CSR_offset_array[MAX_VTX];

/*auxData to serialize out at the end*/
unsigned long CSR_offset_array_out[MAX_VTX];

/*This is a binread implementation function.  It is currently
written with poor modularity to avoid doing a function call
in the inner loop of the bin reading traversal, in this case
to increment the CSR offset array*/
void thd_CSR_count_neigh(void *vctx){

  /*
    Bin Read Phase - Each thread processes a subset of bins to
    have a disjoint set of elements in the offset_array
  */
  bin_ctx_t *ctx = (bin_ctx_t*)vctx;
  int tid = ctx->tid;

  /* This code assumes that NUM_BINS 
     divides evenly by NUM_THDS */ 
  int bins_per_thd = NUM_BINS / NUM_THDS;

  unsigned long total_neighs = 0;
  for(int h = 0; h < NUM_THDS; h++){

    for(int i = bins_per_thd * tid; i < bins_per_thd * (tid + 1); i++){
      
      for(int j = 0; j < (*ctx->bin_sz)[h][i]; j++){

         /* Each thread is operating on bins with
            disjoint keys.  The elements of CSR_offset_array
            that each thread accesses will be disjoint
            avoiding the need for synchronization */
        unsigned long ind = (*ctx->bins)[h][i][j].key;  
        CSR_offset_array[ ind ]++; 
        total_neighs++;

      }

    }

  }

}

void CSR_cumul_neigh_count(){

  unsigned long sum_so_far = 0; 
  for(unsigned long i = 0; i < MAX_VTX; i++){

    unsigned long tmp = CSR_offset_array[i];
    CSR_offset_array[i] = sum_so_far;
    sum_so_far += tmp;

  }
  memcpy(CSR_offset_array_out, CSR_offset_array, MAX_VTX * sizeof(unsigned long));

}

void CSR_print_neigh_counts(){
  
  printf("Neighbor Counts\n---------------\n");
  for(unsigned long i = 0; i < MAX_VTX; i++){

    if( CSR_offset_array[i] > 0 ){

      printf("v%lu %lu\n",i,CSR_offset_array[i]);

    }

  } 

}


vertex_t *CSR_neigh_array;
void CSR_alloc_neigh(unsigned long alloc_num_edges){
  CSR_neigh_array = malloc( alloc_num_edges * sizeof(vertex_t) );
}


/*This is another binread function, like the one that
  counts adjacencies above.  This is "inlined" like the other
  one at a cost in modularity, but at a benefit in performance,
  avoiding any abstraction/modularity overheads in the inner loop
*/
void thd_CSR_neigh_pop(void *vctx){

  bin_ctx_t *ctx = (bin_ctx_t*)vctx;
  int tid = ctx->tid;
  int bins_per_thd = NUM_BINS / NUM_THDS;

  unsigned long total_neighs = 0;
  for(int h = 0; h < NUM_THDS; h++){

    for(int i = bins_per_thd * tid; i < bins_per_thd * (tid + 1); i++){
  
      for(int j = 0; j < (*ctx->bin_sz)[h][i]; j++){

        vertex_t key = (*ctx->bins)[h][i][j].key;
        val_t val = (*ctx->bins)[h][i][j].val;

        unsigned long neigh_ind = CSR_offset_array[key];
        CSR_neigh_array[ neigh_ind ] = val;
        CSR_offset_array[key] = CSR_offset_array[key] + 1;
      }
 
    }

  }

}

void CSR_out(char *out, unsigned long num_edges){
 
  printf ("Writing out CSR data to [%s]...",out);
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
