#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>

#include "pb.h"

int num_edges;
int bin_sz[NUM_BINS];
bin_elem_t *bins[NUM_BINS];

int fd; /*graph fd*/

char *init_el_file(char *f){

  printf("Loading [%s]...",f);
  struct stat stat;
  fd = open(f,O_RDONLY,0);
  if( fd == -1 ){  

    fprintf(stderr,"bad edgelist file\n");
    exit(0); 

  }

  fstat(fd,&stat);
  int sz = stat.st_size;
  num_edges = sz / (sizeof(vertex_t) * 2);
  fprintf(stderr,"Loading %d edges\n",num_edges);

  char *el = 
    mmap(NULL, sz, PROT_READ, MAP_PRIVATE /*| MAP_HUGETLB | MAP_HUGE_1GB*/, fd, 0);

  if( el == MAP_FAILED ){
    fprintf(stderr,"bad edgelist map attempt\n");
  }

  printf("Done.\n");
  return el;

}

void bin_init(char *el){

  vertex_t *cur = (vertex_t *)el;
  for(int i = 0; i < num_edges; i++){ 

    vertex_t src = *cur;
    (bin_sz[ src % NUM_BINS ]) ++;    
    cur++; 

    vertex_t dst = *cur;
    cur++;

  }

  for(int i = 0 ; i < NUM_BINS; i ++){

    /*An element of a bin contains a source key and an update value*/
    /*For now, these are both the size of a vertex_t*/
    bins[i] = (bin_elem_t *)malloc( bin_sz[i] * sizeof(bin_elem_t) );

  }

}


void bin(char *el){

  printf("Binning...");
  unsigned long bin_i[NUM_BINS];
  memset( bin_i, 0, NUM_BINS * sizeof(unsigned long));

  vertex_t *cur = (vertex_t *)el;
  for(int i = 0; i < num_edges; i++){ 

    vertex_t src = *cur;
    cur++;  
   
    vertex_t dst = *cur;
    cur++;  
   
    int ind = bin_i[ e2bin(src,dst) ]; 
    (bin_i[ e2bin(src,dst) ])++;

    bins[e2bin(src,dst)][ ind ].key = e2key(src,dst);
    bins[e2bin(src,dst)][ ind ].val = e2val(src,dst);

  }
  printf("Done.\n");

}

void dump_bins(){

  for(int i = 0; i < NUM_BINS; i++){

    printf("Bin %d (%d edges)\n",i, bin_sz[i]);

    
    for(int j = 0; j < bin_sz[i]; j++){

      printf("\t%lu %lu\n",bins[i][j].key,bins[i][j].val);

    }

  }   

}



/*auxData for use during neighpop*/
unsigned long CSR_offset_array[MAX_VTX];

/*auxData to serialize out at the end*/
unsigned long CSR_offset_array_out[MAX_VTX];

void CSR_count_neigh(){

  printf("Counting neighbors...");
  unsigned long total_neighs = 0;
  for(int i = 0; i < NUM_BINS; i++){

    for(int j = 0; j < bin_sz[i]; j++){

      CSR_offset_array[ bins[i][j].key ]++; 
      total_neighs++;
    }

  }
  printf("Got %lu total neighbors\n",total_neighs);
  printf("Done.\n");

}

/*
cd: 3 2 1 2 3
re: 0 3 5 6 8


*/
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

void CSR_alloc_neigh(){

 
  CSR_neigh_array = malloc( num_edges * sizeof(vertex_t) );

}

void CSR_neigh_pop(){

  printf("Populating neighbors...");
  for(int i = 0; i < NUM_BINS; i++){

    for(int j = 0; j < bin_sz[i]; j++){

      vertex_t key = bins[i][j].key;
      val_t val = bins[i][j].val;
      unsigned long neigh_ind = CSR_offset_array[key];
      CSR_neigh_array[ neigh_ind ] = val;
      CSR_offset_array[key] = CSR_offset_array[key] + 1;

    }

  }
  printf("Done.\n");

}

void CSR_out(char *out){
  
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

int main (int argc, char *argv[]){

  char *el = init_el_file(argv[1]);
  bin_init(el);
  bin(el);
  CSR_count_neigh();
  CSR_cumul_neigh_count();
  CSR_alloc_neigh();
  CSR_neigh_pop();
  CSR_out(argv[2]);

}
