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
  size_t sz = stat.st_size;
  num_edges = sz / (sizeof(vertex_t) * 2);
  fprintf(stderr,"Loading %lu edges\n",num_edges);

  char *el = 
    mmap(NULL, sz, PROT_READ, MAP_PRIVATE /*| MAP_HUGETLB | MAP_HUGE_1GB*/, fd, 0);

  if( el == MAP_FAILED ){
    fprintf(stderr,"bad edgelist map attempt\n");
  }

  printf("Done.\n");
  return el;

}

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

/*auxData for use during neighpop*/
unsigned long CSR_offset_array[MAX_VTX];

/*auxData to serialize out at the end*/
unsigned long CSR_offset_array_out[MAX_VTX];


/*This is a binread implementation function.  It is currently
written with poor modularity to avoid doing a function call
in the inner loop of the bin reading traversal, in this case
to increment the CSR offset array*/
void thd_CSR_count_neigh(int tid){

  /*
    Bin Read Phase - Each thread processes a subset of bins to
    have a disjoint set of elements in the offset_array
  */
  /* This code assumes that NUM_BINS 
     divides evenly by NUM_THDS */ 
  int bins_per_thd = NUM_BINS / NUM_THDS;

  unsigned long total_neighs = 0;
  for(int h = 0; h < NUM_THDS; h++){

    for(int i = bins_per_thd * tid; i < bins_per_thd * (tid + 1); i++){
  
      for(int j = 0; j < bin_sz[h][i]; j++){

        /* Each thread is operating on bins with
           disjoint keys.  The elements of CSR_offset_array
           that each thread accesses will be disjoint
           avoiding the need for synchronization */
        unsigned long ind = bins[h][i][j].key; 
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

void CSR_alloc_neigh(){
  CSR_neigh_array = malloc( num_edges * sizeof(vertex_t) );
}


/*This is another binread function, like the one that
  counts adjacencies above.  This is "inlined" like the other
  one at a cost in modularity, but at a benefit in performance,
  avoiding any abstraction/modularity overheads in the inner loop
*/
void thd_CSR_neigh_pop(int tid){

  int bins_per_thd = NUM_BINS / NUM_THDS;

  unsigned long total_neighs = 0;
  for(int h = 0; h < NUM_THDS; h++){

    for(int i = bins_per_thd * tid; i < bins_per_thd * (tid + 1); i++){
  
      for(int j = 0; j < bin_sz[h][i]; j++){

        vertex_t key = bins[h][i][j].key;
        val_t val = bins[h][i][j].val;

        unsigned long neigh_ind = CSR_offset_array[key];
        CSR_neigh_array[ neigh_ind ] = val;
        CSR_offset_array[key] = CSR_offset_array[key] + 1;
      }
 
    }

  }

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

  /*Done with binning. Now bin read*/
  
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
