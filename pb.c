#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <string.h>

#define V_NAME_LEN 8
#define NUM_BINS 256

#define v2bin(x) (x % NUM_BINS)
#define e2bin(x,y) (x % NUM_BINS)
#define e2key(x,y) (x)
#define e2val(x,y) (y)

typedef unsigned long vertex_t;
typedef unsigned long val_t;
typedef struct edge {
  vertex_t src;
  vertex_t dst;
} edge_t;

typedef struct bin_elem{
  vertex_t key;
  val_t val;
} bin_elem_t;

int num_edges;
int bin_sz[NUM_BINS];
bin_elem_t *bins[NUM_BINS];

char *init_el_file(char *f){

  struct stat stat;
  int fd = open(f,O_RDONLY,0);
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

  printf("Bins\n----\n|");
  for(int i = 0 ; i < NUM_BINS; i ++){

    printf("%d|",bin_sz[i]);

    /*An element of a bin contains a source key and an update value*/
    /*For now, these are both the size of a vertex_t*/
    bins[i] = (bin_elem_t *)malloc( bin_sz[i] * sizeof(bin_elem_t) );

  }
  printf("\n");

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

int main (int argc, char *argv[]){

  char *el = init_el_file(argv[1]);
  bin_init(el);
  bin(el);
  dump_bins();

}
