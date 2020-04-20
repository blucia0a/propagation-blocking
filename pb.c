#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <string.h>

#define V_NAME_LEN 8
#define NUM_BINS 8

#define v2bin(x) (x % NUM_BINS)

typedef unsigned long vertex_t;
typedef struct edge {
  vertex_t src;
  vertex_t dst;
} edge_t;

int num_edges;
int bin_sz[NUM_BINS];
vertex_t *bins[NUM_BINS];

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

void bin_info(char *el){

  vertex_t *cur = (vertex_t *)el;
  for(int i = 0; i < num_edges; i++){ 

    vertex_t src = *cur;
    (bin_sz[ src % NUM_BINS ]) ++;    
    cur++; 

    vertex_t dst = *cur;
    cur++;

    fprintf(stderr,"(%lu,%lu)\n",src,dst);

  }

  printf("Bin Sizes:\n");
  for(int i = 0 ; i < NUM_BINS; i ++){

    printf("%d\n",bin_sz[i]);
    //bins[i] = malloc(sizeof(vertex_t)

  }

}

int main (int argc, char *argv[]){

  char *el = init_el_file(argv[1]);
  bin_info(el);
  

}
