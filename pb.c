#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <string.h>

#define V_NAME_LEN 8
#define NUM_BINS 256

int bin_sz[NUM_BINS];
int num_edges;
char *init_el_file(char *f){

  struct stat stat;
  int fd = open(f,O_RDONLY,0);
  if( fd == -1 ){  

    fprintf(stderr,"bad edgelist file\n");
    exit(0); 

  }

  fstat(fd,&stat);
  num_edges = stat.st_size;
  char *el = 
    mmap(NULL, num_edges, PROT_READ, 
                             MAP_PRIVATE /*| MAP_HUGETLB | MAP_HUGE_1GB*/, fd, 0);
  if( el == MAP_FAILED ){
    fprintf(stderr,"bad edgelist map attempt\n");
  }
  return el;

}

void bin_info(char *el){
  char *cur = el;
  char src[V_NAME_LEN+1];
  char dst[V_NAME_LEN+1];
  for(int i = 0; i < num_edges / (V_NAME_LEN * 2); i++){ 

    memcpy(src,cur,V_NAME_LEN);
    src[V_NAME_LEN] = '\0';
    printf("%s\n",src);
    cur+=V_NAME_LEN;

    memcpy(dst,cur,V_NAME_LEN);
    dst[V_NAME_LEN] = '\0';
    printf("%s\n",dst);
    cur+=V_NAME_LEN;

    printf("%16lu %16lu\n", *((unsigned long*)src), *((unsigned long*)dst));
  }
}

int main (int argc, char *argv[]){

  char *el = init_el_file(argv[1]);
  bin_info(el);

}
