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

int fd; /*graph fd*/

char *init_el_file(char *f, unsigned long *num_edges){

  printf("Loading [%s]...",f);
  struct stat stat;
  fd = open(f,O_RDONLY,0);
  if( fd == -1 ){  

    fprintf(stderr,"bad edgelist file\n");
    exit(0); 

  }

  fstat(fd,&stat);
  size_t sz = stat.st_size;
  *num_edges = sz / (sizeof(vertex_t) * 2);
  fprintf(stderr,"Loading %lu edges\n",*num_edges);

  char *el = 
    mmap(NULL, sz, PROT_READ, MAP_PRIVATE /*| MAP_HUGETLB | MAP_HUGE_1GB*/, fd, 0);

  if( el == MAP_FAILED ){
    fprintf(stderr,"bad edgelist map attempt\n");
  }

  printf("Done.\n");
  return el;

}
