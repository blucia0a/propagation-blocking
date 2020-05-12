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
#include "el.h"


el_t *init_el_file(char *f){

  el_t *el = (el_t *)malloc(sizeof(el_t));

  printf("Loading [%s]...",f);
  struct stat stat;
  int fd = open(f,O_RDONLY,0);
  if( fd == -1 ){  

    fprintf(stderr,"bad edgelist file\n");
    exit(0); 

  }
  el->fd = fd;

  fstat(fd,&stat);
  size_t sz = stat.st_size;
  *num_edges = sz / (sizeof(vertex_t) * 2);
  fprintf(stderr,"Loading %lu edges\n",*num_edges);

  el->num_edges = num_edges;
  el->num_vtx = 0; /*if num_vtx is 0, haven't counted vertices yet*/

  char *map_el = 
    mmap(NULL, sz, PROT_READ, MAP_PRIVATE /*| MAP_HUGETLB | MAP_HUGE_1GB*/, fd, 0);

  if( map_el == MAP_FAILED ){
    fprintf(stderr,"bad edgelist map attempt\n");
  }

  el->el = map_el;

  printf("Done.\n");
  return el;

}
