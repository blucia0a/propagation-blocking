#ifndef _PB_H_
#define _PB_H_

#include "graph.h"
#include "el.h"

#define NUM_BINS 256 
#define NUM_THDS 8

#define v2bin(x) (x % NUM_BINS)
#define e2bin(x,y) (x % NUM_BINS)
#define e2key(x,y) (x)
#define e2val(x,y) (y)

typedef struct bin_elem{

  vertex_t key;
  val_t val;

} bin_elem_t;

typedef struct bin_ctx {

  int **bin_sz; /*2d array of sizes for NUM_THDS * NUM_BINS*/
  bin_elem_t ***bins; /*2d array of bins for NUM_THDS * NUM_BINS*/
  int num_edges;
  int tid;
  void *data; /*polymorphic field for app-specific data used in binread*/

} bin_ctx_t;

typedef struct thd_binner {

  int tid;
  unsigned long thd_edges;
  el_t *el;
  char *el_ptr;

} thd_binner_t;


#endif
