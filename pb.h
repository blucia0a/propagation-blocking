#ifndef _PB_H_
#define _PB_H_
#define MAX_VTX 1000000
#define V_NAME_LEN 8
#define NUM_BINS 256 
#define NUM_THDS 8

#define v2bin(x) (x % NUM_BINS)
#define e2bin(x,y) (x % NUM_BINS)
#define e2key(x,y) (x)
#define e2val(x,y) (y)

#include "el.h"

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

typedef struct bin_ctx {

  int **bin_sz; /*2d array of sizes for NUM_THDS * NUM_BINS*/
  bin_elem_t ***bins; /*2d array of bins for NUM_THDS * NUM_BINS*/
  int num_edges;
  int tid;

} bin_ctx_t;

typedef struct thd_binner {

  int tid;
  unsigned long thd_edges;
  el_t *el;
  char *el_ptr;

} thd_binner_t;


#endif
