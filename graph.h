#ifndef _GRAPH_H_
#define _GRAPH_H_
#define MAX_VTX 1000000
#define V_NAME_LEN 8

typedef unsigned long vertex_t;
typedef unsigned long val_t;

typedef struct edge {

  vertex_t src;
  vertex_t dst;

} edge_t;
#endif
