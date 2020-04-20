#define MAX_VTX 1000000
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
