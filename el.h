typedef struct el {

  int fd;
  char *el;
  unsigned long num_edges;
  unsigned long num_vtx; /*If 0, num vertices is unknown (hasn't been counted yet)*/

} el_t;

el_t *init_el_file(char *);
