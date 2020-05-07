CC = gcc
CFLAGS = -g -Og -pthread
LFLAGS = 
LIBS   = 

SRCS = csr.c pb.c futil.c 
OBJS = $(SRCS:.c=.o)

MAIN = pb 

all:    $(MAIN)

$(MAIN): $(OBJS) 
	$(CC) $(CFLAGS) $(INCLUDES) -o $(MAIN) $(OBJS) $(LFLAGS) $(LIBS)

# this is a suffix replacement rule for building .o's from .c's
# it uses automatic variables $<: the name of the prerequisite of
# the rule(a .c file) and $@: the name of the target of the rule (a .o file) 
# (see the gnu make manual section about automatic variables)
.c.o:
	$(CC) $(CFLAGS) $(INCLUDES) -c $<  -o $@

rand_graph: rand_graph.c pb.h
	gcc -O3 rand_graph.c -o rand_graph

clean:
	-rm pb rand_graph
