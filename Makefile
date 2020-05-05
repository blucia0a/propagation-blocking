all: pb.c pb.h
	gcc -Og -g pb.c -pthread -o pb

rand_graph: rand_graph.c pb.h
	gcc -O3 rand_graph.c -o rand_graph

clean:
	-rm pb rand_graph
