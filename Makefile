all: pb.c
	gcc -Og -g pb.c -o pb

rand_graph: rand_graph.c
	gcc -O3 rand_graph.c -o rand_graph

clean:
	-rm pb rand_graph
