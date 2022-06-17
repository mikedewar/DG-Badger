a little program to write a large graph (an edgelist, to be precise) to a badger database. The intent is then to write a couple of samplers on top of that database to draw sequences of events from each edge at a large scale.

The graph is created based on a desired degree distribution, and the txns based
on a desird rate distribution. Both of these could be learned from data.

The graph generation is based on a naive implementation of "An Efficient and Scalable Algorithmic Method for Generating Large–Scale Random Graphs" by Alam et al. (2016). 
