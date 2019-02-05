Source: [The Stanford GraphBase](http://www-cs-staff.stanford.edu/~uno/sgb.html)

A network can be represented by an [adjacency matrix](http://en.wikipedia.org/wiki/Adjacency_matrix), where each cell __ij__ represents an edge from vertex __i__ to vertex __j__. Here, vertices represent characters in a book, while edges represent co-occurrence in a chapter.

Given this two-dimensional representation of a graph, a natural visualization is to show the matrix! However, the effectiveness of a matrix diagram is heavily dependent on the order of rows and columns: if related nodes are placed closed to each other, it is easier to identify clusters and bridges.

This example lets you try different orderings via the drop-down menu. This type of diagram can be extended with manual reordering of rows and columns, and expanding or collapsing of clusters, to allow deeper exploration. <a href="http://en.wikipedia.org/wiki/Jacques_Bertin">Jacques Bertin</a> (or more specifically, his fleet of assistants) did this by hand with paper strips.

The multiple ordering algorithms are provided by the library [Reorder.js](http://github.com/jdfekete/reorder.js).

While path-following is harder in a matrix view than in a [node-link diagram](http://mbostock.github.com/d3/ex/force.html), matrices have other advantages. As networks get large and highly connected, node-link diagrams often devolve into giant hairballs of line crossings. Line crossings are impossible with matrix views. Matrix cells can also be encoded to show additional data; here color depicts clusters computed by a community-detection algorithm.

