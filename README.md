# CTG Project

This project uses Spark Gprahx to generate a user call graph, and use the built-in shortest path algorithm to evaluate the relationship between users.

To model the weight between each user vertices, we add virtual nodes on an edge with non-unit weight.

The project runs on Spark 1.6.1, and the project is build by sbt.
