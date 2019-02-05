## Path Testing


Programa que usa el algoritmo [BFS de graphframes](https://graphframes.github.io/api/scala/index.html#org.graphframes.lib.BFS) para calcular los caminos más cortos desde
los artistas invitados al festival hacia los artistas relevantes.

Modificar parametros (e.g directorios de entrada y salida, directorio spark) en el script *run.sh* el cual corre el programa usando spark-submit.

- Input:
	- parquet de grafo previamente calculado. Es el grafo que representa a toda la base de datos generada a partir de las distintas fuentes.
	- directorio de CSV con artistas invitados (por año).
	- directorio de CSV con artistas relevantes.
	- numero minimo de releases en común entre pares de artistas.
	- directorio de salida.

- Output: Un parquet con esquema: from | e0 | v1 | e1 | v2 | e2 | ... | to | key

donde:

- "from| e0 | v1 | e1 | v2 | ... | to" denota el camino encontrado desde "from" hacia "to" pasando por los vertices
"v1", "v2", etc y conectados por las aristas "e0", "e1", etc
- key denota la longitud del camino. Ejemplos:
	- key=-1  => el artista invitado no tiene ningún camino hacia algun relevante
	- key=0   => es un "salto" directo desde el artista invitado al artista relevante: (from)-[e0]->(to)
	- key=1   => existe un artista/nodo de por medio entre el artista invitado y el relevante: (from)-[e0]->(v1)-[e1]->(to)
	- etc

Al leer el parquet desde otro programa tener en cuenta que el mismo fue escrito usando
[Schema Merging](https://spark.apache.org/docs/latest/sql-programming-guide.html#schema-merging)
