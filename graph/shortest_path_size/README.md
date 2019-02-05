## Shortest Path Size


Programa que computa las distancias desde cada artista invitado hacia los artistas relevantes.

Modificar parametros (e.g directorios de entrada y salida, directorio spark) en el script *run.sh* el cual corre el programa usando spark-submit.

- Input: 
	* parquet de grafo previamente calculado con programa *graph_creation* (en directorio: files/db/graph/degree_CC_CI_CII).
	* directorio de csv de artistas invitados por año (ej: csv/guests/YEAR).
	* directorio de csv de artistas relevantes.
	* numero de *releases* para filtrar.
	* directorio de salida.

- Output: parquet con las distancias computadas.
