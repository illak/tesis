## Programa que genera JSONS para visualización de caminos usando *Concentric Circular Layout* pero con cada artista relevante por separado.

Pide 6 argumentos:

- Directorio de parquet de caminos filtrados por *rank* (path_filter.pqt).
- Directorio de csv de artistas invitados por año (ej: /csv/guests/YEAR).
- Directorio de csv de artistas relevantes (ej: /csv/relevants).
- Rank (similar a graph_popcha).
- Año.
- Directorio de salida.

Output:

- Dos JSONs, uno con rango CI y otro con rango CII con nombres "ED<YEAR>_BY_RELEVANT_CI.json" y "ED<YEAR>_BY_RELEVANT_CII.json" respectivamente.
- Un archivo con los artistas relevantes para generar el selector con javascript.

 
