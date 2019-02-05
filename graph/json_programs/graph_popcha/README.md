## Programa para generar archivos JSON necesarios para la visualización de caminos usando: *Concentric Circular Layout* y *Path Layout*.

El programa filtra caminos usando las metricas de Collective Influence, y una versión mejorada llamada Collective Initialized Influence y devuelve arhivos JSON para visualización.

Pide 6 argumentos:

- directorio del parquet que contiene los caminos filtrados (path_filter.pqt)
- directorio de los csv con artistas invitados (ej: /csv/guests/YEAR)
- directorio de los csv con artistas relevantes (ej: /csv/relevants)
- <<r>> cota para filtrar caminos con rank <= r (se recomienda usar r = 5, para una correcta visualización)
- año de la edición del festival (ej: 2017)
- directorio de salida.

Output: JSONSs necesarios para la visualización del grafo. Un JSON con caminos filtrados por CI y otro con caminos filtrados por CI.
Los nombres de estos archivos tienen el siguiente formato:

- ED<<YEAR>>_RANKCI.json
- ED<<YEAR>>_RANKCII.json
