## Programa que computa el ranking de cada camino basandose en las metricas CI y CII de cada nodo.

Usa 4 argumentos:

- directorio del parquet que contiene los paths calculados por path_testing.
- directorio de csv con artistas invitados por año
- directorio de csv con artistas relevantes
- directorio de salida.

Output: parquet con ranks ci y cii calculados y ordenados de forma ascendente (rank=1 es el mas importante)
