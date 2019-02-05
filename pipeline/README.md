# SCRIPT PARA GENERAR ARCHIVOS DE VISUALIZACIÓN

A continuación se detallan los pasos a seguir para generar los archivos necesarios para la visualización de caminos en los 
distintos formatos.

## Primer paso

El primer paso consiste en la generación de credenciales en Discogs para el uso de la API:

* ir a https://www.discogs.com/es/settings/developers -> Applications
* si no existe una aplicación creada hacer click en: *Crear una aplicación* y seguir los pasos.
* ir a *Editar ajustes* y copiar *Clave del Cliente* (*consumer key*) e *Información secreta del cliente* (*consumer secret*).
* pegar las claves en el archivo *config.py* (cambiar nombre de archivo **config_example.py** por **config.py**)


## Segundo paso

El segundo paso consiste en generar los token de acceso, para esto ejecutar el programa **consumer_key.py**
y seguir los pasos de autorización. Los tokens de acceso se guardaran en un archivo para ser usados durante la ejecución del pipeline.

```
$ python consumer_key.py
```

## Tercer paso

El tercer paso consiste en la ejecución del pipeline. Primero abrir el script *pipeline_viz.sh* con algun editor de texto y
cambiar los directorios de donde se van a leer los archivos de la Base de Datos y algunos parámetros opcionales. Luego ejecutar de la siguiente manera:

```
$ ./pipeline_viz.sh <year>
```

donde <year> es el la edición del festival que se desea computar.
**IMPORTANTE**: Antes de ejecutar el script es necesario haber generado previamente los archivos CSV correspondientes a dicho año (**TODO**: agregar link a estructura de archivos y/o tesis).


**NOTAS**: Los pasos 1 y 2 son necesarios para obtener las imagenes de los artistas y de los discos y deben ser ejecutados una única ves. El tercer paso 
se repite por cada edición que se desee computar. Por cada edición se generan directorios con *parquets*, al momento de volver a correr el pipeline para una
edición previamente computada, revisar que dichos archivos fueron borrados de los directorios correspondientes.
