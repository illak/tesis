# README #

## Trabajo final de Licenciatura en Ciencias de la Computación

Título: Exploración y visualización de redes de músicos.
Tesista: Illak Zapata
Directores: Damián Barsotti y Mariano Barsotti

### Contexto

El análisis de redes es un área extensa de conocimiento cuyo objetivo de estudio puede sintetizarse en la extracción de propiedades sobre un conjunto de entidades relacionadas entre sí. Estas entidades junto con sus relaciones se las denomina redes y abarcan objetos muy disímiles como redes sociales, de transporte, eléctrica, biológicas, epidemiológicas, etc. 
Hoy en día gran parte del análisis de redes se ha enfocado particularmente al estudio de las redes sociales cuyos campos de aplicación van desde la sociología hasta el marketing empresarial. En este caso particular las entidades pueden ser personas o grupos y las relaciones representan atributos en común o flujos de información entre aquellas. Los análisis clásicos sobre estos tipos de redes son Detección de Comunidades y Medidas de Centralidad.
La visualización de datos es un elemento esencial a la hora interpretar los resultados obtenidos en dichos análisis lo cual permite al usuario la interacción directa con los datos, facilitando a la comprensión y conocimiento de los mismos y ayudando en la toma de decisiones.
Objetivo

El propósito general de este trabajo especial es la aplicación de técnicas de análisis de redes sociales a redes de músicos. En este caso las entidades pueden ser músicos o bandas y las relaciones pueden ser, coautoría, colaboración, región geográfica, músicos en común, etc.
El trabajo tendrá como resultado final la implementación de un portal web para la visualización de las redes de las que son partícipes los músicos invitados a las distintas ediciones del CBA JAZZ FESTIVAL.



---


### Demo de visualización para artistas invitados a la edición 2017 del CBA Jazz Festival.

http://www.famaf.unc.edu.ar/~ilz0111/tesina/viz/

### Implementación en la edición 2018 del CBA Jazz Festival.

http://cordobajazzfestival.com.ar/2018/wp-content/uploads/sites/10/2018/relacion_artistas/

---
### Estructura de directorios

* artist_finder_app: App para búsqueda de artistas por edición de festival.

* bd_source/builders/: Programas generadores de parquet a partir de bd fuentes.
    * discogs/discogs2parquet
    * musicbrainz/musicbrainz2parquet
    * wikidata/wikidata2parquet

* bd_target/target_builder: Programa generador de bd target.

* common: archivos varios

* pipeline: script para generar archivos de visualización (2da etapa de analisis)

* graph/
    * graph_creation: Programa que genera el grafo de toda la BD
    * graph_only_ids: Programa que genera un JSON unicamente con IDS de artistas
    * path_testing: Programa que genera parquet con caminos mas cortos desde artistas invitados hacia artistas relevantes
    * shortest_path_size: Programa que genera parquet con tamaños de caminos
    * viz/
        * viz_graph_ccl: archivos para visualización "Concentric Circular Layout"
        * viz_graph_cscl: archivos para visualización "Concentric Single Circular Layout"
        * viz_graph_pl: archivos para visualización "Path Layout"
        * viz_tree: archivos para visualización estilo Spotify
    * jsons_programs/
        * graph_path_tree: Programa generador de jsons para viz estilo spotify.
        * graph_popcha: Programa generador de jsons para viz estilo PopCha! para todos los relevantes
	* graph_popcha_individual: Programa generador de jsons para viz estilo PopCha individual

---

### Requisitos para correr los programas

* [Apache Spark](https://spark.apache.org/downloads.html)
* [SBT](https://www.scala-sbt.org)
* [Python](https://www.python.org)


---
### Pasos para la ejecución

**Importante**: Para la correcta ejecución de los programas tener en consideración:


1) Usar la última versión disponible de Spark (en este trabajo se utilizó Spark 2.3.1).
Extraer los archivos descargados en el directorio:

* *$HOME/spark/*

Si se quiere usar otro directorio declarar una variable de entorno:

* *SPARK_HOME=`<spark dir>`*

ejemplo:
* *SPARK_HOME=/home/illak/spark/spark-2.3.1-bin-hadoop2.7*


2) Usar versión de **java 8** para evitar conflictos con Spark. Si se usan versiones superiores a java 8 se pueden encontrar inconsistencias en la compilación y ejecución de los programas.


3) Usar el esquema de directorios que se presenta junto con el repositorio y en los pasos a continuación. Esto es para facilitar la ejecución de los programas en un futuro mediante la automatización de los mismos usando *script bashs*.


#### 1) Descarga de datos

EL primer paso es la descarga de los datos de las distintas fuentes:

* Discogs: https://data.discogs.com. Los archivos necesarios son los siguientes:

    * *discogs_`<year-month-day>`_releases.xml.gz*
    * *discogs_`<year-month-day>`_artists.xml.gz*

tamaño descomprimido: ~40GB

* Musicbrainz: https://musicbrainz.org/doc/MusicBrainz_Database/Download ir seccion *Download* y descargar:

    * *mbdump.tar.bz2*

tamaño descomprimido: ~8.7 GB

* Wikidata: https://dumps.wikimedia.org/wikidatawiki/entities/ descargar:

    * *latest-all.json.{bz2 or gz}*

tamaño descomprimido: ~562 GB

Extraer los archivos en los directorios correspondientes a cada fuente en *files/extracted/*. Es decir en:

* *files/db/extracted/discogs/*
* *files/db/extracted/musicbrainz/*
* *files/db/extracted/wikidata/*

---

#### 2) ETL1: Generación de parquets a partir de los datos

Para este paso es necesario correr cada uno de los programas en el directorio: [bd_source/builders](https://bitbucket.org/bigdata_famaf/tesis_illak/src/master/bd_source/builders/).


Para **Discogs**:

El programa es *discogs2parquet*. Es necesario editar el archivo *build_discogs.sh* 
especificando el directorio donde se encuentran los archivos de Releases y Artists (FINREL y FINART). En este caso:

* *FINREL=../../../../files/db/extracted/discogs/`<releases_file.xml>`*
* *FINART=../../../../files/db/extracted/discogs/`<artists_file.xml>`*

además del directorio de salida (FOUT) a donde se guardarán los parquets:

* *FOUT=../../../../files/db/transformed/tmp1/discogs/*

Compilar:

```
$ sbt clean; sbt package
```

Ejecutar script:

```
$ ./build_discogs.sh
```

**ETA**: ~2hrs

Para **Musicbrainz**:

El programa es *musicbrainz2parquet*. Es necesario editar el archivo *build_mb.sh*
especificando el directorio donde se encuentran las tablas de musicbrainz (FIN):

* *FIN=../../../../files/db/extracted/musicbrainz/mbdump/*

 y el directorio de salida (FOUT).

 * *FOUT=../../../../files/db/transformed/tmp1/musicbrainz/*


Compilar:

```
$ sbt clean; sbt package
```

 Ejecutar script:

 ```
$ ./build_mb.sh
 ```

**ETA**: ~20seg

Para **Wikidata**:

El programa es *wikidata2parquet*. Es necesario editar el archivo *build_wiki.sh"
especificando el directorio del JSON extraido anteriormente (FIN):

* *FIN=../../../../files/db/extracted/wikidata/*

 y el directorio de salida (FOUT):

 * *FOUT=../../../../files/db/transformed/tmp1/wikidata/*


Compilar:

```
$ sbt clean; sbt package
```

Ejecutar script:

```
$ ./build_wiki.sh
```



**ETA**: ~5hrs


---


#### 3) ETL2: Unificación de datos

Una vez generados los parquets en la etapa anterior se procede con la unificación de los mismos para generar la BD Target.
Para esto se utiliza el programa en el directorio [bd_target/target_builder](https://bitbucket.org/bigdata_famaf/tesis_illak/src/master/bd_target/target_builder/).

Primero se debe editar el script *build_db.sh* especificando el directorio donde se encuentran los parquets generados en la etapa anterior, es decir:

* *FIN=../../files/db/transformed/tmp1/*

Especificar el directorio de salida que es el mismo en donde se generan los archivos intermedios de la etapa anterior pero los parquets que se generen dutante esta etapa se almacenarán en los directorios *tmp2/* y *final/* los cuales se crean automaticamente:

* *FOUT=../../files/db/transformed/*


Compilar:

```
$ sbt clean; sbt package
```

Ejecutar script:

```
$ ./build_db.sh
```


---

#### 4) Generación de grafo, collective influence y búsqueda de caminos.


**IMPORTANTE**: A partir de este punto se deben realizar todos los pasos por cada edición de festival que se desee calcular. Los pasos anteriores se realizan cada vez que se quiera actualizar la base de datos usando *dumps* de datos más actuales.


##### Generación de CSV de artistas invitados
Para generar dicho CSV se desarrolló el programa [artist_finder_app](https://bitbucket.org/bigdata_famaf/tesis_illak/src/master/artist_finder_app/)
el cual busca artistas por nombre en las distintas fuentes de datos.

Además de realizar la búsqueda de artistas invitados en las fuentes el programa ofrece la opción de actualizar la BD Target al finalizar la búsqueda. En caso de activar la actualización de la BD, al finalizar la búsqueda el programa tendá en cuenta las opciones seleccionadas por el usuario y de ser necesario actualizará la información basada en estas deciciones. La actualización se realiza agregando información faltante a los artistas seleccionados que por distintas razones no hayan sido consideradas durante la etapa de unificación de fuentes.
En caso de optar por actualizar la BD el programa solamente actualiza el parquet *dfArtists.pqt* que conforma a la BD Target. Por lo que a partir de este punto se recomienda la creación de un directorio que contenga la Base de Datos actualizada, por ejemplo:

```
files/db/transformed/final/updated/
```
o bien

```
files/db/transformed/final_updated/
```

en este directorio deben se deben copiar los 3 parquet que conforman la BD Target:

* el parquet de **artistas actualizado**: dfArtists.pqt
* el parquet de **releases original**: dfReleases.pqt
* el parquet de **relaciones original**: dfArtistsRel.pqt

Los dos últimos parquet se encuentran en el directorio de salida de la BD unificada en el paso (3) es decir:

```
files/db/transformed/final/
```

En el caso de que se opte por no actualizar la BD Target usar el directorio por defecto como se usa en los siguientes programas.


##### Creación del grafo y cálculo de CI:

Una vez realizada la unificación de datos se procede con la construcción del grafo que represente las relaciones entre artistas.
Este es el grafo de **toda** la BD Target y el objetivo de los siguientes programas es filtrar información relevante del mismo para cada edición.
Para crear el grafo usaremos el programa que se encuentra en [graph/graph_creation/](https://bitbucket.org/bigdata_famaf/tesis_illak/src/master/graph/graph_creation/).
El mismo usa GraphFrames para la creación del grafo y además calcula el valor de influencia colectiva de cada vértice/artista y lo agrega como atributo del mismo.
Pero además del valor de CI se computa una mejora al mismo haciendo una inicialización previa de la misma teniendo en cuenta las categorias (indicadas por un experto) de los artistas relevantes, a esta métrica la denominamos CII (*Collective Initialized Influence*).

Antes de la ejecución del mismo se debe modifircar el script *build_graph.sh* especificando el directorio donde se encuentran los datos unificados:

**NOTA**: En caso de haber actualizado la BD Target durante la búsqueda de artistas invitados usar el directorio de la BD actualizada.

* *FINDIR=../../files/db/transformed/final/*

el directorio donde se encuentra el CSV de los artistas relevantes con sus categorias:

* *FINDIR2=../../files/db/graph/csv/relevants/*

y el directorio de salida el cual debe ser como sigue:

* *FOUTDIR=../../files/db/graph/degree_CC_CI_CII*

Los archivos que se generan en este directorio son dos parquets, uno correspondiente a los vertices y otro correspondiente a las aristas del grafo.


Compilar:

```
$ sbt clean; sbt package
```


Ejecutar el script:

```
$ ./build_graph.sh
```

---


##### Búsqueda de caminos

A continuación se procede con la búsqueda de caminos más cortos (*shortests paths*) partiendo de artistas invitados hacia artistas relevantes. Para esto se usará el programa que se encuentra en [graph/path_computing/](https://bitbucket.org/bigdata_famaf/tesis_illak/src/master/graph/path_computing/).
Antes de ejecutar el script *run_bfs.sh* se debe modificar el mismo especificando directorio del grafo:

* *FINPUTDIR1=../../files/db/graph/degree_CC_CI_CII/*

el directorio con el csv de los artistas invitados por edición, por ejemplo:

* *FINPUTDIR2=../../files/db/graph/csv/2017/*

el directorio con el csv de los artistas relevantes:

* *FINPUTDIR3=../../files/db/graph/csv/relevants/*

la cantidad mínima de releases/discos que los artistas tienen en común (recomendado 2):

* *NUMREL=2*

y el directorio de salida (usar año para distinguir entre ediciones):

* *FOUTDIR=../../files/db/graph/output/path_computing/2017/*


Compilar:

```
$ sbt clean; sbt package
```


Ejecutar el script:

```
$ ./run_bfs.sh
```

##### Asignación de rankings a caminos

Finalmente se procede con la asignación de valores de importancia a los caminos encontrados, esto con la finalidad de que en la etapa de visualización se muestren únicamente aquellos que se consideran más relevantes.
El programa encargado de esto es el que se encuentra en [graph/path_ranking/](https://bitbucket.org/bigdata_famaf/tesis_illak/src/master/graph/path_ranking/).
Primero se debe modifica el script especificando los directorios correspondientes:
La entrada es el parquet que contiene los caminos encontrados en la etapa anterior:

* *FINPUTDIR1=../../files/db/graph/output/path_computing/2017/*

y el directorio donde se encuentra el CSV de los artistas invitados:

* *FINPUTDIR2=../../files/db/graph/csv/2017/*

por último el diectorio de salida:

* *FOUTDIR=../../files/db/graph/output/path_ranking/2017/*


Compilar:

```
$ sbt clean; sbt package
```

Ejecutar el script:

```
$ ./run_rankings.sh
```

---

#### 5) Visualización

**NOTA**: existe un script encargado de realizar todos los pasos de ejecución que se describen a continuación. Ver readme en: [pipeline/](https://bitbucket.org/bigdata_famaf/tesis_illak/src/master/pipeline/) 

**IMPORTANTE**: El programa fué hecho con **Python 3.6.6**, por lo tanto se recomienda el uso de python 3.x.

Como primer paso para la generación de archivos de visualización ejecutaremos un script hecho en python el cual usa la API de Discogs y que tiene como objetivo la obtención de imagenes de artistas y releases.
El programa encargado de esto se encuentra en [graph/viz/images_fetcher/](https://bitbucket.org/bigdata_famaf/tesis_illak/src/master/graph/viz/images_fetcher/).
Para obtener las imagenes debemos:

1) Generar credenciales en Discogs para el uso de la API:

* ir a https://www.discogs.com/es/settings/developers -> Applications
* si no existe una aplicación creada hacer click en: Crear una aplicación y seguir los pasos.
* ir a Editar ajustes y copiar Clave del Cliente (consumer key) e Información secreta del cliente (consumer secret).
* pegar las claves en el archivo *config_example.py*. Cambiar nombre de archivo config_example.py por *config.py*.


2) Instalar las librerias requeridas (*requirements.txt*)

**NOTA**: Se recomienda el uso de un entorno virtual como [virtualenv](https://virtualenv.pypa.io/en/stable/) con python 3.

```
$ pip install -r requirements.txt
```


3) El segundo paso consiste en generar los token de acceso, para esto ejecutar el programa consumer_key.py y seguir los pasos de autorización. Los tokens de acceso se guardaran en un archivo para ser usados durante la ejecución del programa:

```
$ python consumer_key.py
```

4) El programa toma como argumentos los ids de artistas y releases unicamente. Para obtener estos ids debemos ejecutar el programa en [graph/graph_only_ids/](https://bitbucket.org/bigdata_famaf/tesis_illak/src/master/graph/graph_only_ids/)
 en donde debemos modificar el script *run_only_ids.sh* especificando el directorio de entrada el cual tiene los caminos con los rankings asignados:

 * *FINPUTDIR1=../../files/db/graph/output/path_ranking/2017/*

 el valor maximo de ranking (recomendado 5 o menos):

 * *RANK=5*

 el año:

 * *YEAR=2017*

 y el directorio de salida en donde se van a guardar los archivos con los ids:

 * *FOUTDIR=../../files/db/graph/output/ids/2017*


5) Una vez obtenidos los ids podemos ejecutar el programa que utiliza la API de Discogs para la extracción de imágenes:


```
$ python discogs_api_images.py <directorio de ids> <año> -o <directorio de salida>
```


donde:

* *`<directorio de ids>`* es el output del programa de ids: 
    * *../../../files/db/graph/output/ids/2017*
* *`<año>`* es el año de la edición (necesario para el nombre del archivo de salida).
    * ej: 2017
* *`<directorio de salida>`* es a donde se guardan los archivos con las imagenes:
    * usar: *../../../files/db/graph/output/images_from_discogs/`<año>`/*
    * ejemplo: *../../../files/db/graph/output/images_from_discogs/2017/


---
Una vez obtenidas las imagenes de artistas y releases, se procede con la generación de los archivos JSON necesarios para la visualización.
Existen 3 programas encargados de realizar esta tarea, cada uno genera JSON correspondiente al tipo de visualización (o layout):

El programa en [graph/json_programs/graph_popcha/](https://bitbucket.org/bigdata_famaf/tesis_illak/src/master/graph/json_programs/graph_popcha/) genera JSON para el layout __*Concentric Circle Layout*__ y para __*Path Layout*__.
Primero debemos editar el script *run_popcha.sh* especificando los directorios de entrada:


El directorio de los caminos con valores de ranking:

* *FINPUTDIR1=../../../files/db/graph/output/path_ranking/2017/*

el directorio del CSV de artistas invitados:

* *FINPUTDIR2=../../../files/db/graph/csv/2017/*

el directorio del CSV de artistas relevantes:

* *FINPUTDIR3=../../../file/db/graph/csv/relevants/*

el ranking maximo (recomendado 5 o menos):

* *RANK=5*

el año:

* *YEAR=2017*

y finalmente el directorio de salida:

* *FOUTDIR=../../../files/db/graph/output/viz/popcha/2017*


Compilar:

```
$ sbt clean; sbt package
```


Ejecutar con:

```
$ ./run_popcha.sh
```


Este programa genera JSON para:

**Concentric Circle Layout:**

* Copiar los archivos de salida en FOUTDIR y pegarlos en [graph/viz/viz_graph_ccl/](https://bitbucket.org/bigdata_famaf/tesis_illak/src/master/graph/viz/viz_graph_ccl/).

* Copiar las imágenes obtenidas de artistas en *graph/viz/viz_graph_ccl/images/artists/*

* Copiar las imágenes obtenidas de releases en *graph/viz/viz_graph_ccl/images/releases/*

* Abrir el archivo *graph/viz/viz_graph_ccl/years.csv* y agregar los años de los archivos obtenidos en formato CSV.
Por ejemplo si se tienen JSON de los años 2016 y 2017 el CSV debe ser de la forma:

```
years
2016
2017
```

* abrir *index.html* con el navegador.

**Path Layout:**

* Copiar los archivos de salida en FOUTDIR y pegarlos en [graph/viz/viz_graph_pl/](https://bitbucket.org/bigdata_famaf/tesis_illak/src/master/graph/viz/viz_graph_pl/).

* Copiar las imágenes obtenidas de artistas en *graph/viz/viz_graph_pl/images/artists/*

* Copiar las imágenes obtenidas de releases en *graph/viz/viz_graph_pl/images/releases/*

* Abrir el archivo *graph/viz/viz_graph_pl/years.csv* y agregar los años de los archivos obtenidos en formato CSV.
Por ejemplo si se tienen JSON de los años 2016 y 2017 el CSV debe ser de la forma:

```
years
2016
2017
```

* abrir *index.html* con el navegador.


---
El programa en [graph/json_programs/graph_popcha_individual/](https://bitbucket.org/bigdata_famaf/tesis_illak/src/master/graph/json_programs/graph_popcha_indivual/) genera JSON para el layout __*Concetric Single Circle Layout*__.
primero debemos editar el script *run_popcha_individual.sh* especificando directorios de entrada:

**NOTA**: los directorios son los mismos que en el programa anterior a excepción del directorio de salida.

* *FOUTDIR=../../../files/db/graph/output/viz/popcha_individual/2017*

Compilar:

```
$ sbt clean; sbt package
```

Ejecutar con:

```
$ ./run_popcha_individual.sh
```

Este programa genera JSON para:

**Concentric Single Circle Layout:**

* Copiar los archivos de salida en FOUTDIR y pegarlos en [graph/viz/viz_graph_cscl/](https://bitbucket.org/bigdata_famaf/tesis_illak/src/master/graph/viz/viz_graph_cscl/).

* Copiar las imágenes obtenidas de artistas en *graph/viz/viz_graph_cscl/images/artists/*

* Copiar las imágenes obtenidas de releases en *graph/viz/viz_graph_cscl/images/releases/*

* Abrir el archivo *graph/viz/viz_graph_cscl/years.csv* y agregar los años de los archivos obtenidos en formato CSV.
Por ejemplo si se tienen JSON de los años 2016 y 2017 el CSV debe ser de la forma:

```
years
2016
2017
```

* abrir *index.html* con el navegador.

---

El programa en [graph/json_programs/graph_path_tree/](https://bitbucket.org/bigdata_famaf/tesis_illak/src/master/graph/json_programs/graph_path_tree/) genera JSON para el layout __*Tree Layout*__.
Modificar el script *run_tree.sh* especificando directorios de entrada:

**Nota**: los directorios son los mismos que en los programas anteriores menos el parametro de RANK ya que el programa genera JSON para visualizar los caminos más relevantes, es decir aquellos de ranking == 1.

* *FOUTDIR=../../../files/db/graph/output/viz/tree_spotify/2017*


Compilar:

**Nota**: A diferencia de los programas anteriores en este caso es necesario generar un Fat Jar usando *sbt assembly* como se indica a continuación. Esto es debido al uso de librerias para I/O de Scala.

```
$ sbt clean; sbt assembly
```

Ejecutar con:

```
$ ./run_tree.sh
```

Este programa genera JSON para:

**Tree Layout:**

* Copiar los archivos de salida en FOUTDIR y pegarlos en [graph/viz/viz_tree/](https://bitbucket.org/bigdata_famaf/tesis_illak/src/master/graph/viz/viz_tree/).

* Copiar las imágenes obtenidas de artistas en *graph/viz/viz_tree/images/*

* Abrir el archivo *graph/viz/viz_tree/years.csv* y agregar los años de los archivos obtenidos en formato CSV.
Por ejemplo si se tienen JSON de los años 2016 y 2017 el CSV debe ser de la forma:

```
years
2016
2017
```

* abrir *index.html* con el navegador.

---

**NOTA**: Las imágenes de todos los años deben ir en el mismo directorio para cada visialización. No es necesario crear directorios diferentes por cada edición.
Por ejemplo, para la visualización *Concentric Circle Layout* todas las imágenes de
artistas para distintas ediciones (..., 2016, 2017, 2018, ...) deben ser copiadas en el
directorio: ```images/artists/```, de igual forma se debe proceder con las imágenes de *releases*.






