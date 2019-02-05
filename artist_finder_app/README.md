## App para búsqueda de artistas por edición de festival.


**Requisitos**:

* Scala

* Binarios de Spark

* SBT

### Instrucciones:

1) Ir a */src* y compilar:

```
$ scalac Finder.scala
```

Editar el script *`runFinder.sh`* especificando los directorios:

* *SPARK_HOME=`<directorio de binarios de Spark>`*
* *TMP2_DIR=`<directorio de parquets de las fuentes tmp2>`*
* *BDTARGET_DIR=`<directorio de parquets de la BD Target>`*
* *BDTARGET_UPDATE_DIR=`<directorio de salida de la BD Target actualizada>`*
* *BDTARGET_UPDATE=`<1=activate, 0=deactivate>`*

La opción *BDTARGET_UPDATE* es la que determina si la base de datos unificada (BD Target) será actualizada al finalizar la búsqueda de artistas invitados.

En caso de setear la variable a 1, luego de cada búsqueda el programa actualizará la BD Target (si se detectan cambios) agregando información proveniente de las fuentes complementarias (Musicbrainz y Wikidata) en caso de que esta información no haya sido agregada en la etapa de unificación (esto posiblemente debido a la ausencia de ID de Discogs en dichas fuentes). De esta manera el programa ayuda a mejorar la información existente en la BD unificada actualizando la información en caso de ser necesario.

La BD actualizada se guarda en el directorio especificado en *BDTARGET_UPDATE_DIR*.

En caso de que no se desee actualizar la BD unificada luego de cada búsqueda, setear la variable a 0. Esta opción probablemente sea deseada para realizar búsquedas rápidas sin tener que esperar por la actualización de la BD.

**Observación**: Cabe destacar que el objetivo principal del programa es la búsqueda de artistas en la BD Target y que como efecto secundario se hace la actualización de la BD al finalizar la búsqueda de los mismos.

---

2) Luego debemos compilar cada uno de los programas en spark que realizan la búsqueda y actualización de las tablas.
Para esto ir al directorio "/spark_programs" y ejecutar el script *`compile.sh`*.

```
$ ./compile.sh
```
---

3) Para la ejecución (de vuelta en directorio "/src") ejecutar el script *`runFinder.sh`*

```
$ ./runFinder.sh
```

Como primer instancia el programa le pedirá al usuario que ingrese el año de la edición a los que pertenecen los artistas invitados, esto es necesario para diferenciar directorios y logs generados.

Luego se pedirá el ingreso de una lista de nombres de artistas los cuales serán buscados en cada fuente.
La búsqueda de cada artista se realiza de la siguiente manera:

* Primero se busca al artista en la BD de Discogs. Se pueden dar dos posibles resultados:
    * En caso de no encontrarse el artista, la búsqueda en las otras fuentes queda desestimada, esto debido a que las relaciones entre artistas se basan en los creditos existentes en esta fuente, debido a esto se considera a Discogs como la base de datos principal.
    * En caso de encontrarse el artista, entonces se procede con la búsqueda del mismo en las fuentes complementarias, es decir en Musicbrainz y Wikidata. Esto con el fin de agregar información adicional que no se encuentre en la BD principal (por ejemplo nacionalidad o instrumento musical).

Para cada caso la búsqueda del artista devuelve una lista de artistas cuyos nombres coinciden con el ingresado por el usuario, pero además del nombre se devuelve el link al perfil del artista en cada fuente de datos. De esta manera el usuario puede digirse al perfil del artista y verificar que sea el correcto. Una vez verificado el artista se pedirá al usuario ingresar la opción correcta. 
Al finalizar la búsqueda de todos los artistas en la lista ingresada el programa procede con la comparación de las opciones seleccionadas por el usuario y los datos existentes en la BD. En el caso de que se detecten cambios en los mismos se procede con la actualización del parquet *dfArtists* que forma parte de la BD Target (esto último se hace en caso de haber activado la opción de actualizar la BD como se explicó anteriormente).

**NOTA**: Si el programa detecta y realiza la actualización de la BD Target (en particular el parquet *dfArtists.pqt*) se recomienda crear un nuevo directorio con la BD Target **actualizada**, esto con el fin preservar la versión original de la BD unificada. Luego usar este directorio como entrada de los siguientes programas, de esta manera el sistema siempre utilizará la última versión actualizada de la BD Target y tendremos un respaldo de la BD original.



### Salida:

El archivo *guests.csv* correspondiente al año deseado se genera en el mismo directorio del programa *Finder.scala*.
Tener en cuenta que este archivo CSV se sobreescribe con cada búsqueda realizada en el programa, por lo que se recomienda al finalizar el programa,
copiar el archivo en el directorio correspondiente desde donde será leido por el resto de los programas.

Los logs de salida del programa (en el caso de activarse la actualización de la BD Target) se almacenan en:

* /src/festival_ed_csvs
    - /2010
    - /2011
    - /2012
    - ...

Los logs se guardan en formato CSV y tienen dos columnas (mbID_STATUS y wkID_STATUS) que indican si se agrega información nueva o no a cada artista. Estas columnas pueden tener los siguientes valores:

* *DISTINCT*: Los ids eran distintos (el existente y el seleccionado por el usuario). El programa se queda con el id seleccionado por el usuario y por ende con toda la información asociada al nuevo ID.
* *MATCHED*: Los ids ya existian. No se agregan datos a los ya existentes.
* *ADDED*: El id no existia en la BD Target. El programa lo agrega junto con la información asociada a dicho ID.
* *ERROR*: Si no se han encontrado resultados en las BD complementarias o bien falla la asociación de datos nuevos.



**Importante**:

* En caso de que la compilación resulte en error, probar cambiando la versión de Java por una más reciente.