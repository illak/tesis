import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.graphframes._
import org.apache.spark.sql.types.{StructType, StructField, LongType, IntegerType}
import org.apache.spark.sql._
import org.apache.hadoop.fs._
import scala.collection.Map

import java.io.File

//import org.apache.log4j.Logger
//import org.apache.log4j.Level

object GraphPopcha {

    def main(args: Array[String]) {

        //  Logger.getLogger("info").setLevel(Level.OFF)

        if (args.length != 6) {
          Console.err.println("Need six arguments: <input dir paths parquet> <csv guests> <csv relevants> <rank filter parameter> <year> <output dir name>")
          sys.exit(1)
        }

        /* File names
         * ***********/
        val fDirIn1 = if (args(0).last != '/') args(0).concat("/") else args(0)
        val fDirIn2 = if (args(1).last != '/') args(1).concat("/") else args(1)
        val fDirIn3 = if (args(2).last != '/') args(2).concat("/") else args(2)
        val valRank = args(3).toInt
        val year = args(4)
        val fDirOut = if (args(5).last != '/') args(5).concat("/") else args(5)


        val spark = SparkSession
          .builder()
          .appName("graph viz with rank")
          .config("spark.some.config.option", "algun-valor")
          .config("spark.sql.crossJoin.enabled", "true")
          //.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        import spark.implicits._

        /* Load Files
         * ************/
        var pqtPaths = spark.read.option("mergeSchema", "true").parquet(fDirIn1 + "path_filter.pqt").cache
    

        val relev = spark.sqlContext.read
            .format("com.databricks.spark.csv")
            .option("header", "true") // Use first line of all files as header
            .option("inferSchema", "true") // Automatically infer data types
            .load(fDirIn3 + "relevants.csv")
            .rdd.map(r => (r.getInt(0), r.getString(1), r.getInt(2))).collect.toList

        // Listas de artistas por categoria
        val category1 = relev.filter(_._3 == 1).map(_._1).asInstanceOf[Seq[Long]]
        val category2 = relev.filter(_._3 == 2).map(_._1).asInstanceOf[Seq[Long]]

        val guests = spark.sqlContext.read
            .format("com.databricks.spark.csv")
            .option("header", "true") // Use first line of all files as header
            .option("inferSchema", "true") // Automatically infer data types
            .load(fDirIn2 + "guests.csv")
            .rdd.map(r => (r.getInt(0), r.getString(1))).collect.toList

        val guestMap = guests.map(_._1).zipWithIndex.map( t => (t._1.asInstanceOf[Long], t._2 + 1) ).toMap

        /*
        val artistImageMap = spark.sqlContext.read
            .format("com.databricks.spark.csv")
            .option("header", "true") // Use first line of all files as header
            .option("inferSchema", "true") // Automatically infer data types
            .load(fDirIn4 + s"artists_images_${year}.csv")
            .rdd.map(r => (r.getInt(0).asInstanceOf[Long], r.getString(1))).collect.toMap


        val releaseImageMap = spark.sqlContext.read
            .format("com.databricks.spark.csv")
            .option("header", "true") // Use first line of all files as header
            .option("inferSchema", "true") // Automatically infer data types
            .load(fDirIn4 + s"releases_images_${year}.csv")
            .rdd.map(r => (r.getInt(0).asInstanceOf[Long], r.getString(1))).collect.toMap*/

        // FUNCIONES AUXILIARES =============================================================================================
        //val getArtistImageURL = udf((id: Long) => artistImageMap(id))
        //val getReleasesImageURL = udf((ids: Seq[Long]) => ids.map(id => releaseImageMap(id)))

        val addImgExtension = udf((id: Long) => id.toString + ".jpg")
        val addImgExtensions = udf((ids: Seq[Long]) => ids.map(id => id.toString + ".jpg"))
        
        val onlyRelIds = udf((t: Seq[Row]) => {
            t.map{case Row(id:Long, title: String) => id}
        })
        val onlyRelTitles = udf((t: Seq[Row]) => {
            t.map{case Row(id:Long, title: String) => title}
        })

        // Función que asigna id normalizado usando diccionario
        def setIndexes(dict: Map[Long, Long]) = udf((n: Seq[Long]) => n.map(id => dict(id)))


        val head = udf((l: Seq[String]) => l.head)

        // Función que retorna lista de vertices a partir de un vertice v
        def selectPath(l: Seq[Seq[Long]]) = udf((v: Long) => {
          l.filter(_.contains(v)).flatten.distinct
        })


        //Función que setea la clase del nodo (necesario para la visualización)
        def setClass(listg: Seq[Int], listr: Seq[Int]) = udf((id: Int) => {
          if(listg.contains(id)){
            "guest"
          }
          else if(listr.contains(id)){
            "relevant"
          }
          else{
            "other"
          }
        })

        val wikiURL = udf((id: String) => "https://www.wikidata.org/wiki/" + id)
        val mbURL = udf((id: String) => "https://musicbrainz.org/artist/" + id)

        def setWeight(max: Int) = udf((n: Int) => n.toFloat/max)
        def setIndex(dict: Map[Long, Long]) = udf((n: Long) => dict(n))

        /** Writes dataframe to especified output as json format
          *
          * @param graphDF dataframe to save as json
          * @param outputDir output dir
          * @param name name of the file
          */
        def writeJson(graphDF: DataFrame, outputDir: String, name: String) = {
          val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

          graphDF.repartition(1).write.mode(SaveMode.Overwrite).json(outputDir + name)

          val file = fs.globStatus(new Path(outputDir + name + "/part*"))(0).getPath.getName

          fs.rename(new Path(outputDir + name + File.separatorChar + file),
            new Path(outputDir + File.separatorChar + name +".json"))

          fs.delete(new Path(outputDir + name), true)
        }

        def levelUdf(n: String) = udf((k: Int) => {
            n match {
                case "from" => k+1
                case "to"   => 0
                case _ if n.startsWith("v") => k - n.substring(1).toInt + 1
            }
        })

        def isVertice(n: String) = {
            n.startsWith("f") || n.startsWith("v") || n.startsWith("t")
        }

        def isEdge(n: String) = {
            n.startsWith("e")
        }

        //Sort Columns
        def rank(c: String): Double = {
            // from < e0 < v1 < e1 < ... < to
            c match {
              case "from" => 0.0
              case "to" => Double.PositiveInfinity
              case _ if c.startsWith("e") => 0.6 + c.substring(1).toInt
              case _ if c.startsWith("v") => 0.3 + c.substring(1).toInt
            }
        }

        val setPathID = udf((id: Long) => guestMap(id))


        def setListOfPaths(df: DataFrame, pathsDF: DataFrame, rank: Int, rank_filter: String, v_names: Seq[String]) : DataFrame = {
            (1 to rank).foldLeft(df)((df, r) => {

                val pathList = pathsDF.filter(col(rank_filter) <= r).select(array(v_names.map(n => col(s"$n.id")):_*))
                .rdd.map(r => r.getAs[Seq[Any]](0).filter(_ != null).asInstanceOf[Seq[Long]]).collect.toList

                df.withColumn(s"paths${r}", selectPath(pathList)($"id"))

            })
        }

        def setPathIndexes(df: DataFrame, idMap: Map[Long,Long], rank: Int) : DataFrame = {
            (1 to rank).foldLeft(df)((df, r) => {
                df.withColumn(s"links${r}", setIndexes(idMap)(col(s"paths${r}")))
            })
        }

        // devuelve la categoria asignada a un artista
        val category = udf((id: Long) => {
            if(category1.contains(id)){
                1
            }else if(category2.contains(id)){
                2
            }else 0
        })

        // devuelve distancia hacia relevante de categoria 1
        def getCat1Distance(cat1List: Seq[Long]) = udf((key: Int, id: Long) => {
            if(cat1List.contains(id)){
                key
            }else{
                -1
            }
        })

        //==================================================================================================================

        val rank_list = List(("rank_ci", s"ED${year}_RANKCI"),
                             ("rank_cii", s"ED${year}_RANKCII"))

        for((rank_filter, file_name) <- rank_list){

            //var pathsDF = pqtPaths.filter($"to.id" === tid).filter($"rank_cb" < valRank)
            var pathsDF = pqtPaths.filter(col(rank_filter) <= valRank)

            val ordered = pathsDF.columns.filter(c => isVertice(c) || isEdge(c)).sortBy(rank)
            val triplas = ordered.sliding(3,2).toSeq.map(l => (l(0), l(1), l(2)))


            // EDGES DATAFRAME
            val edges = triplas.map( t => {
                    pathsDF.select( col(s"${t._1}.id").as("Source"),
                                    size(col(s"${t._2}.id_title_list")).as("num_rel"),
                                    onlyRelIds(col(s"${t._2}.id_title_list")).as("releases"),
                                    onlyRelTitles(col(s"${t._2}.id_title_list")).as("titles"),
                                    col(rank_filter),
                                    setPathID($"from.id").as("path_id"),
                                    when(col(s"${t._1}.id").isNotNull && col(s"${t._3}.id").isNull, col("to.id"))
                                        .otherwise(col(s"${t._3}.id")).as("Target"))
                        .withColumn("images", addImgExtensions($"releases"))
                }).reduceLeft(_ union _).filter($"Source".isNotNull && $"Target".isNotNull).distinct

            val bidir_edges = edges.select($"Source", $"Target").union(edges.select($"Target".as("Source"), $"Source".as("Target")))


            val v_names = pathsDF.columns.filter(isVertice).sortBy(rank)

            // VERTICES DATAFRAME
            val vertices = v_names.map(n => {
                pathsDF.select(col(n).as("vstruct"), levelUdf(n)($"key").as("level"), getCat1Distance(category1)($"key", $"to.id").as("cat1Distance"))
            })
            .reduceLeft(_ union _).groupBy($"vstruct").agg(collect_set($"level").as("levels"), collect_set($"cat1Distance").as("cat1Distances"))
            .distinct()
            .select($"vstruct.*", $"levels", $"cat1Distances")
            .withColumn("category", category($"id"))


             val vertices_with_paths = setListOfPaths(vertices, pathsDF, valRank, rank_filter, v_names)
                .withColumn("class", setClass(guests.map(_._1), relev.map(_._1))($"id"))
                .withColumnRenamed("degree", "degree_origin")
                .filter($"id".isNotNull)

        
            // Armamos SUBGRAFO de la edicion del festival año: <<year>>
            // PARA CALCULO DE METRICAS CONVIENE PENSAR EL GRAFO COMO NO DIRIGIDO
            val subgraph = GraphFrame(vertices_with_paths, bidir_edges.select($"Source".as("src"), $"Target".as("dst")))

            val idList = subgraph.vertices.rdd.map(r => r.getLong(0)).collect.toList 
            val totalVertices = idList.length
            val totalPaths = pathsDF.count

            val spDF = subgraph.shortestPaths.landmarks(idList).run()


            // Betweeness Centrality (para visualizacion)
            // Caalculamos lista de caminos
            val verticesOnly = pathsDF.columns.filter(isVertice)
            val pathList = pathsDF
                .select(array(verticesOnly.map(n => col(s"${n}.id")):_*))
                .rdd.map(r => r.getAs[Seq[Any]](0).filter(_ != null).asInstanceOf[Seq[Long]]).collect.toList
            
            val computeBC = udf((id: Long) => pathList.map(l => if(l.contains(id)) 1.0 else 0.0).sum / totalPaths)

            val vertices_BC = subgraph.vertices.select($"id").withColumn("bc", computeBC($"id"))


            // Closeness Centrality (para visualizacion)
            val computeCC = udf((m: Map[Long, Int]) => {
                val sum = m.values.sum
                if(sum != 0.0) totalVertices / sum.toDouble else 0.0
            })

            val vertices_CC = spDF.withColumn("cc", computeCC($"distances")).drop($"distances")


            // Calculamos degree (para visualizacion)
            val vertices_deg = subgraph.degrees

            val vertices_deg_norm = vertices_deg
                .join(vertices_CC, Seq("id"), "inner")
                .join(vertices_BC, Seq("id"), "inner")

            //================================================================================================================
            // Agregamos columna de ids por indice (de 0 a n, requerido por d3)
            val vertices_schema = vertices_deg_norm.schema

            val inputRows = vertices_deg_norm.rdd.zipWithIndex.map{ case (r: Row, id: Long) => Row.fromSeq(id +: r.toSeq) }


            val vertices_with_index = spark.sqlContext
                .createDataFrame(inputRows, StructType(StructField("index", LongType, nullable = false) +: vertices_schema.fields))


            // Generamos un diccionario de id -> index
            val idMap = vertices_with_index.select($"id", $"index").rdd.map(r => (r.getLong(0), r.getLong(1))).collectAsMap()
            //================================================================================================================


            val vertices_struct = setPathIndexes(vertices_with_index, idMap, valRank)
                .withColumn("image", addImgExtension($"id"))
                .withColumn("mbLink", when($"musicbrainz_id".isNotNull, mbURL($"musicbrainz_id")).otherwise(null))
                .withColumn("wikiLink", when($"wikidata_id".isNotNull, wikiURL($"wikidata_id")).otherwise(null))
                .orderBy(asc("index"))
                .select(struct(Seq(
                    col("index"),
                    col("id"),
                    col("class"),
                    col("name"),
                    col("image"),
                    col("degree"),
                    col("degree_origin"),
                    col("mbLink"),
                    col("wikiLink"),
                    col("ci"),
                    col("bc"),
                    col("cc"),
                    col("cii"),
                    col("category"),
                    col("cat1Distances"),
                    col("citizenships"),
                    col("instruments"),
                    col("levels")) ++ (1 to valRank).map(i => col(s"links${i}")):_*).as("struct"))
                .withColumn("myID", lit(1))
                .groupBy($"myID").agg(collect_list($"struct").as("nodes")).select($"nodes", $"myID")


            val max_num_rel = edges.agg(max($"num_rel")).collect()(0).getInt(0)

            val edges_struct = edges.distinct
                .withColumn("weight", setWeight(max_num_rel)($"num_rel"))
                .withColumn("source", setIndex(idMap)($"Source"))
                .withColumn("target", setIndex(idMap)($"Target"))
                //.groupBy($"source", $"target", $"weight", $"releases", $"path_id", col(rank_filter))//.agg( min(col(rank_filter)).as(rank_filter))
                .select(struct($"source", $"target", $"weight", $"releases", $"titles", $"images", $"path_id", col(rank_filter)).as("struct"))
                .withColumn("myID", lit(1))
                .groupBy($"myID").agg(collect_list($"struct").as("links")).select($"links", $"myID")


            val graphDF = vertices_struct.join(edges_struct, Seq("myID"), "leftouter").select($"nodes", $"links")

            writeJson(graphDF, fDirOut, file_name)


        }
    }
}
