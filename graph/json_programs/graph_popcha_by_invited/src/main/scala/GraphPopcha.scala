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

  // More functions
  /** Writes dataframe to especified output as json format
    *
    * @param graphDF dataframe to save as json
    * @param outputDir output dir
    * @param name name of the file
    */
  def writeJson(graphDF: DataFrame, outputDir: String, name: String, spark: SparkSession) = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    graphDF.repartition(1).write.mode(SaveMode.Overwrite).json(outputDir + name)

    val file = fs.globStatus(new Path(outputDir + name + "/part*"))(0).getPath.getName

    fs.rename(new Path(outputDir + name + File.separatorChar + file),
      new Path(outputDir + File.separatorChar + name +".json"))

    fs.delete(new Path(outputDir + name), true)
  }


  def main(args: Array[String]) {

        //  Logger.getLogger("info").setLevel(Level.OFF)

        if (args.length != 7) {
          Console.err.println("Need six arguments: "
            + "<input dir paths parquet>"
            + "<csv guests>"
            + "<csv relevants>"
            + "<input dir transform final parquet>"
            + "<rank filter parameter>"
            + "<year>"
            + "<output dir name>")
          sys.exit(1)
        }

        /* File names
         * ***********/
        val fDirIn1 = if (args(0).last != '/') args(0).concat("/") else args(0)
        val fDirIn2 = if (args(1).last != '/') args(1).concat("/") else args(1)
        val fDirIn3 = if (args(2).last != '/') args(2).concat("/") else args(2)
        val fDirIn4 = if (args(3).last != '/') args(3).concat("/") else args(3)
        val valRank = args(4).toInt
        val year = args(5)
        val fDirOut = if (args(6).last != '/') args(6).concat("/") else args(6)


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

        var dfReleases = spark.read.parquet(fDirIn4 + "final/dfReleases.pqt").cache()

        val relev: List[(Int, String, Int)] = spark.sqlContext.read
            .format("com.databricks.spark.csv")
            .option("header", "true") // Use first line of all files as header
            .option("inferSchema", "true") // Automatically infer data types
            .load(fDirIn3 + "relevants.csv")
            .rdd.map(r => (r.getInt(0), r.getString(1), r.getInt(2))).collect.toList

        // Listas de artistas por categoria
        val category1: Seq[Long] = relev.filter(_._3 == 1).map(_._1).asInstanceOf[Seq[Long]]
        val category2: Seq[Long] = relev.filter(_._3 == 2).map(_._1).asInstanceOf[Seq[Long]]

        val guests: List[(Int, String)] = spark.sqlContext.read
            .format("com.databricks.spark.csv")
            .option("header", "true") // Use first line of all files as header
            .option("inferSchema", "true") // Automatically infer data types
            .load(fDirIn2 + "guests.csv")
            .rdd.map(r => (r.getInt(0), r.getString(1))).collect.toList

        // Map each id with its position
        val guestMap: Map[Long,Int] = guests.map(_._1).zipWithIndex.map( t => (t._1.asInstanceOf[Long], t._2 + 1) ).toMap

        // UDF definitions
        val addImgExtension = udf((id: Long) => id.toString + ".jpg")
//        val addImgExtensions = udf((ids: Seq[Long]) => ids.map(id => id.toString + ".jpg"))
        
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
        // UDFs to extract artists urlrs
        val getWikipediaUrlUDF = udf((urls: Seq[String], wikiLinks: Seq[String], otherUrls: Seq[String]) =>{

          val allUrls = (urls ++ wikiLinks ++ otherUrls).filter(_ != null)

          val wikiUrls = allUrls.filter(_.contains("wikipedia.org"))
            .filter(! _.contains("commons.wikipedia.org"))

          val wikiUrlsOrd = wikiUrls.filter(_.contains("es.wikipedia.org")) ++ wikiUrls.filter(_.contains("en.wikipedia.org")) ++ wikiUrls

          wikiUrlsOrd.headOption

        })

        val getHomePageUrlUDF = udf((name: String, urls: Seq[String], wikiLinks: Seq[String], otherUrls: Seq[String]) =>{

          val names = name.split(" ").map(_.replaceAll("""\(\d*\)$""","").toLowerCase).filter(_ != "")

          val allUrls = (urls ++ wikiLinks ++ otherUrls).filter(_ != null)

          val comDomain = allUrls.filter(_.endsWith(".com")) ++ allUrls.filter(_.contains(".com/")) // first end with com

          val notOthers = comDomain
            .filter(!_.contains("discogs.com"))
            .filter(!_.contains("twitter.com"))
            .filter(!_.contains("imdb.com"))
            .filter(!_.contains("facebook.com"))
            .filter(!_.contains("myspace.com"))
            .filter(!_.contains("instagram.com"))
            .filter(!_.contains("repertoire.bmi.com"))

          val withAllName = notOthers.filter(url => names.forall(url.toLowerCase.split("""\.com""").head.contains(_)))
            .sortWith(_.length < _.length) // ocam?

          val withLastName = notOthers.filter(_.toLowerCase.split("""\.com""").head.contains(names.last))
            .sortWith(_.length < _.length) // ocam?

          val withSomeName = notOthers.filter(url => names.exists(url.toLowerCase.split("""\.com""").head.contains(_)))
            .sortWith(_.length < _.length) // ocam?

          (withAllName ++ withLastName ++ withSomeName).headOption

        })
        def getUrlUDF(sSearch: String) =
          udf((urls: Seq[String], wikiLinks: Seq[String], otherUrls: Seq[String]) =>{

            val allUrls = (urls ++ wikiLinks ++ otherUrls).filter(_ != null)

            val sSearch1 = "//www." + sSearch + "/"
            val sSearch2 = "//" + sSearch + "/"

            val results1 = allUrls.filter(_.toLowerCase().contains(sSearch1))
              .sortWith(_.length < _.length) // ocam?

            val results2 = allUrls.filter(_.toLowerCase().contains(sSearch2))
              .sortWith(_.length < _.length) // ocam?

            (results1 ++ results2).headOption

          })

        val getAllMusicUrl = getUrlUDF("allmusic.com")
        val getFacebookUrl = getUrlUDF("facebook.com")
        val getImdbUrl = getUrlUDF("imdb.com")
        val getMySpaceUrl = getUrlUDF("myspace.com")
        val getInstagramUrl = getUrlUDF("instagram.com")


        val getDateOfBirthUDF = udf((dateOfBirth: Seq[Row]) =>{
          if (dateOfBirth != null)
            dateOfBirth.map(r => (r.getAs[String]("_2"),r.getAs[String]("_1")))
              .toMap.get("Q1985727")
          else
            None
          //dateOfBirth.filter(!_.isNullAt(0)).map(_.getAs[String]("_1")).headOption
          //dateOfBirth.map(_.swap).toMap.get("Q1985727")
        })

    //==================================================================================================================

        val (rank_filter, base_file_name) = ("rank_cii", s"ED${year}_RANKCII")

        for((guestId, guestName) <- guests){

            //var pathsDF = pqtPaths.filter($"to.id" === tid).filter($"rank_cb" < valRank)
            var pathsDF: DataFrame = pqtPaths.filter(col(rank_filter) <= valRank)
              .filter($"from.id" === guestId)

            // Only from, vi, ei, to columns ordered by function rank defined before
            val ordered: Array[String] =
              pathsDF.columns.filter(c => isVertice(c) || isEdge(c)).sortBy(rank)

            // one triple is vertex->edge->vertex
            // for example Seq((from,e0,v1), (v1,e1,v2), (v2,e2,v3), (v3,e3,to))
            val triplas: Seq[(String, String, String)] =
              ordered.sliding(3,2).toSeq.map(l => (l(0), l(1), l(2)))


            // EDGES DATAFRAME
            val edges1: DataFrame = triplas.map( t => {
                    pathsDF.select( col(s"${t._1}.id").as("Source"),
                                    size(col(s"${t._2}.id_title_list")).as("num_rel"),
                                    onlyRelIds(col(s"${t._2}.id_title_list")).as("releases"),
                                    onlyRelTitles(col(s"${t._2}.id_title_list")).as("titles"),
                                    col(rank_filter),
                                    setPathID($"from.id").as("path_id"),
                                    when(col(s"${t._1}.id").isNotNull && col(s"${t._3}.id").isNull, col("to.id"))
                                        .otherwise(col(s"${t._3}.id")).as("Target"))
//                        .withColumn("images", addImgExtensions($"releases"))
                }).reduceLeft(_ union _).filter($"Source".isNotNull && $"Target".isNotNull)
              .distinct
              .cache()

          val bidir_edges = edges1.select($"Source", $"Target")
            .union(edges1.select($"Target".as("Source"), $"Source".as("Target")))

          // Add info releases.
            // Join edges to add.
            val edgesByRelease = edges1.withColumn("id_release",explode($"releases"))

            val colsEdgesGroupBy =  edgesByRelease.columns
              .diff(Seq("id_release", "releases", "titles"))
              .map(col(_))
            // Array(Source, num_rel, titles, rank_cii, path_id, Target, images)

            val edges: DataFrame = edgesByRelease.join(dfReleases, "id_release")
              .groupBy(colsEdgesGroupBy: _*)
              .agg(collect_list(struct( $"id_release",
                                        $"title",
                                        addImgExtension($"id_release").as("image"),
                                        $"released",
                                        $"country",
                                        $"genres",
                                        $"labels",
                                        $"notes",
                                        $"primary_artists",
                                        $"primary_artist_names"
              )).as("releases_data"))

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
                .withColumnRenamed("name", "old_name")
                .withColumn("name", regexp_replace($"old_name", """[\s\(\d\)]*$""", "")) // remove (n) ending
                .withColumn("image", addImgExtension($"id"))
                .withColumn("mbLink", when($"musicbrainz_id".isNotNull, mbURL($"musicbrainz_id")).otherwise(null))
                .withColumn("wikiLink", when($"wikidata_id".isNotNull, wikiURL($"wikidata_id")).otherwise(null))
                .withColumn("wikipediaUrl", getWikipediaUrlUDF($"urls", $"wikidata_links", $"other_urls"))
                .withColumn("allMusicUrl", getAllMusicUrl($"urls", $"wikidata_links", $"other_urls"))
                .withColumn("facebookUrl", getFacebookUrl($"urls", $"wikidata_links", $"other_urls"))
                .withColumn("imdbUrl", getImdbUrl($"urls", $"wikidata_links", $"other_urls"))
                .withColumn("mySpaceUrl", getMySpaceUrl($"urls", $"wikidata_links", $"other_urls"))
                .withColumn("instagramUrl", getInstagramUrl($"urls", $"wikidata_links", $"other_urls"))
                .withColumn("homePageUrl", getHomePageUrlUDF($"name", $"urls", $"wikidata_links", $"other_urls"))
                .withColumn("dateOfBirthStr", getDateOfBirthUDF($"date_of_birth_calendar"))
                .withColumn("dateOfBirth", to_date($"dateOfBirthStr","'+'yyyy-MM-dd'T'HH:mm:ss'Z'")) //+1961-10-20T00:00:00Z
                .withColumn("yearOfBirth", org.apache.spark.sql.functions.year($"dateOfBirth"))
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
                    col("wikipediaUrl"),
                    col("allMusicUrl"),
                    col("facebookUrl"),
                    col("imdbUrl"),
                    col("mySpaceUrl"),
                    col("instagramUrl"),
                    col("homePageUrl"),
                    col("dateOfBirth"),
                    col("yearOfBirth"),
                    col("profile"),
                    col("levels")) ++ (1 to valRank).map(i => col(s"links${i}")):_*).as("struct"))
                .withColumn("myID", lit(1))
                .groupBy($"myID").agg(collect_list($"struct").as("nodes")).select($"nodes", $"myID")


            val max_num_rel = edges.agg(max($"num_rel")).collect()(0).getInt(0)

            val edges_struct = edges.distinct
                .withColumn("weight", setWeight(max_num_rel)($"num_rel"))
                .withColumn("source", setIndex(idMap)($"Source"))
                .withColumn("target", setIndex(idMap)($"Target"))
                //.groupBy($"source", $"target", $"weight", $"releases", $"path_id", col(rank_filter))//.agg( min(col(rank_filter)).as(rank_filter))
                .select(struct( $"source", $"target", $"weight",
                                $"releases_data",
                                $"path_id", col(rank_filter)).as("struct"))
                .withColumn("myID", lit(1))
                .groupBy($"myID").agg(collect_list($"struct").as("links")).select($"links", $"myID")

            val graphDF = vertices_struct.join(edges_struct, Seq("myID"), "leftouter").select($"nodes", $"links")

            writeJson(graphDF, fDirOut, base_file_name + "_" + guestName, spark)

            edges1.unpersist()

        }
    }
}
