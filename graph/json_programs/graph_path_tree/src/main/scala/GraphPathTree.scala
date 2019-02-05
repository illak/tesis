import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.write

import sys.process._
import java.io.{Console => _, _} // Para evitar conflictos con Console de scala
import java.nio.file.{Paths, Files}

import better.files.{File => File2, _}
import better.files.File._

//import org.apache.log4j.Logger
//import org.apache.log4j.Level

object GraphPathTree {

    implicit val formats = DefaultFormats
    case class Artist(  name: String, 
                        image: Option[String], 
                        discogs_id: Option[Long], 
                        children: Option[List[Artist]],
                        mbLink: Option[String],
                        wikiLink: Option[String],
                        wikipediaUrl: Option[String],
                        allMusicUrl: Option[String],
                        facebookUrl: Option[String],
                        imdbUrl: Option[String],
                        mySpaceUrl: Option[String],
                        instagramUrl: Option[String],
                        homePageUrl: Option[String],
                        dateOfBirthStr: Option[String],
                        dateOfBirth: Option[java.sql.Date],
                        yearOfBirth: Option[Int],
                        citizenships: Option[Seq[String]],
                        instruments: Option[Seq[String]],
                        profile: Option[String])

    case class Release( title: Option[String],
                        image: Option[String],
                        released: Option[String],
                        country: Option[String],
                        genres: Option[Seq[String]],
                        labels: Option[Seq[String]],
                        notes: Option[String],
                        primary_artists: Option[Seq[Long]],
                        primary_artist_names: Option[Seq[String]])
    
    case class Link(src: Long, dst: Long, releases_data: Seq[Release])
    case class Links(links: Seq[Link])

    def main(args: Array[String]) {

        //  Logger.getLogger("info").setLevel(Level.OFF)

        if (args.length != 6) {
          Console.err.println("Need six arguments: "
            + "<input dir paths parquet> "
            + "<csv guests> "
            + "<csv relevants> "
            + "<year> "
            + "<output dir name> "
            + "<releases dir name>")
          sys.exit(1)
        }

        /* File names
         * ***********/
        val fDirIn1 = if (args(0).last != '/') args(0).concat("/") else args(0)
        val fDirIn2 = if (args(1).last != '/') args(1).concat("/") else args(1)
        val fDirIn3 = if (args(2).last != '/') args(2).concat("/") else args(2)
        val year = args(3).toInt
        val fDirOut = if (args(4).last != '/') args(4).concat("/") else args(4)
        val fDirReleases = if (args(5).last != '/') args(5).concat("/") else args(5)

        val spark = SparkSession
          .builder()
          .appName("Graph tree")
          .config("spark.some.config.option", "algun-valor")
          .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")

        import spark.implicits._

        /* Load Files
         * ************/

        var pqtPaths = spark.read.option("mergeSchema", "true").parquet(fDirIn1 + "path_filter.pqt")

        val relev = spark.sqlContext.read
            .format("com.databricks.spark.csv")
            .option("header", "true") // Use first line of all files as header
            .option("inferSchema", "true") // Automatically infer data types
            .load(fDirIn3 + "relevants.csv")
            .rdd.map(r => (r.getInt(0), r.getString(1))).collect.toList


        val guests = spark.sqlContext.read
            .format("com.databricks.spark.csv")
            .option("header", "true") // Use first line of all files as header
            .option("inferSchema", "true") // Automatically infer data types
            .load(fDirIn2 + "guests.csv")
            .rdd.map(r => (r.getInt(0), r.getString(1))).collect.toList


        val head = udf((l: Seq[String]) => l.head)

        /*val imagesMap = spark.sqlContext.read
            .format("com.databricks.spark.csv")
            .option("header", "true") // Use first line of all files as header
            .option("inferSchema", "true") // Automatically infer data types
            .load(fDirIn4 + "artists_images.csv")
            .rdd.map(r => (r.getInt(0).asInstanceOf[Long], r.getString(1))).collect.toMap*/



        /** Writes json string to especified dir with especified name
          *
          * @param json string with json format
          * @param fDirOut output dir name
          * @param rname relevant artis name
          */
        def writeToFile(json: String, fDirOut: String, rname: String){

            val dir: File2 = fDirOut
                .toFile
                .createIfNotExists(true, true)

            val outputDir = dir + "/" + rname + ".json"
            val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputDir)))
            writer.write(json)
            writer.close()
        }

        /** Return columns with graphframe's BFS schema
          * e0 | v1 | e1 | v2 | ... | vn | em-1
          *
          * @param n length of array with vertices
          * @param m length of array with edges
          * @param v name of column with array of vertices
          * @param e name of column with array of edges
          */
        def colFromArrays(n: Int, m: Int, v: String, e: String) = {
    
            val cols = (0 until m + 1).map(i => "e" + i.toString).zipWithIndex
                            .zip((0 until n + 1).map(i => "v" + (i + 1).toString).zipWithIndex)
                            .flatMap{ case (a, b) => List(a,b) }.dropRight(1).toList

            cols.map{ case (c, i) => {
                if (c.startsWith("e")) col(e).getItem(i).as(c) else col(v).getItem(i).as(c)
            }}
        }

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

        def verticesFromKey(k: Int) = {
            "from" +: (1 to k).map(i => "v" + i.toString) :+ "to"
        }

        val addImgExtension = udf((id: String) => id + ".jpg")

        //val getImageURL = udf((id: Long) => imagesMap(id))

        val schema = List(StructField("json", StringType, true))

        val pathFilteredByRank = pqtPaths.filter($"rank_cii" <= 1)

        //=================================================================
        // UDFS
        val wikiURL = udf((id: String) => "https://www.wikidata.org/wiki/" + id)
        val mbURL = udf((id: String) => "https://musicbrainz.org/artist/" + id)
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



        val idProfileDF = pathFilteredByRank.columns
            .filter(isVertice)
            .map(c => pathFilteredByRank.select(col(c+".id").as("id"), col(c+".profile").as("profile")))
            .reduce(_ union _)
            .na.drop()

        idProfileDF.filter($"id".isNull).show()
        idProfileDF.filter($"id".isNotNull).show()


        val idProfileDict = idProfileDF.rdd
            .map(r => (r.getLong(0), r.getString(1)))
            .collectAsMap()


        //=================================================================
        // CREATION OF JSON WITH EDGES


        val releases = spark.read.parquet(fDirReleases + "dfReleases.pqt")

        releases.show()
        releases.printSchema

        val edgesNames      = pathFilteredByRank.columns.filter(isEdge)
        val pathsOnlyEdges  = pathFilteredByRank.select(edgesNames.map(col):_*)
        
        val edgesUnion = pathsOnlyEdges.columns
            .map(n => pathsOnlyEdges.select(col(n))).reduce(_.union(_))
            .na.drop()
            .select($"e0.src".as("src"), $"e0.dst".as("dst"), explode($"e0.id_title_list._1").as("id_release"))

        val edgesWithInfo = edgesUnion.join(releases, Seq("id_release"), "inner")
            .groupBy($"src", $"dst")
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

        val edges = edgesWithInfo.select(to_json(struct($"src", $"dst", $"releases_data")).as("json"))

        val jsonLinks  = edges.select($"json").distinct.rdd.map(r => r.getString(0)).collect.toList

        val finalJsons = jsonLinks.map(j => parse(j).extract[Link])

        val rootLink = Links(links = finalJsons)

        val toFileLink = write(rootLink)

        writeToFile(toFileLink, fDirOut, s"tree_graph_links_${year.toString}")



        /*val edgesNames    = pathFilteredByRank.columns.filter(isEdge)
        val pathsOnlyEdges  = pathFilteredByRank.select(edgesNames.map(col):_*)
        
        val edges = pathsOnlyEdges.columns
            .map(n => pathsOnlyEdges.select(col(n))).reduce(_.union(_))
            .select(to_json(col("e0")).as("json")).na.drop()

        val jsonLinks  = edges.select($"json").distinct.rdd.map(r => r.getString(0)).collect.toList

        val finalJsons = jsonLinks.map(j => parse(j).extract[Link])

        val rootLink = Links(links = finalJsons)

        val toFileLink = write(rootLink)

        writeToFile(toFileLink, fDirOut, s"tree_graph_links_${year.toString}")*/


        //=================================================================

        val guestList = pathFilteredByRank.select(  $"from.id",
                                                    $"from.name",
                                                    addImgExtension($"from.id".cast(StringType)),
                                                    when($"from.musicbrainz_id".isNotNull, mbURL($"from.musicbrainz_id")).otherwise(null).as("mbLink"),
                                                    when($"from.wikidata_id".isNotNull, wikiURL($"from.wikidata_id")).otherwise(null).as("wikiLink"),
                                                    getWikipediaUrlUDF($"from.urls", $"from.wikidata_links", $"from.other_urls").as("wikipediaUrl"),
                                                    getAllMusicUrl($"from.urls", $"from.wikidata_links", $"from.other_urls").as("allMusicUrl"),
                                                    getFacebookUrl($"from.urls", $"from.wikidata_links", $"from.other_urls").as("facebookUrl"),
                                                    getImdbUrl($"from.urls", $"from.wikidata_links", $"from.other_urls").as("imdbUrl"),
                                                    getMySpaceUrl($"from.urls", $"from.wikidata_links", $"from.other_urls").as("mySpaceUrl"),
                                                    getInstagramUrl($"from.urls", $"from.wikidata_links", $"from.other_urls").as("instagramUrl"),
                                                    getHomePageUrlUDF($"from.name", $"from.urls", $"from.wikidata_links", $"from.other_urls").as("homePageUrl"),
                                                    getDateOfBirthUDF($"from.date_of_birth_calendar").as("dateOfBirthStr"),
                                                    to_date(getDateOfBirthUDF($"from.date_of_birth_calendar"),"'+'yyyy-MM-dd'T'HH:mm:ss'Z'").as("dateOfBirth"), //+1961-10-20T00:00:00Z
                                                    org.apache.spark.sql.functions.year(
                                                        to_date(getDateOfBirthUDF($"from.date_of_birth_calendar"),"'+'yyyy-MM-dd'T'HH:mm:ss'Z'")).as("yearOfBirth"),
                                                    $"from.citizenships",
                                                    $"from.instruments",
                                                    $"from.profile"
                                                ).distinct
                                                .rdd.map(r => ( r.getLong(0), 
                                                                r.getString(1), 
                                                                r.getString(2),
                                                                r.getString(3),
                                                                r.getString(4),
                                                                r.getString(5),
                                                                r.getString(6),
                                                                r.getString(7),
                                                                r.getString(8),
                                                                r.getString(9),
                                                                r.getString(10),
                                                                r.getString(11),
                                                                r.getString(12),
                                                                r.getDate(13),
                                                                if(!r.isNullAt(14)) r.getInt(14) else 0,
                                                                r.getSeq(15),
                                                                r.getSeq(16),
                                                                r.getString(17)
                                                                )).collect.toList


        val cleanJson = udf((s: String) => {
            val remove1 = s.replace("\\", "")
            val remove2 = remove1.replace("\"{", "{")
            val remove3 = remove2.replace("}\"", "}")
            val remove4 = remove3.replace("\"[", "[")
            remove4.replace("]\"", "]")
            })

        def addProfileChildren(artL: List[Artist]) : List[Artist] = {
            artL.map(addProfile)
        }

        def addProfile(art: Artist) : Artist = {

            // El id siempre está, ver como evitar el "warning" del caso None
            val id = art.discogs_id match {
                case Some(id) => id
            }
            art.children match {
                case Some(artists) => art.copy(profile = idProfileDict.get(id), children = Some(addProfileChildren(artists)))
                case None => art.copy(profile = idProfileDict.get(id))
            }

            //art.copy(profile = idProfileDict.get(id), children = children)
        }

        val gList = guestList.map{ case (   fid,
                                            fname, 
                                            image_link,
                                            mbLink,
                                            wikiLink,
                                            wikipediaUrl,
                                            allMusicUrl,
                                            facebookUrl,
                                            imdbUrl,
                                            mySpaceUrl,
                                            instagramUrl,
                                            homePageUrl,
                                            dateOfBirthStr,
                                            dateOfBirth,
                                            yearOfBirth,
                                            citizenships,
                                            instruments,
                                            profile
                                            ) => {

            var pathsJson = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], StructType(schema))

            val pathsFilteredByArtists = pathFilteredByRank.filter($"from.id" === fid)

            val v_names = pathsFilteredByArtists.columns.filter(isVertice).sortBy(rank)

            var paths = pathsFilteredByArtists.select(v_names.map(col):_*).cache

            val numCols = paths.columns.length


            // Modificamos el dataframe para que tenga el esquema json necesario
            for ((c,i) <- paths.columns.zipWithIndex.reverse){
                if(c.startsWith("t")){
                    paths = paths.withColumn(c, to_json(
                                                struct( paths(c+".name").as("name"),
                                                        addImgExtension(paths(c+".id").cast(StringType)).as("image"),
                                                        paths(c+".id").as("discogs_id"),
                                                        when(paths(c+".musicbrainz_id").isNotNull, mbURL(paths(c+".musicbrainz_id"))).otherwise(null).as("mbLink"),
                                                        when(paths(c+".wikidata_id").isNotNull, wikiURL(paths(c+".wikidata_id"))).otherwise(null).as("wikiLink"),
                                                        getWikipediaUrlUDF(paths(c+".urls"), paths(c+".wikidata_links"), paths(c+".other_urls")).as("wikipediaUrl"),
                                                        getAllMusicUrl(paths(c+".urls"), paths(c+".wikidata_links"), paths(c+".other_urls")).as("allMusicUrl"),
                                                        getFacebookUrl(paths(c+".urls"), paths(c+".wikidata_links"), paths(c+".other_urls")).as("facebookUrl"),
                                                        getImdbUrl(paths(c+".urls"), paths(c+".wikidata_links"), paths(c+".other_urls")).as("imdbUrl"),
                                                        getMySpaceUrl(paths(c+".urls"), paths(c+".wikidata_links"), paths(c+".other_urls")).as("mySpaceUrl"),
                                                        getInstagramUrl(paths(c+".urls"), paths(c+".wikidata_links"), paths(c+".other_urls")).as("instagramUrl"),
                                                        getHomePageUrlUDF(paths(c+".name"), paths(c+".urls"), paths(c+".wikidata_links"), paths(c+".other_urls")).as("homePageUrl"),
                                                        getDateOfBirthUDF(paths(c+".date_of_birth_calendar")).as("dateOfBirthStr"),
                                                        to_date(getDateOfBirthUDF(paths(c+".date_of_birth_calendar")),"'+'yyyy-MM-dd'T'HH:mm:ss'Z'").as("dateOfBirth"), //+1961-10-20T00:00:00Z
                                                        org.apache.spark.sql.functions.year(
                                                            to_date(getDateOfBirthUDF(paths(c+".date_of_birth_calendar")),"'+'yyyy-MM-dd'T'HH:mm:ss'Z'")).as("yearOfBirth"),
                                                        paths(c+".citizenships").as("citizenships"),
                                                        paths(c+".instruments").as("instruments")
                                                        //paths(c+".profile").as("profile")
                                                        )))

                    //println("schema to...")
                    //paths.printSchema

                    //paths.select(paths.columns(i)).show(false)
                }
                else if(c.startsWith("f")){
                    paths = paths.withColumn("child", paths(paths.columns(i+1)))
                }
                else{
                    val byID = Window.partitionBy(paths(c))


                    val structCurrent = to_json(struct( paths(c+".name").as("name"),
                                                        addImgExtension(paths(c+".id").cast(StringType)).as("image"),
                                                        paths(c+".id").as("discogs_id"),
                                                        collect_set(paths(paths.columns(i+1))) over byID as "children",
                                                        when(paths(c+".musicbrainz_id").isNotNull, mbURL(paths(c+".musicbrainz_id"))).otherwise(null).as("mbLink"),
                                                        when(paths(c+".wikidata_id").isNotNull, wikiURL(paths(c+".wikidata_id"))).otherwise(null).as("wikiLink"),
                                                        getWikipediaUrlUDF(paths(c+".urls"), paths(c+".wikidata_links"), paths(c+".other_urls")).as("wikipediaUrl"),
                                                        getAllMusicUrl(paths(c+".urls"), paths(c+".wikidata_links"), paths(c+".other_urls")).as("allMusicUrl"),
                                                        getFacebookUrl(paths(c+".urls"), paths(c+".wikidata_links"), paths(c+".other_urls")).as("facebookUrl"),
                                                        getImdbUrl(paths(c+".urls"), paths(c+".wikidata_links"), paths(c+".other_urls")).as("imdbUrl"),
                                                        getMySpaceUrl(paths(c+".urls"), paths(c+".wikidata_links"), paths(c+".other_urls")).as("mySpaceUrl"),
                                                        getInstagramUrl(paths(c+".urls"), paths(c+".wikidata_links"), paths(c+".other_urls")).as("instagramUrl"),
                                                        getHomePageUrlUDF(paths(c+".name"), paths(c+".urls"), paths(c+".wikidata_links"), paths(c+".other_urls")).as("homePageUrl"),
                                                        getDateOfBirthUDF(paths(c+".date_of_birth_calendar")).as("dateOfBirthStr"),
                                                        to_date(getDateOfBirthUDF(paths(c+".date_of_birth_calendar")),"'+'yyyy-MM-dd'T'HH:mm:ss'Z'").as("dateOfBirth"), //+1961-10-20T00:00:00Z
                                                        org.apache.spark.sql.functions.year(
                                                            to_date(getDateOfBirthUDF(paths(c+".date_of_birth_calendar")),"'+'yyyy-MM-dd'T'HH:mm:ss'Z'")).as("yearOfBirth"),
                                                        paths(c+".citizenships").as("citizenships"),
                                                        paths(c+".instruments").as("instruments")
                                                        //paths(c+".profile").as("profile")
                                                        ))

                    paths = paths.withColumn(c, when(paths(c).isNotNull, cleanJson(structCurrent))
                        .otherwise(paths(paths.columns(i+1))))

                    //println(s"showing ${i}th: ")
                    //paths.select(col(c)).show((false))

                    //println("schema vertice...")
                    //paths.printSchema

                    //paths.select(paths.columns(i)).show(false)

                }
            }

            pathsJson = paths.distinct.select($"child".as("json"))

            //pathsJson.show(false)

            val jsons  = pathsJson.select($"json").distinct.rdd.map(r => r.getString(0)).collect.toList

            val artists = jsons.map(j => addProfile(parse(j).extract[Artist]))

            Artist( name = fname,
                    image = Some(image_link),
                    discogs_id = Some(fid),
                    children = Some(artists),
                    mbLink = Some(mbLink),
                    wikiLink = Some(wikiLink),
                    wikipediaUrl = Some(wikipediaUrl),
                    allMusicUrl = Some(allMusicUrl),
                    facebookUrl = Some(facebookUrl),
                    imdbUrl = Some(imdbUrl),
                    mySpaceUrl = Some(mySpaceUrl),
                    instagramUrl = Some(instagramUrl),
                    homePageUrl = Some(homePageUrl),
                    dateOfBirthStr = Some(dateOfBirthStr),
                    dateOfBirth = Some(dateOfBirth),
                    yearOfBirth = if(yearOfBirth != 0) Some(yearOfBirth) else None,
                    citizenships = Some(citizenships),
                    instruments = Some(instruments),
                    profile= Some(profile))

        }}

        //val jsons  = pathsJson.select($"json").distinct.rdd.map(r => r.getString(0)).collect.toList

        //val artists = jsons.map(j => parse(j).extract[Artist] )

        val root = Artist(  name = year.toString,
                            image = Some("https://palabrassinmordaza.files.wordpress.com/2017/07/jazz2.gif"),
                            discogs_id = None,
                            children = Some(gList),
                            mbLink = None,
                            wikiLink = None,
                            wikipediaUrl = None,
                            allMusicUrl = None,
                            facebookUrl = None,
                            imdbUrl = None,
                            mySpaceUrl = None,
                            instagramUrl = None,
                            homePageUrl = None,
                            dateOfBirthStr = None,
                            dateOfBirth = None,
                            yearOfBirth = None,
                            citizenships= None,
                            instruments = None,
                            profile = None)

        val toFile = write(root)

        writeToFile(toFile, fDirOut, s"tree_graph_${year.toString}")
        
    }
}
