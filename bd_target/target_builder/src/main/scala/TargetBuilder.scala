import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ Dataset, DataFrame, Row, Column }
import org.apache.spark.sql.expressions.Window
import java.security.MessageDigest


import org.apache.spark.sql.internal.SQLConf.SHUFFLE_PARTITIONS


//import org.apache.log4j.Logger
//import org.apache.log4j.Level

object TargetBuilder {

  def main(args: Array[String]) {

 
    // Faltan  poner todas las tablas
    if (args.length != 2) {
      Console.err.println("Need 2 arguments: <input dir name> <output dir name>")
      sys.exit(1)
    }

    /* File names
		 * ***********/

    val fInputDir = if (args(0).last != '/') args(0).concat("/") else args(0)
    val fOutDir = if (args(1).last != '/') args(1).concat("/") else args(1)

    //val fTmpDir = if (args(1).last != '/') args(1).concat("/") else args(1)
    //val fOutputDir = if (args(2).last != '/') args(2).concat("/") else args(2)

    val fTmpDir = fOutDir + "tmp2/"
    val fOutputDir = fOutDir + "final/"

    //TODO. chequear si existe el directorio (y subdirectorios)
    //discogs:
    val fDGRelArt = fInputDir.concat("discogs/releases_artists.pqt") // discogs releases_artists.pqt
    val fDGArt = fInputDir.concat("discogs/artists.pqt") // discogs artists.pqt

    //musicbrainz:
    val fMBArtists = fInputDir.concat("musicbrainz/artist.pqt")
    val fMBUrl = fInputDir.concat("musicbrainz/url.pqt")
    val fMBArea = fInputDir.concat("musicbrainz/area.pqt")
    val fMBArtistUrl = fInputDir.concat("musicbrainz/artist_url.pqt")
    val fMBAreaArea = fInputDir.concat("musicbrainz/area_area.pqt")

    //wikidata:
    val fWDArtists = fInputDir.concat("wikidata/wikidata.pqt")
    val fWDLabels = fInputDir.concat("wikidata/wikidataLabel.pqt")


    val spark = SparkSession
      .builder()
      .appName("Target Builder")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val sc = spark.sparkContext


    import spark.implicits._

    spark.sessionState.conf.setConf(SHUFFLE_PARTITIONS, sc.defaultParallelism)


    /* Load Files
		 * ************/
    
    // Discogs releases artists table
    val dfDGRelArt1 = spark.read.parquet(fDGRelArt)
    
    // Filter releases with only one artist
    val wRel = Window.partitionBy($"id_release")

    val dfDGRelArt2 = dfDGRelArt1
                .withColumn("cant_arts", count($"*").over(wRel))
                .withColumn("master_id", $"master_id._VALUE")
                .filter($"cant_arts" > 1)
    
    // Filter artists with only one release but with many master version
    // This filter some bands with empty members
                    
    // Add number of releases by artists
    val wArt = Window.partitionBy($"id_artist")
    
    val dfDGRelArt3 = dfDGRelArt2
                    .withColumn("cant_rels", count($"*").over(wArt))
    
    // Add number of releases by master_id (number of versions)
    
    val wMaster = Window.partitionBy($"master_id").orderBy($"id_release")
    
    val dfDGRelArt4 = dfDGRelArt3
                    .withColumn("n", dense_rank().over(wMaster))
    
    val wMaster2 = Window.partitionBy($"master_id").orderBy($"n".desc)
    
    val dfDGRelArt5 = dfDGRelArt4
                    .withColumn("cant_vers", first($"n").over(wMaster2))
                    .drop($"n")

    val dfDGRelArt = dfDGRelArt5
                    .filter($"cant_rels" > 1 || $"cant_vers" === 1)
                    .cache // Used also by output of releases
                
    // Discogs artists table
    val dfDGArt1 = spark.read.parquet(fDGArt)

    // Filter bands and names with strange characters

    val badPattern = "[~!@#$^%&*_+={}\\[\\]|;:\"'<,>.?`/\\\\]"

    val dfDGArt = dfDGArt1
                .filter(size($"members_name") === 0 
                    || (size($"members_name") === 1 && $"members_name".getItem(0) === $"name"))
                .filter(!($"name" rlike badPattern))


    // Join discogs tables to gather more information. Leave only artists in releases
    val dfDGRelJoinArt = dfDGArt.join(dfDGRelArt,Seq("id_artist"),"inner")


    // DEBUG
    dfDGArt.printSchema()
    dfDGRelArt.printSchema()
    dfDGRelJoinArt.printSchema()

    
    val flatten = udf((xs: Seq[Seq[String]]) => xs.flatten.distinct)

    val baseurldg = "https://www.discogs.com/artist/"

    /* Played with relation table
		 * **************************/

    // Symmetric and not reflexive relation
    // Store only one way relation (id artist out < id id artist in

    // Drop compilations
    val isCompilationUDF = udf((a: Seq[Seq[String]]) => a.flatten.exists(s => { val sl = s.toLowerCase
                                                                   sl.contains("compil") || sl.contains("sampler")
                                                                  }))

    // Drop artists various                                                                  
    val hasVariousUDF = udf((a: Seq[Seq[Long]]) => a.flatten.exists(_ == 194))

    // take de minimum id of equals master releases
    val dfDGRelJoinArt1 = dfDGRelJoinArt
                    .select($"id_artist", $"id_release", $"formats", $"primary_artists",  
                            when($"master_id".isNull, - $"id_release").otherwise($"master_id").as("master_id"))
                    .groupBy($"id_artist",$"master_id")
                    .agg(min($"id_release").as("id_release")
                        , collect_list($"formats").as("formatss")
                        , collect_list($"primary_artists").as("primary_artistss"))
                    .filter(! isCompilationUDF($"formatss"))
                    .filter(! hasVariousUDF($"primary_artistss"))
                    .cache()
    
    val dfDGRelJoinArtOut = dfDGRelJoinArt1
        .select($"id_artist".as("id_artist_out"), $"id_release")
    
    val dfDGRelJoinArtIn = dfDGRelJoinArt1
        .select($"id_artist".as("id_artist_in"), $"id_release")
  
    val distinct = udf((xs: Seq[Long]) => xs.distinct)

    val dfPlayedWith = dfDGRelJoinArtOut.join(dfDGRelJoinArtIn, Seq("id_release"))
                                .filter($"id_artist_out" < $"id_artist_in")
                                .groupBy("id_artist_out", "id_artist_in")
                                .agg(distinct(collect_list("id_release")).as("id_releases"))
    
    /* Artist table
		 * *************/

    // Tabla BD Target artists inicial. 
    // Falta refinar instrumentos desde roles
    // Falta agregar información desde wikidata y musicbrainz 
    val dfArtists1 = 
      dfDGRelJoinArt
        .groupBy($"id_artist",$"name", $"namevariations", $"urls", $"profile")
        .agg(
            flatten(collect_list("genres")).as("genres"),
            flatten(collect_list("roles_artist")).as("roles"),
            concat(lit(baseurldg),
                $"id_artist",
                lit("-"), 
                regexp_replace(trim($"name"), "\\s+","-")).as("url_discogs")            
        ).repartition(numPartitions = 2 * sc.defaultParallelism)

    /* Releases Table
     * Is de input release table without artist information 
     * (without the explode of artists in DiscogsBuilder.scala)
     */
    val dfDGReleases = dfDGRelArt.drop("id_artist", "names_artist", "roles_artist", "cant_rels")
                                 .distinct()
        
    //=============================================================================================
    //musicbrainz:
    val mbArtists   = spark.read.parquet(fMBArtists)
    val mbUrl       = spark.read.parquet(fMBUrl)
    val mbArtistUrl = spark.read.parquet(fMBArtistUrl)
    val mbArea      = spark.read.parquet(fMBArea)
    val mbAreaArea  = spark.read.parquet(fMBAreaArea)

    val mbArtistURL = mbArtistUrl.join(mbUrl, Seq("urlID"), "leftouter")
                                   .drop("urlID").groupBy("id").agg(collect_list("url").as("urls"))

    val getDiscogsID = udf((l: Seq[String]) => {
        val i = l.indexWhere(_.contains("https://www.discogs.com/artist/"))
        if(i != -1) l(i).split("/").last else null
    })

    val getAllmusicID = udf((l: Seq[String]) => {
        val i = l.indexWhere(_.contains("http://www.allmusic.com/artist/"))
        if(i != -1) l(i).split("/").last else null
    })

    val getWikidataID = udf((l: Seq[String]) => {
        val i = l.indexWhere(_.contains("https://www.wikidata.org/wiki/"))
        if(i != -1) l(i).split("/").last else null
    })

    val getWikidataURL = udf((l: Seq[String]) => {
        val i = l.indexWhere(_.contains("https://www.wikidata.org/wiki/"))
        if(i != -1) l(i) else null
    })

    val getDiscogsURL = udf((l: Seq[String]) => {
        val i = l.indexWhere(_.contains("https://www.discogs.com/artist/"))
        if(i != -1) l(i) else null
    })

    val getAllmusicURL = udf((l: Seq[String]) => {
        val i = l.indexWhere(_.contains("http://www.allmusic.com/artist/"))
        if(i != -1) l(i) else null
    })

    val getOtherURLS =  udf((l: Seq[String]) => {
        def isOtherURL(url: String) : Boolean = {
            !url.contains("https://www.wikidata.org/wiki/") && !url.contains("https://www.discogs.com/artist/") 
        }

        l.filter(isOtherURL)
    })


    /*val mbArtistsURLS = mbArtistURL.withColumn("discogs_id", getDiscogsID($"urls"))
                                .withColumn("wikidata_id", getWikidataID($"urls"))
                                .withColumn("allmusic_id", getAllmusicID($"urls"))
                                .withColumn("discogs_url", getDiscogsURL($"urls"))
                                .withColumn("wikidata_url", getWikidataURL($"urls"))
                                .withColumn("allmusic_url", getAllmusicURL($"urls"))
                                .drop("urls")*/

    val mbArtistsURLS = mbArtistURL.withColumn("discogs_id", getDiscogsID($"urls"))
                                .withColumn("wikidata_id", getWikidataID($"urls"))
                                .withColumn("discogs_url", getDiscogsURL($"urls"))
                                .withColumn("wikidata_url", getWikidataURL($"urls"))
                                .withColumn("other_urls", getOtherURLS($"urls"))
                                .drop("urls")

    val mbAreaMap = mbArea.select($"areaID",$"area_type").where($"area_type".isNotNull).rdd.map(r => (r.getLong(0), r.getInt(1))).collectAsMap

    val areaJoins = mbAreaArea
                    .join(mbAreaArea.withColumnRenamed("area2ID", "area21ID")
                                    .withColumnRenamed("areaID", "area2ID"), Seq("area2ID"), "leftouter")
                    .join(mbAreaArea.withColumnRenamed("area2ID", "area22ID")
                                    .withColumnRenamed("areaID", "area2ID"), Seq("area2ID"), "leftouter")
                    .join(mbAreaArea.withColumnRenamed("area2ID", "area23ID")
                                    .withColumnRenamed("areaID", "area22ID"), Seq("area22ID"), "leftouter")
                    .join(mbAreaArea.withColumnRenamed("area2ID", "area24ID")
                                    .withColumnRenamed("areaID", "area23ID"), Seq("area23ID"), "leftouter")
                    .join(mbAreaArea.withColumnRenamed("area2ID", "area25ID")
                                    .withColumnRenamed("areaID", "area24ID"), Seq("area24ID"), "leftouter")

    def getType(a: Long) = {
        mbAreaMap.get(a) match{
            case Some(a) => a
            case _       => -1
        }
    }

    val getCountry = udf((a0: Long, a1: Long, a2: Long, a3: Long, a4: Long, a5: Long, a6: Long) => {
        if(getType(a0) == 1){
            a0
        }
        else if(getType(a1) == 1){
            a1
        }
        else if(getType(a2) == 1){
            a2
        }
        else if(getType(a3) == 1){
            a3
        }
        else if(getType(a4) == 1){
            a4
        }
        else if(getType(a5) == 1){
            a5
        }
        else if(getType(a6) == 1){
            a6
        }
        else{
            -1
        }
    })

    val areaJoins2 = areaJoins.select($"areaID",
                                      $"area2ID",
                                      $"area21ID",
                                      $"area22ID",
                                      $"area23ID",
                                      $"area24ID",
                                      $"area25ID").na.fill(0)

    val areaCountry1 = areaJoins2.withColumn("countryID",
                                             getCountry($"areaID",
                                                        $"area2ID",
                                                        $"area21ID",
                                                        $"area22ID",
                                                        $"area23ID",
                                                        $"area24ID",
                                                        $"area25ID")).select($"areaID",$"countryID") 


    val setCountry = udf((id: Long, id2: Long) => {
      if(getType(id2) == 1){
        id2  
      }
      else if(getType(id) == 1){
        id
      }
      else -1
    })

    val areaCountry2 = mbArea.drop("area_type","area_name")
                            .join(areaCountry1, Seq("areaID"), "leftouter")
                            .na.fill(0)
                            .withColumn("countryID", setCountry($"areaID", $"countryID"))
                            .join(mbArea.drop("area_type").withColumnRenamed("areaID", "countryID"), Seq("countryID"), "leftouter")
                            .filter($"countryID" =!= -1).distinct
                            .withColumnRenamed("area_name", "country").select($"areaID", $"country")
    //Artist Table from Musicbraiz
    val dfArtists2 = mbArtists
                        .join(mbArtistsURLS, Seq("id"), "leftouter")
                        .join(areaCountry2, Seq("areaID"), "leftouter").drop("areaID","id")
                        .distinct()
                        .repartition(numPartitions = 2 * sc.defaultParallelism)

    //=============================================================================================
    //wikidata:

    // drop null elements from list
    val dropNull = udf((s: Seq[String]) => s.filter(_ != null))

    def emptyArrStringIfNull(nameCol: String): Column = {
        when(col(nameCol).isNull,array(lit(null).cast("string"))).otherwise(col(nameCol)).as(nameCol)
    }


    def selectColNotNull(nameCol1: String, nameCol2: String, nameCol3: String): Column = {
        when(col(nameCol1).isNotNull, col(nameCol1).as(nameCol1))
            .when(col(nameCol2).isNotNull, col(nameCol2).as(nameCol2))
                .otherwise(col(nameCol3).as(nameCol3))
    }

    val toLong = udf((s: String) => {
        if (s == null) s.asInstanceOf[Long]
        else if(s.startsWith("h")){
            s.split("/").last.split("-").head.toLong
        } else s.toLong
    })

    val wikidataArtists = spark.read.parquet(fWDArtists).withColumn("discogs_id", toLong($"discogs_id"))

	
    println("SIZE")
    println(wikidataArtists.count())

    val wikidataLabel = spark.read.parquet(fWDLabels).select($"id", $"label")

    val wikidataGenres = wikidataArtists.select($"wikidata_id",$"genre_ids")
                                            .withColumn("id", explode($"genre_ids"))
                                            .join(wikidataLabel, Seq("id"), "leftouter")
                                            .groupBy($"wikidata_id").agg(collect_list($"label").as("genres"))

    val wikidataInstruments = wikidataArtists.select($"wikidata_id",$"instrument_ids")
                                            .withColumn("id", explode($"instrument_ids"))
                                            .join(wikidataLabel, Seq("id"), "leftouter")
                                            .groupBy($"wikidata_id").agg(collect_list($"label").as("instruments"))

    val wikidataCitizenships = wikidataArtists.select($"wikidata_id",$"citizenship_ids")
                                            .withColumn("id", explode($"citizenship_ids"))
                                            .join(wikidataLabel, Seq("id"), "leftouter")
                                            .groupBy($"wikidata_id").agg(collect_list($"label").as("citizenships"))

    
    //Artist Table from Wikidata

    val wikidataArtistsComplete = wikidataArtists
                                       .join(wikidataGenres, Seq("wikidata_id"), "leftouter")
                                       .join(wikidataInstruments, Seq("wikidata_id"), "leftouter")
                                       .join(wikidataCitizenships, Seq("wikidata_id"), "leftouter")
                                       .withColumn("genres", when($"genres".isNull, array(lit(null).cast("string"))).otherwise($"genres"))


    val dfArtists3 = wikidataArtistsComplete
                                    .select($"wikidata_id",
                                            $"discogs_id",
                                            $"musicbrainz_id",
                                            $"name",
                                            $"images",
                                            $"links".as("wikidata_links"),
                                            $"genres".as("genres3"),
                                            $"instruments",
                                            $"citizenships",
                                            $"date_of_birth_calendar")
                                    .withColumn("images", dropNull(emptyArrStringIfNull("images")))
                                    .withColumn("wikidata_links", dropNull(emptyArrStringIfNull("wikidata_links")))
                                    .withColumn("genres3", dropNull(emptyArrStringIfNull("genres3")))
                                    .withColumn("instruments", dropNull(emptyArrStringIfNull("instruments")))
                                    .withColumn("citizenships", dropNull(emptyArrStringIfNull("citizenships")))
                                    .distinct()
                                    .repartition(numPartitions = 2 * sc.defaultParallelism).cache



    //=============================================================================================
    println("dfArtists1 num partitions: ")
    println(dfArtists1.rdd.getNumPartitions)
    println("dfArtists2 num partitions: ")
    println(dfArtists2.rdd.getNumPartitions)
    println("dfArtists3 num partitions: ")
    println(dfArtists3.rdd.getNumPartitions)
    //=============================================================================================

    // Función udf que concatena el contenido de 4 columnas
    val concatt4 = udf((s1: Seq[String], s2: Seq[String], s3: Seq[String], s4: Seq[String]) =>{

        (s1 ++ s2 ++ s3 ++ s4).filter(_ != null).distinct

    })

    val concatt3 = udf((s1: Seq[String], s2: Seq[String], s3: Seq[String]) => {
        (s1 ++ s2 ++ s3).filter(_ != null).distinct
    })

    // Función udf que concatena el contenido de 2 columnas
    val concatt2 = udf((s1: Seq[String], s2: Seq[String]) =>{

        (s1 ++ s2).filter(_ != null).distinct

    })



    val lowerCaseMap = udf((s: Seq[String]) => s.map(x => x.toLowerCase).distinct)

    val flatten_udf = udf((s: Seq[Seq[String]]) => s.flatten.distinct)


    //1°) join wikidata x musicbrainz
    val mbTable1 = dfArtists2.select(    
                                        $"discogs_id",
                                        $"wikidata_id",
                                        $"musicbrainz_id",
                                        $"country",
                                        $"wikidata_url",
                                        $"other_urls"
                                    ).groupBy($"musicbrainz_id")
                                    .agg(
                                        first("discogs_id").as("discogs_id1"),
                                        first("wikidata_id").as("wikidata_id1"),
                                        collect_list("country").as("country1"),
                                        collect_list("wikidata_url").as("wikidata_url1"),
                                        flatten_udf(collect_set("other_urls")).as("other_urls1"))

    val mbTable2 = dfArtists2.select(    
                                        $"discogs_id",
                                        $"wikidata_id",
                                        $"musicbrainz_id",
                                        $"country",
                                        $"wikidata_url",
                                        $"other_urls"
                                    ).groupBy($"discogs_id")
                                    .agg(
                                        first("musicbrainz_id").as("musicbrainz_id2"),
                                        first("wikidata_id").as("wikidata_id2"),
                                        collect_list("country").as("country2"),
                                        collect_list("wikidata_url").as("wikidata_url2"),
                                        flatten_udf(collect_set("other_urls")).as("other_urls2"))

    val mbTable3 = dfArtists2.select(    
                                        $"discogs_id",
                                        $"wikidata_id",
                                        $"musicbrainz_id",
                                        $"country",
                                        $"wikidata_url",
                                        $"other_urls"
                                    ).groupBy($"wikidata_id")
                                    .agg(
                                        first("discogs_id").as("discogs_id3"),
                                        first("musicbrainz_id").as("musicbrainz_id3"),
                                        collect_list("country").as("country3"),
                                        collect_list("wikidata_url").as("wikidata_url3"),
                                        flatten_udf(collect_set("other_urls")).as("other_urls3"))



    val mbTableURLS = dfArtists2.select($"discogs_id", $"other_urls")

    val baseurlmb = "https://musicbrainz.org/artist/"





    val array_head_udf = udf((s: Seq[String]) => s.head)
    val array_tail_udf = udf((s: Seq[String]) => s.tail)


    def array_head(nameCol: String): Column = {
        when(col(nameCol).isNotNull && size(col(nameCol)) > 0, array_head_udf(col(nameCol))).otherwise(null)
    }

    def array_tail(nameCol: String): Column = {
        when(col(nameCol).isNotNull && size(col(nameCol)) > 1, array_tail_udf(col(nameCol))).otherwise(null)
    }

    def emptyString2Null(nameCol: String): Column = {
        when(col(nameCol).isNull || length(col(nameCol)) === 0, null).otherwise(col(nameCol))
    }

                            //.withColumn("wikidata_id", concat_ws(",", $"wikidata_id"))
                            //.withColumn("musicbrainz_id", concat_ws(",", $"musicbrainz_id"))


    val dfArtists3_2 = dfArtists3
                            .join(mbTable1, Seq("musicbrainz_id"), "outer")
                            .join(mbTable2, Seq("discogs_id"), "outer")
                            .join(mbTable3, Seq("wikidata_id"), "outer")
                            .withColumn("genres3", emptyArrStringIfNull("genres3"))
                            .withColumn("citizenships", emptyArrStringIfNull("citizenships"))
                            .withColumn("wikidata_links", emptyArrStringIfNull("wikidata_links"))
                            .withColumn("country1", emptyArrStringIfNull("country1"))
                            .withColumn("country2", emptyArrStringIfNull("country2"))
                            .withColumn("country3", emptyArrStringIfNull("country3"))
                            .withColumn("wikidata_url1", emptyArrStringIfNull("wikidata_url1"))
                            .withColumn("wikidata_url2", emptyArrStringIfNull("wikidata_url2"))
                            .withColumn("wikidata_url3", emptyArrStringIfNull("wikidata_url3"))
                            .withColumn("wikidata_id", selectColNotNull("wikidata_id","wikidata_id2", "wikidata_id1"))
                            .withColumn("musicbrainz_id", selectColNotNull("musicbrainz_id","musicbrainz_id2","musicbrainz_id3"))
                            .withColumn("discogs_id", selectColNotNull("discogs_id","discogs_id1","discogs_id3"))
                            .withColumn("wikidata_links", concatt4($"wikidata_url1", $"wikidata_url2", $"wikidata_url3", $"wikidata_links"))
                            .withColumn("citizenships", concatt4($"country1", $"country2", $"country3", $"citizenships"))
                            .withColumn("other_urls1", emptyArrStringIfNull("other_urls1"))
                            .withColumn("other_urls2", emptyArrStringIfNull("other_urls2"))
                            .withColumn("other_urls3", emptyArrStringIfNull("other_urls3"))
                            .withColumn("other_urls", concatt3($"other_urls1", $"other_urls2", $"other_urls3"))
                            .withColumn("musicbrainz_url", when($"musicbrainz_id".isNull, $"musicbrainz_id")
                                .otherwise(concat(lit(baseurlmb), $"musicbrainz_id")))
                            .groupBy($"discogs_id").agg( collect_set($"musicbrainz_id").as("musicbrainz_id"),
                                                         collect_set($"wikidata_id").as("wikidata_id"),
                                                         collect_set($"musicbrainz_url").as("musicbrainz_url"),
                                                         collect_set($"wikidata_links").as("wikidata_links"),
                                                         collect_set($"images").as("images"),
                                                         collect_set($"instruments").as("instruments"),
                                                         collect_set($"citizenships").as("citizenships"),
                                                         collect_set($"genres3").as("genres3"),
                                                         first($"date_of_birth_calendar").as("date_of_birth_calendar"),
                                                         collect_set($"other_urls").as("other_urls")
                            )
                            .withColumn("wikidata_id_others", array_tail("wikidata_id"))
                            .withColumn("wikidata_id", array_head("wikidata_id"))
                            .withColumn("musicbrainz_id_others", array_tail("musicbrainz_id"))
                            .withColumn("musicbrainz_id", array_head("musicbrainz_id"))
                            .withColumn("images", flatten_udf($"images"))
                            .withColumn("instruments", flatten_udf($"instruments"))
                            .withColumn("genres3", flatten_udf($"genres3"))
                            .withColumn("citizenships", flatten_udf($"citizenships"))
                            .withColumn("wikidata_links", flatten_udf($"wikidata_links"))
                            .withColumn("wikidata_id", emptyString2Null("wikidata_id"))
                            .withColumn("musicbrainz_id", emptyString2Null("musicbrainz_id"))
                            .withColumn("other_urls", flatten_udf($"other_urls"))
                            .select($"discogs_id",
                                    $"wikidata_id",
                                    $"wikidata_id_others",
                                    $"musicbrainz_id",
                                    $"musicbrainz_id_others",
                                    $"wikidata_links",
                                    $"musicbrainz_url",
                                    $"other_urls",
                                    $"images",
                                    $"instruments",
                                    $"citizenships",
                                    $"date_of_birth_calendar",
                                    $"genres3")




    // returns wikimedia image url (for html viz)
    val getUrls = udf((s: Seq[String]) => {
        
        def md5(s: String) = {
            val md: MessageDigest = MessageDigest.getInstance("MD5")
            val res = md.digest(s.getBytes()).foldLeft("")(_+"%02x".format(_))
            
            s"/${res(0)}/${res.slice(0,2)}/"
        }

        val names = s.map(_.substring(40))
        
        names.map(n => "https://upload.wikimedia.org/wikipedia/commons" + md5(n) + n)
    })

    //2°) join discogs x dfArtists3_2
    val dfArtists1_2_3 = dfArtists1
                            .withColumnRenamed("id_artist", "discogs_id")
                            .join(dfArtists3_2, Seq("discogs_id"), "leftouter")
                            .withColumn("genres3", emptyArrStringIfNull("genres3"))
                            .withColumn("citizenships", emptyArrStringIfNull("citizenships"))
                            .withColumn("citizenships", dropNull($"citizenships"))                        
                            .withColumn("instruments", emptyArrStringIfNull("instruments"))
                            .withColumn("instruments", dropNull($"instruments"))
                            .withColumn("images", emptyArrStringIfNull("images"))
                            .withColumn("images", dropNull($"images"))
                            .withColumn("wikidata_links", emptyArrStringIfNull("wikidata_links"))
                            .withColumn("wikidata_links", dropNull($"wikidata_links"))
                            .withColumn("genres", concatt2($"genres",$"genres3"))
                            .withColumn("genres", dropNull($"genres"))
                            //.withColumn("image_links", getUrls($"images"))
                            .withColumn("image_links", array(lit(null).cast("string")))
                            .withColumn("image_links", dropNull($"image_links"))
                            .withColumn("wikidata_id_others", dropNull(emptyArrStringIfNull("wikidata_id_others")))
                            .withColumn("musicbrainz_id_others", dropNull(emptyArrStringIfNull("musicbrainz_id_others")))
                            .withColumn("namevariations", dropNull(emptyArrStringIfNull("namevariations")))
                            .withColumn("urls", dropNull(emptyArrStringIfNull("urls")))
                            .withColumn("roles", dropNull(emptyArrStringIfNull("roles")))
                            .withColumn("musicbrainz_url", dropNull(emptyArrStringIfNull("musicbrainz_url")))
                            .select($"discogs_id",
                                    $"wikidata_id",
                                    $"wikidata_id_others",
                                    $"musicbrainz_id",
                                    $"musicbrainz_id_others",
                                    $"name",
                                    $"namevariations",
                                    $"urls",
                                    $"other_urls",
                                    $"roles",
                                    $"url_discogs",
                                    $"wikidata_links",
                                    $"musicbrainz_url",
                                    $"images",
                                    $"image_links",
                                    $"instruments",
                                    $"citizenships",
                                    $"date_of_birth_calendar",
                                    $"genres",
                                    $"profile")
                            .distinct()

    /*println("bd_target table size:")
    println(dfArtists1_2_3.count())

    dfArtists1.printSchema()
    dfArtists2.printSchema()
    dfArtists3.printSchema()*/

    println("writting tmp2 tables...")
    println("mb")
    dfArtists2.write.mode(SaveMode.Overwrite).save(fTmpDir.concat("musicbrainz.pqt"))
    println("dg")
    dfArtists1.write.mode(SaveMode.Overwrite).save(fTmpDir.concat("discogs.pqt"))
    println("wk")
    dfArtists3.write.mode(SaveMode.Overwrite).save(fTmpDir.concat("wikidata.pqt"))

    println("writting artists parquet...")
    dfArtists1_2_3.write.mode(SaveMode.Overwrite).save(fOutputDir.concat("dfArtists.pqt"))
    dfArtists1_2_3.show()
    println("writting artists relations (played with)...")
    dfPlayedWith.write.mode(SaveMode.Overwrite).save(fOutputDir.concat("dfArtistsRel.pqt"))
    dfPlayedWith.show()

    println("writting releases...")
    dfDGReleases.write.mode(SaveMode.Overwrite).save(fOutputDir.concat("dfReleases.pqt"))
    
  }

}
