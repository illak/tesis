import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{ Dataset, DataFrame, Column }
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ Dataset, DataFrame, Row, Column }
import org.apache.spark.sql.functions.udf

import scala.util.{Try, Success, Failure}


object WikidataBuilder {
  
  case class MyClass(wikidata_id: String,
                        discogs_id: String,
                        mb_id: String,
                        allmusic_id: String,
                        songkick_id: String,
                        name: String,
                        genre_id: Seq[String],
                        instrument_id: Seq[String],
                        dob: Seq[(String, String)],
                        bp_id: Seq[String],
                        image: Seq[String],
                        links: Seq[String])
                        
                        
  case class Label(wikidata_id: String, label: String, description: String)
                        
  def main(args: Array[String]) {

    //  Logger.getLogger("info").setLevel(Level.OFF)

    if (args.length != 2) {
      Console.err.println("Need 2 arguments: <\"latest-all.json\" file> <output dir>")
      sys.exit(1)
    }

    /* File names
		 * ***********/
    val f_wikidata = (if (args(0).last != '/') args(0).concat("/") else args(0)) + "latest-all.json"

    val fPqtOut = if (args(1).last != '/') args(1).concat("/") else args(1)
    
    //val fPqtOut1 = args(1)
    //val fPqtOut2 = args(2)
    val fPqtOut1 = fPqtOut + "wikidata.pqt"
    val fPqtOut2 = fPqtOut + "wikidataLabel.pqt"
    
    val spark = SparkSession
      .builder()
      .appName("Wikidata Builder")
//      .config("spark.sql.shuffle.partitions", "24")
      .getOrCreate()
     
    spark.sparkContext.setLogLevel("WARN")
    
    import spark.implicits._

    println("Creating wikidata BD ...")
    
    val wikidataFirst = spark.sparkContext.textFile(f_wikidata, 24)


    val partitions = wikidataFirst.getNumPartitions

    //Removing first and last lines
    val wikidataDF = wikidataFirst.mapPartitionsWithIndex { (idx, iter) =>
      if (idx == 0) iter.drop(1)
      else if (idx == partitions - 1) iter.sliding(2).map(_.head)
      else iter
    }.coalesce(24)

    println("wikidataDF n part = "+ wikidataDF.partitions.size)

    //Eliminamos la coma al final de cada String
    val cleanWikidataDF = wikidataDF.map(x => if(x.takeRight(1) == ",") x.dropRight(1) else x)
    println("cleanWikidataDF n part = "+ cleanWikidataDF.partitions.size)


    // DEBUG
    /*val cl0 = spark.sparkContext.textFile(f_wikidata).repartition(24)
        .map(x => x.replace("\n", "").trim)

    val cl = spark.read.json(cl0)
    cl.printSchema
    cl.limit(3).write.mode(SaveMode.Overwrite).save(fPqtOut1)
    println("WRITED!!")*/
        
    import org.json4s._
    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._
    
    //Parseamos los jsons (obtenemos un RDD de jsons parseados)
    val wikidataParsed = cleanWikidataDF.map(x => parse(x))//.cache()
    println("wikidataParsed n part = "+ wikidataParsed.partitions.size)

    
    def has_mbID(s: org.json4s.JValue) = compact(s \ "claims" \ "P1953").replace("\"", "") != "" 
    def has_discogsID(s: org.json4s.JValue) = compact(s \ "claims" \ "P434").replace("\"", "") != ""
    def has_allMusicID(s: org.json4s.JValue) = compact(s \ "claims" \ "P1728").replace("\"", "") != ""
    def has_songkickID(s: org.json4s.JValue) = compact(s \ "claims" \ "P3478").replace("\"", "") != ""
    
    def has_musicID(s: org.json4s.JValue) = has_mbID(s) || has_discogsID(s) || has_allMusicID(s) || has_songkickID(s)
    
    def has_occupation(s: org.json4s.JValue) = compact(s \ "claims" \ "P106").replace("\"", "") != ""
    def playSomeInstrument(s: org.json4s.JValue) = compact(s \ "claims" \ "P1303").replace("\"", "") != ""
    def isHuman(s: org.json4s.JValue) = compact(s \ "claims" \ "P31" \ "mainsnak" \ "datavalue" \ "value" \ "id").replace("\"", "") == "Q5"
    
    def isMusician(s: org.json4s.JValue) = compact(s \ "descriptions" \ "en" \ "value").replace("\"", "").toLowerCase.contains("musician")
    
    
    //Primero filtramos entidades que son instancia de: Human
    //Luego filtramos las entidades (humanas) que posean algun ID (musical) o que tocan algun instrumento o tiene ocupacion.
    val wikidataFiltered = wikidataParsed
                                .filter(x => isHuman(x))
                                .filter(x => has_musicID(x) || playSomeInstrument(x) || has_occupation(x))

    println("wikidataFiltered n part = " + wikidataFiltered.partitions.size)

    //Obtenemos Imagen
    def getImage(v : org.json4s.JValue): Seq[String] = {
        
        for {
            JObject(image) <- v \ "claims" \ "P18" \ "mainsnak" \ "datavalue"
            JField("value", JString(value))  <- image
        } yield value
        
    }


    def completeImageUrl = udf { s: Seq[String] => s.map(n => "https://commons.wikimedia.org/wiki/File:".concat(n.replace(" ","_"))) }

    //Obtenemos Links (wikipedia)
    def getLinks(v : org.json4s.JValue): Seq[String] = {
        val pattern = """(\w+)wiki"""
    
        val links_list = for {
            JObject(sitelinks) <- v  \ "sitelinks"
            JField("title", JString(title))  <- sitelinks
            JField("site", JString(site))  <- sitelinks
        } yield (site, title.replace(" ", "_"))

        links_list.filter(t => t._1 matches pattern).map(t => "https://"+t._1.split("wiki")(0) + ".wikipedia.org/wiki/"+ t._2)
    }

    def getWikidataID(v : org.json4s.JValue): String = {
        compact(v \ "id").replace("\"", "")
    }

    def getDiscogsID(v : org.json4s.JValue): String = {
        val parsed = compact(v \ "claims" \ "P1953" \ "mainsnak" \ "datavalue" \ "value")
        if (parsed == "") null
        else parsed
                .replace("\"", "").replaceAll("\\[", "").replaceAll("\\]","")
                .split(",")(0) //Nos quedamos con la primer ocurrencia (de haber mas de una) que generalmente es la que esta referenciada
    }

    def getMBID(v : org.json4s.JValue): String = {
        val parsed = compact(v \ "claims" \ "P434" \ "mainsnak" \ "datavalue" \ "value")
        if (parsed == "") null
        else parsed
                .replace("\"", "").replaceAll("\\[", "").replaceAll("\\]","")
                .split(",")(0) //Nos quedamos con la primer ocurrencia (de haber mas de una) que generalmente es la que esta referenciada
    }

    def getSongkickID(v : org.json4s.JValue): String = {
      val parsed = compact(v \ "claims" \ "P3478" \ "mainsnak" \ "datavalue" \ "value")
      if (parsed == "") null
      else parsed
            .replace("\"", "").replaceAll("\\[", "").replaceAll("\\]","")
            .split(",")(0) //Nos quedamos con la primer ocurrencia (de haber mas de una) que generalmente es la que esta referenciada
    }

    def getAllmusicID(v : org.json4s.JValue): String = {
      val parsed = compact(v \ "claims" \ "P1728" \ "mainsnak" \ "datavalue" \ "value")
      if (parsed == "") null
      else parsed
            .replace("\"", "").replaceAll("\\[", "").replaceAll("\\]","")
            .split(",")(0) //Nos quedamos con la primer ocurrencia (de haber mas de una) que generalmente es la que esta referenciada
    }
    
    def getBPID(v : org.json4s.JValue): Seq[String] = {
        
        for {
            JObject(citizenship) <- v \ "claims" \ "P27" \ "mainsnak" \ "datavalue" \ "value"
            JField("id", JString(id))  <- citizenship
        } yield id
    }

    def getLabel(v : org.json4s.JValue): String = {
        compact(v \ "labels" \ "en" \ "value").replace("\"", "")
    }

    def getGenreID(v : org.json4s.JValue): Seq[String] = {
        //val parsed = compact(v \ "claims" \ "P136" \ "mainsnak" \ "datavalue" \ "value" \ "id").replace("\"", "").replaceAll("\\[", "").replaceAll("\\]","");
        //if (parsed == "") null else parsed.split(",")
        
        
        for {
            JObject(genre) <- v \ "claims" \ "P136" \ "mainsnak" \ "datavalue" \ "value" 
            JField("id", JString(id))  <- genre
        } yield id
    }

    def getInstrumentID(v : org.json4s.JValue): Seq[String] = {
        //val parsed = compact(v \ "claims" \ "P1303" \ "mainsnak" \ "datavalue" \ "value" \ "id").replace("\"", "").replaceAll("\\[", "").replaceAll("\\]","");
        //if (parsed == "") null else parsed.split(",")
        
        for {
            JObject(instrument) <- v \ "claims" \ "P1303" \ "mainsnak" \ "datavalue" \ "value"
            JField("id", JString(id))  <- instrument
        } yield id
        
    }
    def getDescription(v: org.json4s.JValue): String ={
        compact(v \ "descriptions" \ "en" \ "value").replace("\"", "").toLowerCase
    }

    def getDateOfBirth(v : org.json4s.JValue): Seq[(String, String)] = {
        
        for {
            JObject(date) <- v \ "claims" \ "P569" \ "mainsnak" \ "datavalue" \ "value"
            JField("time", JString(time))  <- date
            JField("calendarmodel", JString(calendarmodel)) <- date
        } yield (time, calendarmodel.split("/").last)

}
   


    
    //Dataframe de artistas y el codigo de su lugar de nacimiento (ademas del id de wikidata)
    //Pero filtramos los que tengan mas de un dato en citizenship
    val wikidataArtistsDF = wikidataFiltered.map(x => MyClass(getWikidataID(x),
                                                        getDiscogsID(x),
                                                        getMBID(x),
                                                        getAllmusicID(x),
                                                        getSongkickID(x),
                                                        getLabel(x),
                                                        getGenreID(x),
                                                        getInstrumentID(x),
                                                        getDateOfBirth(x),
                                                        getBPID(x),
                                                        getImage(x),
                                                        getLinks(x)
                                                        ))
                                                        .toDF("wikidata_id",
                                                              "discogs_id",
                                                              "musicbrainz_id",
                                                              "allmusic_id",
                                                              "songkick_id",
                                                              "name",
                                                              "genre_ids",
                                                              "instrument_ids",
                                                              "date_of_birth_calendar",
                                                              "citizenship_ids",
                                                              "images",
                                                              "links")
    

    val wikidataArtistsComplete = wikidataArtistsDF.withColumn("images", completeImageUrl($"images"))


    //Dataframe de todos los IDs de wikidata y su label con el que luego se va a hacer un join con los lugares de nacimiento
    val wikidata_ID_LABEL_DF = wikidataParsed.map(x => Label(getWikidataID(x), getLabel(x), getDescription(x)))
                                                .toDF("id", "label", "description")

                                                        
    wikidataArtistsComplete.write.mode(SaveMode.Overwrite).save(fPqtOut1)
    wikidata_ID_LABEL_DF.write.mode(SaveMode.Overwrite).save(fPqtOut2)

  } 
}
