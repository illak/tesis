import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ Dataset, DataFrame, Row, Column }
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.LongType


import java.io.File


object DbUpdater {

  def main(args: Array[String]) {

    val inputDir = args(0)
    val outputDir = args(1)

    println(args)
    
    val spark = SparkSession
      .builder()
      .appName("db targer updater")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._
  
    // parquet's directory
    val parquet_dir = (if (args(2).last != '/') args(2).concat("/") else args(2)) + "dfArtists.pqt"
    
    //=================================================================================
    // YOU MUST SET THE PARQUET DIRS!!
    // musicbrainz (TMP2)
    val musicbrainz_pqt_dir = (if (args(3).last != '/') args(3).concat("/") else args(3)) + "musicbrainz.pqt"
    // wikidata (TMP2)
    val wikidata_pqt_dir =  (if (args(3).last != '/') args(3).concat("/") else args(3)) + "wikidata.pqt"
    // bdtarget (updated)
    val bd_target_dir  =  (if (args(4).last != '/') args(4).concat("/") else args(4)) + "dfArtists.pqt"
    //=================================================================================
    
    
    //TODO ver de hacer lo siguiente en la etapa de creación de la bd-target
    // para aprovechar la función de spark coalesce debemos pasar los array vacios a null!
    def emptyArray2Null(nameCol: String): Column = {
      
      when(size(col(nameCol)) === 0, null).otherwise(col(nameCol))
      
    }

    def emptyArrStringIfNull(nameCol: String): Column = {
        when(col(nameCol).isNull,array(lit(null).cast("string"))).otherwise(col(nameCol)).as(nameCol)
    }

    def myCoalesce(nameCol1: String, nameCol2: String): Column = {
      when(size(col(nameCol1)) > size(col(nameCol2)), col(nameCol1)).otherwise(col(nameCol2))
    }

    // List of artists
    val artistsIds = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .load(inputDir)
      .select($"_c0".as("discogs_id").cast(LongType)).rdd.map(r => r(0)).collect()



    def emptyArray2NullIfUpdated(nameCol: String): Column = {
      when( col("discogs_id").isin(artistsIds:_*) && size(col(nameCol)) === 0, null).otherwise(col(nameCol))
    }
    
      // drop null elements from list
    val dropNull = udf((s: Seq[String]) => s.filter(_ != null))


    val bdTarget = spark.read.parquet(parquet_dir)
                                  .withColumn("wikidata_links", emptyArray2NullIfUpdated("wikidata_links"))
                                  .withColumn("citizenships", emptyArray2NullIfUpdated("citizenships"))
                                  .withColumn("images", emptyArray2NullIfUpdated("images"))
                                  .withColumn("instruments", emptyArray2NullIfUpdated("instruments"))
                                  .withColumn("date_of_birth_calendar", emptyArray2NullIfUpdated("date_of_birth_calendar"))
                                  .as("bdtarget")
                                  
    
    val artists = spark.read.format("com.databricks.spark.csv")
                      .option("delimiter", "\t")
                      .load(inputDir)
                      .select(  $"_c0".as("discogs_id").cast(LongType),
                                when($"_c1" === "no info", null).otherwise($"_c1").as("musicbrainz_id"),
                                when($"_c2" === "no info", null).otherwise($"_c2").as("wikidata_id"))
                      .as("artists")
 
        
    val musicbrainzTable = spark.read.parquet(musicbrainz_pqt_dir)
    
    val musicbrainzTableUpdate = musicbrainzTable
                                    .join(broadcast(artists.select($"musicbrainz_id")), Seq("musicbrainz_id"), "inner")
                                    .withColumn("country", array($"country"))
                                    .select(  $"musicbrainz_id",
                                              dropNull(emptyArrStringIfNull("country")).as("citizenships"),
                                              emptyArray2NullIfUpdated("other_urls").as("other_urls"))
                                    .as("musicbrainz")

    val wikidataTable = spark.read.parquet(wikidata_pqt_dir)
    
    val wikidataTableUpdate = wikidataTable
                                  .join(broadcast(artists.select($"wikidata_id")), Seq("wikidata_id"), "inner")
                                  .select(  $"wikidata_id",
                                            dropNull(emptyArrStringIfNull("images")).as("images"),
                                            dropNull(emptyArrStringIfNull("wikidata_links")).as("wikidata_links"),
                                            dropNull(emptyArrStringIfNull("instruments")).as("instruments"),
                                            dropNull(emptyArrStringIfNull("citizenships")).as("citizenships"),
                                            $"date_of_birth_calendar"
                                            )
                                  .as("wikidata")

    val updatedDB = bdTarget
                          .join(artists, Seq("discogs_id"), "outer")
                          .select($"discogs_id",
                                    coalesce($"bdtarget.wikidata_id", $"artists.wikidata_id").alias("wikidata_id"),
                                    $"wikidata_id_others",
                                    coalesce($"bdtarget.musicbrainz_id", $"artists.musicbrainz_id").alias("musicbrainz_id"),
                                    $"musicbrainz_id_others",
                                    $"name",
                                    $"namevariations",
                                    $"urls",
                                    $"roles",
                                    $"url_discogs",
                                    $"wikidata_links",
                                    $"musicbrainz_url",
                                    $"images",
                                    $"image_links",
                                    $"other_urls",
                                    $"date_of_birth_calendar",
                                    $"profile",
                                    $"instruments",
                                    $"citizenships",
                                    $"genres").as("updated1")                    
                          .join(wikidataTableUpdate, Seq("wikidata_id"), "outer")
                          .select($"discogs_id",
                                    $"wikidata_id",
                                    $"wikidata_id_others",
                                    $"musicbrainz_id",
                                    $"musicbrainz_id_others",
                                    $"name",
                                    $"namevariations",
                                    $"urls",
                                    $"roles",
                                    $"url_discogs",
                                    $"image_links",
                                    $"other_urls",
                                    myCoalesce("updated1.date_of_birth_calendar", "wikidata.date_of_birth_calendar").alias("date_of_birth_calendar"),
                                    $"profile",
                                    coalesce($"updated1.wikidata_links", $"wikidata.wikidata_links").alias("wikidata_links"),
                                    $"musicbrainz_url",
                                    coalesce($"updated1.images", $"wikidata.images").alias("images"),
                                    coalesce($"updated1.instruments", $"wikidata.instruments").alias("instruments"),
                                    coalesce($"updated1.citizenships", $"wikidata.citizenships").alias("citizenships"),
                                    $"genres").as("updated2")
                           .join(musicbrainzTableUpdate, Seq("musicbrainz_id"), "outer")
                           .select($"discogs_id",
                                    $"wikidata_id",
                                    $"wikidata_id_others",
                                    $"musicbrainz_id",
                                    $"musicbrainz_id_others",
                                    $"name",
                                    $"namevariations",
                                    $"urls",
                                    $"roles",
                                    $"url_discogs",
                                    $"wikidata_links",
                                    $"musicbrainz_url",
                                    $"images",
                                    $"image_links",
                                    $"date_of_birth_calendar",
                                    coalesce($"updated2.other_urls", $"musicbrainz.other_urls").alias("other_urls"),
                                    $"instruments",
                                    coalesce($"updated2.citizenships", $"musicbrainz.citizenships").alias("citizenships"),
                                    $"genres",
                                    $"profile")
                            .withColumn("instruments", dropNull(emptyArrStringIfNull("instruments")))
                            .withColumn("images", dropNull(emptyArrStringIfNull("images")))
                            .withColumn("citizenships", dropNull(emptyArrStringIfNull("citizenships")))
                            .withColumn("wikidata_links", dropNull(emptyArrStringIfNull("wikidata_links")))
                            .withColumn("other_urls", emptyArrStringIfNull("other_urls"))
                            



    def setStatus(id1: String, id2: String): Column = {
      when(col(id1) =!= col(id2), lit("DISTINCT"))
        .when(col(id1) === col(id2), lit("MATCHED"))
        .when(col(id1).isNull && col(id2).isNotNull, lit("ADDED"))
        .when(col(id1).isNull && col(id2).isNull, lit("MATCHED"))
        .otherwise(lit("ERROR"))
      /*when(col(id1) =!= col(id2), lit("DISTINCT"))
      .when(col(id1) === col(id2), lit("MATCHED"))
      .when(col(id1).isNull && col(id2), lit("ADDED"))
      .otherwise(lit("ERROR"))*/
      
    }          
    
    // dataframe for logs
    val dfLog = bdTarget.select($"discogs_id",
                                $"wikidata_id",
                                $"musicbrainz_id")
                                .join(artists.select( $"discogs_id",
                                                      $"musicbrainz_id".as("musicbrainz_id_selected"),
                                                      $"wikidata_id".as("wikidata_id_selected")),
                                Seq("discogs_id"), "inner")
                                .withColumn("mbID_STATUS", setStatus("musicbrainz_id", "musicbrainz_id_selected"))
                                .withColumn("wkID_STATUS", setStatus("wikidata_id", "wikidata_id_selected"))
                                .withColumn("log_date", current_date)
                                                      
    import org.apache.hadoop.fs._
                                              
    
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    //dfLog.write.mode(SaveMode.Overwrite).save("/home/illak/big_data/parquets/testing.pqt")
    dfLog.repartition(1).write
                    .format("com.databricks.spark.csv")
                    .mode("overwrite")
                    .option("header", "true")
                    .save(outputDir + "/log/")
    
    val file = fs.globStatus(new Path(outputDir + "/log/part*"))(0).getPath().getName()
        
    fs.rename(new Path(outputDir + File.separatorChar + "log/" + file),
        new Path(outputDir + File.separatorChar + "log/log.csv"))
                           
    //===================================================================================
    val csv_register = artists.withColumn("search_date", current_date)
    
    // delete dir if already exists
    val fs2 = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    //TODO chequear si es necesario borrar el directorio de salida de csv
    //fs.delete(new Path(outputDir), true)
    
    csv_register.repartition(1).write
                    .format("com.databricks.spark.csv")
                    .mode("overwrite")
                    .option("header", "true")
                    .save(outputDir+ "/ids/")

    val file2 = fs2.globStatus(new Path(outputDir + "/ids/part*"))(0).getPath().getName()
        
    fs2.rename(new Path(outputDir + File.separatorChar + "ids/" +file2),
        new Path(outputDir + File.separatorChar + "ids/artists.csv"))

    //===================================================================================
    // Guardamos la bd_target actualizada
    updatedDB.write.mode(SaveMode.Overwrite).save(bd_target_dir)
  }
}
