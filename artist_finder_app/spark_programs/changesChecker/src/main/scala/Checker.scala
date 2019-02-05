import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ Dataset, DataFrame, Row, Column }
import org.apache.spark.sql.expressions.Window

import java.io.File


object Checker {

  def main(args: Array[String]) {

    val inputDir = args(0)
    val outputDir = (if (args(2).last != '/') args(2).concat("/") else args(2))
    //val idsFileDir = args(2)

    println(args)
    
    val spark = SparkSession
      .builder()
      .appName("db targer updater")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._
  
    // parquet's directory
    val parquet_dir = (if (args(1).last != '/') args(1).concat("/") else args(1)) + "dfArtists.pqt"

    // ids to check
    val artists = spark.read.format("com.databricks.spark.csv")
                      .option("delimiter", "\t")
                      .load(inputDir)
                      .select(  $"_c0".as("discogs_id"),
                                when($"_c1" === "no info", null).otherwise($"_c1").as("musicbrainz_id"),
                                when($"_c2" === "no info", null).otherwise($"_c2").as("wikidata_id"))
                      .as("artists")

  
    val discogsIDs = artists.select($"artists.discogs_id").rdd.map(r => r(0)).collect()


    val targetDBArtists = spark.read.parquet(parquet_dir)
      .select($"name", $"discogs_id", $"musicbrainz_id", $"wikidata_id")
      .filter($"discogs_id".isin(discogsIDs:_*))

    val checkerDF = targetDBArtists.join(artists.select(  $"discogs_id",
                                                          $"musicbrainz_id".as("musicbrainz_id_selected"),
                                                          $"wikidata_id".as("wikidata_id_selected")),
                                                          Seq("discogs_id"), "inner")

    def setStatus(id1: String, id2: String): Column = {
      
      when(col(id1) =!= col(id2), lit(true))
        .when(col(id1) === col(id2), lit(false))
        .when(col(id1).isNull && col(id2).isNotNull, lit(true))
        .when(col(id1).isNull && col(id2).isNull, lit(false))
        .otherwise(lit(false))
      
    }


    import org.apache.hadoop.fs._
    
    // delete dir if already exists
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.delete(new Path(outputDir), true)

    val statuses = checkerDF
      .withColumn("mb_status", setStatus("musicbrainz_id", "musicbrainz_id_selected"))
      .withColumn("wiki_status", setStatus("wikidata_id", "wikidata_id_selected"))
      .withColumn("update_status", $"mb_status" || $"wiki_status")
      .select($"name", $"discogs_id", $"musicbrainz_id", $"wikidata_id", $"musicbrainz_id_selected", $"wikidata_id_selected", $"update_status")

    statuses.repartition(1).write
      .format("com.databricks.spark.csv")
      .mode("overwrite")
      .option("header", "true")
      .save(outputDir)


    val file = fs.globStatus(new Path(outputDir + "/part*"))(0).getPath().getName()
    
    fs.rename(new Path(outputDir + File.separatorChar + file),
        new Path(outputDir + File.separatorChar  + "update_status.csv"))
        
    //fs.delete(new Path(name.replaceAll("\\s+", "_")+".csv-temp"), true)
    //fs.delete(new Path(tmpParquetDir), true)
  }
}
