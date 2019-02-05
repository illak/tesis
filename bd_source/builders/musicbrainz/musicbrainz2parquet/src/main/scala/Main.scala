import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{ Dataset, DataFrame, Column }
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ Dataset, DataFrame, Row, Column }
import org.apache.spark.sql.functions.udf



object Main {

  def main(args: Array[String]) {

    //  Logger.getLogger("info").setLevel(Level.OFF)

    if (args.length != 2) {
      Console.err.println("Need 2 arguments: <input dir name> <output dir name>")
      sys.exit(1)
    }

    /* File names */

    val fInputDir = if (args(0).last != '/') args(0).concat("/") else args(0)

    val f_artist = fInputDir.concat("artist")
    val f_area = fInputDir.concat("area")
    val f_url = fInputDir.concat("url")
    val f_l_artist_url = fInputDir.concat("l_artist_url")
    val f_l_area_area = fInputDir.concat("l_area_area")

    val fOutputDir = if (args(1).last != '/') args(1).concat("/") else args(1)
      
    if (! new java.io.File(fOutputDir).exists){
      Console.err.println("Error, output dir doesn't exist")
      sys.exit(1)
    }
    
    val spark = SparkSession
      .builder()
      .appName("Musicbrainz Builder")
      .getOrCreate()
     
    spark.sparkContext.setLogLevel("WARN")
    
    import spark.implicits._
    
    println("Creating musicbrainz BD ...")
    
    val string2null = udf((s: String) => if (s == "\\N") null else s)


    import org.apache.spark.sql.types._
    
    
    //Tabla de artistas
    val mbArtistParsed = spark.sqlContext.read.format("com.databricks.spark.csv")
                          .option("delimiter", "\t")
                          .load(f_artist)
                          .filter($"_c10" === "1")
                          .select($"_c0".cast(LongType).as("id"),
                                  $"_c1".as("musicbrainz_id"),
                                  $"_c2".as("artist_name"),
                                  $"_c11".cast(LongType).as("areaID"))
    
    //Tabla relacion area_area
    val mbArea_Area_parsed = spark.sqlContext.read.format("com.databricks.spark.csv")
                            .option("delimiter", "\t")
                            .load(f_l_area_area)
                            .select($"_c3".cast(LongType).as("areaID"),
                                    $"_c2".cast(LongType).as("area2ID"))
    
    //Tabla de areas
    val mbAreaParsed = spark.sqlContext.read.format("com.databricks.spark.csv")
                            .option("delimiter", "\t")
                            .load(f_area)
                            .select($"_c0".cast(LongType).as("areaID"),
                                    $"_c2".as("area_name"),
                                    $"_c3".cast(IntegerType).as("area_type"))
    
/*    val urlType = udf((s: String) => {
        if (s.contains("https://www.discogs.com/artist")) "discogs"
        else if (s.contains("http://www.allmusic.com/artist/")) "allmusic"
        else if (s.contains("https://www.songkick.com/artists/")) "songkick"
        else "other"
    })
    
    val urlParseID = udf((s: String, u: String) => {
        if (s == "discogs") u.split("/").last
        else if (s == "allmusic") u.split("/").last
        else if (s == "songkick") u.split("/").last
        else "none"
    })*/
    
    //Tabla de urls
    val mbUrlParsed = spark.sqlContext.read.format("com.databricks.spark.csv")
                            .option("delimiter", "\t")
                            .load(f_url)
                            .select($"_c0".cast(LongType).as("urlID"),
                                    $"_c2".as("url"))
    
    //Tabla de relacion artista_url                                    
    val mbArtist_URL_parsed = spark.sqlContext.read.format("com.databricks.spark.csv")
                                .option("delimiter", "\t")
                                .load(f_l_artist_url)
                                .select($"_c2".cast(LongType).as("id"),
                                        $"_c3".cast(LongType).as("urlID"))
    
    mbArtistParsed.write.mode(SaveMode.Overwrite).save(fOutputDir+"artist.pqt")
    mbArea_Area_parsed.write.mode(SaveMode.Overwrite).save(fOutputDir+"area_area.pqt")
    mbAreaParsed.write.mode(SaveMode.Overwrite).save(fOutputDir+"area.pqt")
    mbUrlParsed.write.mode(SaveMode.Overwrite).save(fOutputDir+"url.pqt")
    mbArtist_URL_parsed.write.mode(SaveMode.Overwrite).save(fOutputDir+"artist_url.pqt")
                                        
  } 
}
