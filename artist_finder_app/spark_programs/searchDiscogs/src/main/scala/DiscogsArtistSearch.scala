import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ Dataset, DataFrame, Row, Column }
import org.apache.spark.sql.expressions.Window
import java.text.Normalizer

import java.io.File


object DiscogsArtistSearch {

  def main(args: Array[String]) {

    val outputDir = args(0)
    
    val spark = SparkSession
      .builder()
      .appName("Discogs artist finder")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._
  

    //=================================================================================
    // YOU MUST SET THE PARQUET DIRS!!
    // discogs
    val parquet_dir = (if (args(1).last != '/') args(1).concat("/") else args(1)) + "discogs.pqt"
    //=================================================================================    
    
    val stripAccentsUDF = udf((n: String) => {
      Normalizer.normalize(n, Normalizer.Form.NFD)
                    .replaceAll("[^\\p{ASCII}]", "")
    })
    
    val removePunctUDF = udf((n: String) => n.replaceAll("""[\p{Punct}]""", ""))
    
    val dfArt1 = spark.read.parquet(parquet_dir)
                            .withColumn("name", removePunctUDF($"name"))
                            .withColumn("name_lc", stripAccentsUDF(lower($"name")))
    
    val artNames = args.slice(2, args.length).map(_.replaceAll("_", " ").toLowerCase())
    
    
    val nameContains = udf((n1: String, n2: String) => n1.contains(n2))
    
    import org.apache.hadoop.fs._
    
    // delete dir if already exists
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.delete(new Path(outputDir), true)
    
    // search for each artist on the dataframe and create a csv with the results
    for( name <- artNames ){
        var dfArtAux = dfArt1.filter($"name_lc".rlike(name))
                                  .withColumnRenamed("id_artist","discogs_id")
                                  .select($"name",$"url_discogs", $"discogs_id")
                                  .withColumn("search_name", lit(name))
                                  .select($"name",$"url_discogs", $"discogs_id", $"search_name")
                                  
        var tmpParquetDir = outputDir + name.replaceAll("\\s+", "_")

        dfArtAux.repartition(1).write
                    .format("com.databricks.spark.csv")
                    .mode("overwrite")
                    .option("header", "true")
                    .save(tmpParquetDir)
                    
        val file = fs.globStatus(new Path(tmpParquetDir + "/part*"))(0).getPath().getName()
        
        fs.rename(new Path(tmpParquetDir + File.separatorChar + file),
            new Path(outputDir + File.separatorChar + name.replaceAll("\\s+", "_") + ".csv"))
            
        fs.delete(new Path(name.replaceAll("\\s+", "_")+".csv-temp"), true)
        fs.delete(new Path(tmpParquetDir), true)
    }

  }
}