import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Dataset,DataFrame,Column}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
//import org.apache.log4j.Logger
//import org.apache.log4j.Level

object XmlToParquet {


  def main(args: Array[String]) {

  //  Logger.getLogger("info").setLevel(Level.OFF)

    if (args.length != 3) {
                            Console.err.println("Need three arguments: <tag> <xml file> <parquet file>")
                            sys.exit(1)
                          }

	/* File names
	 * ***********/
    val tag = args(0)
    val fXmlIn = args(1)
    val fPqtOut = args(2)

    val spark = SparkSession
            .builder()
            .appName("xml to Parquet")
            .config("spark.some.config.option", "algun-valor")
                .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    
    import spark.implicits._

	/* Load Files
	 * ************/
	println("Loadding " ++ fXmlIn)
	val dfReleases = spark.read
				.format("com.databricks.spark.xml")
				.option("rowTag", tag)
				.load(fXmlIn)

	println("Writting " ++ fPqtOut)
	
    dfReleases.write.mode(SaveMode.Overwrite).save(fPqtOut)

  }
}
