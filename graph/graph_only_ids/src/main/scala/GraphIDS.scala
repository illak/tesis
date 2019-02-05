import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{ Dataset, DataFrame, Column }
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.graphframes.lib.{ AggregateMessages => AM }
import org.apache.spark.sql.types.{StructType, StructField, LongType, ArrayType}
import org.apache.spark.sql._
import org.apache.hadoop.fs._
import scala.collection.Map


import java.io.File

//import org.apache.log4j.Logger
//import org.apache.log4j.Level

object GraphIDS {

  def main(args: Array[String]) {

    //  Logger.getLogger("info").setLevel(Level.OFF)

    if (args.length != 4) {
      Console.err.println("Need four arguments: <input dir paths parquet> <rank filter parameter> <year> <output dir name>")
      sys.exit(1)
    }

    /* File names
     * ***********/
    val fDirIn1 = if (args(0).last != '/') args(0).concat("/") else args(0) + "path_filter.pqt"
    val valRank = args(1).toInt
    val year = args(2)
    val fDirOut = if (args(3).last != '/') args(3).concat("/") else args(3)


    val spark = SparkSession
      .builder()
      .appName("graph id extractor")
      .config("spark.some.config.option", "algun-valor")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    /* Load Files
      * ************/
    var pqtPaths = spark.read.option("mergeSchema", "true").parquet(fDirIn1)

   
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

    def isVertice(n: String) = {
      n.startsWith("f") || n.startsWith("v") || n.startsWith("t")
    }

    def isEdge(n: String) = {
      n.startsWith("e")
    }

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

    def allVerticesDF(df: DataFrame, pathsDF: DataFrame, v_names: Seq[String]) : DataFrame = {
      v_names.foldLeft(df)((df, v) => {
          df.union(pathsDF.select(col(s"$v.id")))
      })
    }

    val onlyRelIds = udf((t: Seq[Row]) => {
        t.map{case Row(id:Long, title: String) => id}
    })

    def allEdgesDF(df: DataFrame, pathsDF: DataFrame, e_names: Seq[String]) : DataFrame = {
      e_names.foldLeft(df)((df, e) => {
          df.union(pathsDF.select(when(col(e).isNotNull, explode(onlyRelIds(col(s"$e.id_title_list"))).as("id")).otherwise(null)))
      })
    }

    def oneColEdgesDF(df: DataFrame, pathsDF: DataFrame, e_names: Seq[String]) : DataFrame = {
      e_names.foldLeft(df)((df, e) => {
          df.union(pathsDF.select(when(col(e).isNotNull, onlyRelIds(col(s"$e.id_title_list")).as("ids")).otherwise(null)))
      })
    }

    // Vertices
    val schema = List(StructField("id", LongType, true))

    val v_names = pqtPaths.columns.filter(isVertice).sortBy(rank)

    val paths_vertices_ci = pqtPaths.filter($"rank_ci" <= valRank).select(v_names.map(col):_*)
    val paths_vertices_cii = pqtPaths.filter($"rank_cii" <= valRank).select(v_names.map(col):_*)

    val path_vertices_all = paths_vertices_ci.union(paths_vertices_cii)

    val empty_vertices_DF =  spark.createDataFrame(spark.sparkContext.emptyRDD[Row], StructType(schema))

    val vertices_DF = allVerticesDF(empty_vertices_DF, path_vertices_all, v_names).filter($"id".isNotNull).distinct


    writeJson(vertices_DF, fDirOut, s"vertices_id_list_$year")

    // Edges
    val schema2 = List(StructField("ids", ArrayType(LongType, true), true))

    val e_names = pqtPaths.columns.filter(isEdge).sortBy(rank)

    val paths_edges_ci = pqtPaths.filter($"rank_ci" <= valRank).select(e_names.map(col):_*)
    val paths_edges_cii = pqtPaths.filter($"rank_cii" <= valRank).select(e_names.map(col):_*)

    val path_edges_all = paths_edges_ci.union(paths_edges_cii)

    val empty_edges_DF =  spark.createDataFrame(spark.sparkContext.emptyRDD[Row], StructType(schema2))

    val path_edges_all_one_col = oneColEdgesDF(empty_edges_DF, path_edges_all, e_names).filter($"ids".isNotNull).distinct

    val edges_DF = path_edges_all_one_col.select(explode($"ids").as("id")).distinct



    //val edges_DF = allEdgesDF(empty_edges_DF, path_edges_all, e_names).filter($"id".isNotNull).distinct


    writeJson(edges_DF, fDirOut, s"edges_id_list_$year")


  }
}
