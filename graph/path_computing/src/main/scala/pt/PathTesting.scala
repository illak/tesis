package pt

import org.apache.spark.scheduler._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{ Dataset, DataFrame, Column }
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.graphframes._
import org.apache.spark.sql.types.{StructType, StructField, LongType, ArrayType, DataType, IntegerType, StringType}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.ScalaReflection

import org.apache.spark.sql.internal.SQLConf.SHUFFLE_PARTITIONS

import java.io.File

//import org.apache.log4j.Logger
//import org.apache.log4j.Level

// MODIFY IF NECCESARY AND UPDATE COLUMNS SELECTION ON vertices DATAFRAME
case class Vertice(id: Long, name: String, ci: Long, degree: Int)

object PathTesting {

  def main(args: Array[String]) {

    // Logger.getLogger("info").setLevel(Level.OFF)

    if (args.length != 5) {
      Console.err.println("Need five arguments: <input graph dir name> <csv guests> <csv relevants> <num releases> <output dir name>")
      sys.exit(1)
    }

    /* File names
     * ***********/
    val fDirIn1 = if (args(0).last != '/') args(0).concat("/") else args(0)
    val fDirIn2 = if (args(1).last != '/') args(1).concat("/") else args(1)
    val fDirIn3 = if (args(2).last != '/') args(2).concat("/") else args(2)
    val num_rel = args(3).toInt
    val fDirOut = if (args(4).last != '/') args(4).concat("/") else args(4)

    val fEdges = fDirIn1 + "edges.pqt"
    val fVertices = fDirIn1 + "vertices.pqt"

    val spark = SparkSession
      .builder()
      .appName("BFS Test Optimized")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
 
    import spark.implicits._

    spark.sessionState.conf.setConf(SHUFFLE_PARTITIONS, sc.defaultParallelism)

    /* Load Files
     * ************/
    val edges = spark.read.parquet(fEdges)
      .filter(size($"id_title_list") >= num_rel)

    val bidirEdges = edges.union(edges.select('dst.as("src"), 'src.as("dst"), 'id_title_list))
      .repartition(numPartitions = sc.defaultParallelism).distinct

    // SELECT COLUMNS
    val verticesFromFile = spark.read.parquet(fVertices)

    // Remove vertices that don't appear on filtered edges
    val vFromEdges = edges.select($"src".as("id")).union(edges.select($"dst").as("id")).distinct

    val vertices = verticesFromFile
      .join(vFromEdges, Seq("id"), "inner")
      .repartition(numPartitions = sc.defaultParallelism).cache

    val vertices_id = vertices.select($"id")
    val vertices_struct = vertices.select($"id", struct("*").as("v_struct")).cache


    val gUndir = GraphFrame(
        vertices_id,
        edges = bidirEdges
    ).cache

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

    // Relevants
    val toExprs = relev.map{ case (id, name) => ("id = " + id.toString, name, id) }
    // Guests
    val fromExprs = guests.map{ case (id, name) => ("id = " + id.toString, name, id) }

    // Using my own version of bfs...
    val mybfs = new MyBFS(gUndir)

    val outputDir = fDirOut + "pqt/"

    // Sort Columns
    def rank(c: String): Double = {
        // from < e0 < v1 < e1 < ... < to
        c match {
          case "from" => 0.0
          case "to" => Double.PositiveInfinity
          case _ if c.startsWith("e") => 0.6 + c.substring(1).toInt
          case _ if c.startsWith("v") => 0.3 + c.substring(1).toInt
        }
    }


    // create a Map with path found boolean
    val guestIds = guests.map(_._1)
    var pathFoundMap = guestIds.map(id => (id, false)).toMap

    for ((fe, fname, fid) <- fromExprs; (te, rname, tid) <- toExprs) {

      println(s"=====> RUNNING BFS FROM: $fe TO $te")
      var paths: DataFrame = spark.time(mybfs.fromExpr(fe).toExpr(te).run.cache)

      if (!paths.head(1).isEmpty){

        // set path found to true
        pathFoundMap += (fid -> true)

        for (v <- paths.columns.filter(!_.startsWith("e"))){
            paths = paths
              .withColumn(v, col(v + ".id"))
              .join(vertices_struct.withColumnRenamed("id", v), Seq(v), "inner")
              .drop(v).withColumnRenamed("v_struct", v)
        }

        val key = paths.columns.filter(_.startsWith("v")).length

        val ordered = paths.columns.sortBy(rank _)
        paths = paths.select(ordered.map(col): _*)
        paths.write.mode(SaveMode.Append).parquet(outputDir + "key=" + key.toString)
        paths.unpersist()
      }
      else{
        println(s"*** PATH NOT FOUND: from:${fname.replace("_", " ")}, to:${rname.replace("_", " ")} ***")
      }
    }

    println("printing Map of calculated paths ...")
    println(pathFoundMap)

    // Adding info of guest artists without paths
    val guestsWithoutPath = pathFoundMap.filter(_._2 == false).keys.toList
    val guestsWithoutPathDF = guestsWithoutPath.toDF("id")

    if(guestsWithoutPath.size > 0){
      vertices_struct
        .join(guestsWithoutPathDF, Seq("id"), "inner")
        .drop("id")
        .withColumnRenamed("v_struct", "from")
        .write.mode(SaveMode.Append).parquet(outputDir + "key=-1")
    }
  }
}
