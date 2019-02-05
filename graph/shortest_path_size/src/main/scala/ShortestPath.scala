import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{ Dataset, DataFrame, Column }
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.graphframes._
import org.apache.spark.sql._
import scala.collection.Map

import java.io.File

//import org.apache.log4j.Logger
//import org.apache.log4j.Level

object ShortestPath {

    def main(args: Array[String]) {

        //  Logger.getLogger("info").setLevel(Level.OFF)

        if (args.length != 5) {
          Console.err.println("Need five arguments: <input dir name> <csv guest dir> <csv relevant dir> <num releases> <output dir name>")
          sys.exit(1)
        }

        /* File names
         * ***********/
        val fDirIn1 = if (args(0).last != '/') args(0).concat("/") else args(0)
        val fDirIn2 = if (args(1).last != '/') args(1).concat("/") else args(1)
        val fDirIn3 = if (args(2).last != '/') args(2).concat("/") else args(2)
        val numRel = args(3).toInt
        val fDirOut = if (args(4).last != '/') args(4).concat("/") else args(4)

        val fEdges = fDirIn1 + "edges.pqt"
        val fVertices = fDirIn1 + "vertices.pqt"

        val spark = SparkSession
            .builder()
            .appName("SP size")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()

        val sc = spark.sparkContext

        spark.sparkContext.setLogLevel("WARN")

        import spark.implicits._

        /* Load Files
         * ************/

        val edges = spark.read.parquet(fEdges) //.cache

        // Solo necesitamos "id" y "name", esto reduce tiempo de computo
        val vertices = spark.read.parquet(fVertices).select($"id", $"name") //.cache

        // Chequear valor de cantidad de releases en comun
        val edges2 = edges.filter(size($"id_title_list") > numRel)
            .repartition(numPartitions = 2 * sc.defaultParallelism)
            .select($"src", $"dst")

        val gUndir = GraphFrame( vertices,
                                  edges2.union(edges2.select($"dst".as("src"), $"src".as("dst")))
                                ).cache

        val relev = spark.sqlContext.read
            .format("com.databricks.spark.csv")
            .option("header", "true") // Use first line of all files as header
            .option("inferSchema", "true") // Automatically infer data types
            .load(fDirIn3 + "relevants.csv")
            .rdd.map(r => (r.getInt(0).asInstanceOf[Long], r.getString(1))).collect.toList

        val guests = spark.sqlContext.read
            .format("com.databricks.spark.csv")
            .option("header", "true") // Use first line of all files as header
            .option("inferSchema", "true") // Automatically infer data types
            .load(fDirIn2 + "guests.csv")
            .rdd.map(r => (r.getInt(0).asInstanceOf[Long], r.getString(1))).collect.toList

        // Relevantes
        val toExprs = relev.map(t => (s"id = ${t._1}", t._2.replace("_", " ")))

        // Invitados
        val fromExprs = guests.map(t => (s"id = ${t._1}", t._1))


        val relevants = relev.map(t => (t._2.replace("_", " "), t._1))
        

        val relevantIds = relevants.map(_._2)
        val guestIds = fromExprs.map(_._2)
        val dfIds = spark.sparkContext.parallelize(guestIds).toDF("id")
        val dfRelevants = spark.sparkContext.parallelize(relevants).toDF("name_relevant","id_relevant")
        val res = gUndir.shortestPaths.landmarks(relevantIds).run().coalesce(numPartitions = 2 * sc.defaultParallelism).cache

        val sp_len = res.join(dfIds, "id")
            .select($"id", $"name", explode($"distances"))
            .select($"id", $"name", $"key".as("id_relevant"), $"value".as("distancia"))
            .join(dfRelevants,"id_relevant")
            .select($"id", $"name", struct($"name_relevant", $"distancia").as("distancia"))
            .groupBy($"name".as("artista_invitado"))
            .agg(collect_list($"distancia").as("grado_de_separacion"))
            
        sp_len.write.mode(SaveMode.Overwrite).parquet(fDirOut)
    }
}