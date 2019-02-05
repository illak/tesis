import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.graphframes._
import org.apache.spark.sql.types.{StructType, StructField, LongType, IntegerType}
import org.apache.spark.sql._
import org.apache.hadoop.fs._
import scala.collection.Map
import org.apache.spark.sql.expressions.Window

import java.io.File

//import org.apache.log4j.Logger
//import org.apache.log4j.Level

object PathRanking {

    def main(args: Array[String]) {

        //  Logger.getLogger("info").setLevel(Level.OFF)

        if (args.length != 4) {
          Console.err.println("Need four arguments: <input dir paths parquet> <csv guests> <csv relevants> <output dir name>")
          sys.exit(1)
        }

        /* File names
         * ***********/
        val fDirIn1 = if (args(0).last != '/') args(0).concat("/") else args(0)
        val fDirIn2 = if (args(1).last != '/') args(1).concat("/") else args(1)
        val fDirIn3 = if (args(2).last != '/') args(2).concat("/") else args(2)
        val fDirOut = if (args(3).last != '/') args(3).concat("/") else args(3)


        val spark = SparkSession
          .builder()
          .appName("graph path ranking")
          .config("spark.some.config.option", "algun-valor")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        import spark.implicits._

        /* Load Files
         * ************/
        var pqtPaths = spark.read.option("mergeSchema", "true").parquet(fDirIn1)
    

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

        val winSpec = Window.partitionBy("from.id")

        //==================================================================================================================

        for((tid, rname) <- relev){

            var pathsDF = pqtPaths.filter($"to.id" === tid)
            val ordered = pathsDF.columns.filter(!_.startsWith("k")).sortBy(rank _)
            val triplas = ordered.sliding(3,2).toSeq.map(l => (l(0), l(1), l(2)))

            val verticesOnly = pathsDF.columns.filter(isVertice)

            // Calculate list of paths for every guest artist
            val pathListMap = guests.map(_._1)
                .map(k => (k.asInstanceOf[Long] -> pathsDF.filter($"from.id" === k)
                    .select(array(verticesOnly.map(n => col(s"${n}.id")):_*))
                    .rdd.map(r => r.getAs[Seq[Any]](0).filter(_ != null).asInstanceOf[Seq[Long]]).collect.toList)).toMap

            val sumCB = udf((s: Seq[Any], id: Long) => {
                val nullFiltered = s.filter(_ != null)
                if(!nullFiltered.isEmpty){
                    nullFiltered.asInstanceOf[Seq[Long]]
                        .map(v => pathListMap(id).map(l => if (l.contains(v)) 1.0 else 0.0).sum).sum
                }else{
                    0.0
                }
            })

            val maxCol = udf((s: Seq[Any]) => {
                val nullFiltered = s.filter(_ != null)
                if(!nullFiltered.isEmpty) nullFiltered.asInstanceOf[Seq[Long]].max else 1
            })

            val sumCI_CII = udf((s: Seq[Any]) => {
                val nullFiltered = s.filter(_ != null)
                if(!nullFiltered.isEmpty) nullFiltered.asInstanceOf[Seq[Long]].sum else 1
            })

            val columns_names_with_v = pathsDF.columns.filter(_.startsWith("v"))

            pathsDF = pathsDF
                .withColumn("path_count", count($"to.id").over(winSpec))
                .withColumn("cb", sumCB(array(columns_names_with_v.map(n => col(s"${n}.id")):_*), $"from.id"))
                .withColumn("sum_CI", sumCI_CII(array(columns_names_with_v.map(n => col(s"${n}.ci")):_*)))
                .withColumn("sum_CII", sumCI_CII(array(columns_names_with_v.map(n => col(s"${n}.cii")):_*)))
                .withColumn("rank_cb", dense_rank().over(winSpec.orderBy(desc("cb"))))
                .withColumn("rank_ci", dense_rank().over(winSpec.orderBy(desc("sum_CI"))))
                .withColumn("rank_cii", dense_rank().over(winSpec.orderBy(desc("sum_CII"))))

            pathsDF.write.mode(SaveMode.Append).parquet(fDirOut + "path_filter.pqt")
        }
    }
}