//Se carga con 
//./bin/spark-shell --driver-memory 100g --conf spark.default.parallelism=40 --packages graphframes:graphframes:0.5.0-spark2.1-s_2.11

import org.graphframes._

val basedir = "/home/damian/space/illak/files"
val basedirG = basedir + "/db/transformed/graph/degree_CC_CI_not_compil"
//val basedirG = basedir + "/db/transformed/graph/degree_CC_CI_masters"
val fvOut = basedirG + "/vertices.pqt"
val feOut = basedirG + "/edges.pqt"

val vDegreeCCCI = spark.read.parquet(fvOut)
val eDegreeCCCI = spark.read.parquet(feOut)


val gUndir = GraphFrame(vDegreeCCCI,
                   eDegreeCCCI.union(eDegreeCCCI.select('dst.as("src"), 'src.as("dst"), 'id_releases)))
//              .cache

val fromExpr = "id = 255129"
val paths = gUndir.bfs.fromExpr(fromExpr).toExpr("id = 23755").run().cache()

