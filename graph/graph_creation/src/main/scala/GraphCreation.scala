import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{ Dataset, DataFrame, Column }
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.graphframes._
import org.graphframes.lib.{ AggregateMessages => AM }

//import org.apache.log4j.Logger
//import org.apache.log4j.Level

object GraphCreation {

  def main(args: Array[String]) {

    //  Logger.getLogger("info").setLevel(Level.OFF)

    if (args.length != 3) {
      Console.err.println("Need three arguments: <input dir name> <csv relevants> <output dir name>")
      sys.exit(1)
    }

    /* File names
     * ***********/
    val fDirIn = if (args(0).last != '/') args(0).concat("/") else args(0)
    val fDirIn2 = if (args(1).last != '/') args(1).concat("/") else args(1)
    val fDirOut = if (args(2).last != '/') args(2).concat("/") else args(2)

    val fArts = fDirIn + "dfArtists.pqt"
    val fArtsRel = fDirIn + "dfArtistsRel.pqt"
    val fRels = fDirIn + "dfReleases.pqt"

    val fvOut = fDirOut + "vertices.pqt"
    val feOut = fDirOut + "edges.pqt"

    val spark = SparkSession
      .builder()
      .appName("Grapg creation")
      .config("spark.some.config.option", "algun-valor")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    /* Load Files
     * ************/

    val relev = spark.sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(fDirIn2 + "relevants.csv")
      .rdd.map(r => (r.getInt(0), r.getString(1), r.getInt(2))).collect.toList

    // Listas de artistas por categoria
    val category1 = relev.filter(_._3 == 1).map(_._1)
    val category2 = relev.filter(_._3 == 2).map(_._1)

    val dfArts = spark.read.parquet(fArts) //.cache
    val dfArtsRel = spark.read.parquet(fArtsRel) //.cache
    val dfRels = spark.read.parquet(fRels)

    val getTuple = udf((id: Long, title: String) => (id, title))

    // Agregamos información de releases
    val dfArtsRel2 = dfArtsRel.select($"id_artist_out", $"id_artist_in", explode($"id_releases").as("id_release"))
      .join(dfRels.select($"id_release", $"title"), Seq("id_release"), "inner")
      .withColumn("id_title", getTuple($"id_release", $"title"))
      .groupBy($"id_artist_out", $"id_artist_in").agg(collect_set($"id_title").as("id_title_list"))

    // Create Graph
    val g = GraphFrame(dfArts.withColumnRenamed("discogs_id", "id"),
      dfArtsRel2.select('id_artist_out.as("src"), 'id_artist_in.as("dst"), 'id_title_list))
    //                   .cache
    spark.sparkContext.setCheckpointDir("./tmp/")

    println("Calc Connected Components ...")
    val resultCC = g.connectedComponents.run() //.cache

    println("Calc Degrees ...")
    val nodesDegree = g.degrees

    val vDegreeCC = nodesDegree.join(resultCC, "id")

    println("Calc Collective Influence ...")

    val gDegreeCC = GraphFrame(vDegreeCC, g.edges)
    /*
    Comenzamos a calcular CI: descomenta el codigo a continuacion y por cada nodo calcula la sumatoria del degree-1 de sus vecinos.
    Te recomendamos usar la api de AggregateMessages para realizar esta sumatoria.
    El DataFrame sumNeighborDegrees debe tener 2 columnas:
      [id: bigint, sum_neighbor_degree: bigint]
    */
    val msgToSrc = AM.dst("degree") - 1
    val msgToDst = AM.src("degree") - 1
    val sumNeighborDegrees = gDegreeCC.aggregateMessages
      .sendToSrc(msgToSrc) // send destination user's age to source
      .sendToDst(msgToDst) // send source user's age to destination
      .agg(sum(AM.msg).as("sum_neighbor_degree")) // sum up ages, stored in AM.msg column

    /*
    Calculamos CI: para terminar de calcular CI debemos multiplicar la sumatoria calculada en el paso anterior por el degree - 1 de 
    cada nodo.
    Descomenta el codigo a continuacion y joinea sumNeighborDegrees con degrees para tener todas las columnas necesarias para calcular CI.
    El DataFrame collectiveInfluence debe tener 2 columnas: id, ci
    Por ultimo ordena descendentemente por ci 
    */
    val vDegreeCCCI = vDegreeCC.join(sumNeighborDegrees, "id")
      .select('*, (('degree - 1) * 'sum_neighbor_degree).as("ci")).cache //.sort('ci.desc)

    println("Calc Collective Initialized Influence ...")
    
    val alpha = 1 //Peso que se le da a la categorización  manual
    val maxDegree = nodesDegree.agg(max($"degree")).head.getInt(0)//Maximo degree


    // Calcula ci (numero de categorias + 1 - categoria asignada al artista)
    val category = udf((id: Long) => {
      if(category1.contains(id)){
        2
      }else if(category2.contains(id)){
        1
      }else 0
   })

    val relevanceToSrc = AM.dst("degree") - 1 + category(AM.dst("id")) * alpha * maxDegree
    val relevanceToDst = AM.src("degree") - 1 + category(AM.src("id")) * alpha * maxDegree

    val sumRelevance = gDegreeCC.aggregateMessages
      .sendToSrc(relevanceToSrc)
      .sendToDst(relevanceToDst)
      .agg(sum(AM.msg).as("sum_neighbor_relevance"))

    val vDegreeCC_CI_CII = vDegreeCCCI.join(sumRelevance, "id")
      .select($"*" , (($"degree" - 1 + category($"id") * alpha * maxDegree) * $"sum_neighbor_relevance").as("cii")).cache
      
    println("Save Graph ...")
    
    vDegreeCC_CI_CII.write.mode(SaveMode.Overwrite).parquet(fvOut)
    gDegreeCC.edges.write.mode(SaveMode.Overwrite).parquet(feOut)

  }

}
