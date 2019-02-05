package pt


import pt.SQLHelpers._

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.functions.{udf, array}
import org.apache.spark.sql._
import org.graphframes.GraphFrame
import org.apache.spark.sql.internal.SQLConf.SHUFFLE_PARTITIONS



class MyBFS (private val graph: GraphFrame)
  extends  Serializable {

  private var maxPathLength: Int = 10
  private var edgeFilter: Option[Column] = None
  private var fromExpr: Column = _
  private var toExpr: Column = _

  private def applyExprToCol(expr: Column, colName: String) = {
    new Column(getExpr(expr).transform {
      case UnresolvedAttribute(nameParts) => UnresolvedAttribute(colName +: nameParts)
    })
  }

  def fromExpr(value: Column): this.type = {
    fromExpr = value
    this
  }

  def fromExpr(value: String): this.type = fromExpr(expr(value))

  def toExpr(value: Column): this.type = {
    toExpr = value
    this
  }

  def toExpr(value: String): this.type = toExpr(expr(value))

  def maxPathLength(value: Int): this.type = {
    require(value >= 0, s"BFS maxPathLength must be >= 0, but was set to $value")
    maxPathLength = value
    this
  }

  def edgeFilter(value: Column): this.type = {
    edgeFilter = Some(value)
    this
  }

  def edgeFilter(value: String): this.type = edgeFilter(expr(value))

  def run(): DataFrame = {
    require(fromExpr != null, "fromExpr is required.")
    require(toExpr != null, "toExpr is required.")

    MyBFS.run(graph, fromExpr, toExpr, maxPathLength, edgeFilter)
  }
}


private object MyBFS extends Logging with Serializable {

  private def run(
      g: GraphFrame,
      from: Column,
      to: Column,
      maxPathLength: Int,
      edgeFilter: Option[Column]): DataFrame = {

    /* Agregados */
    val sqlContext = g.vertices.sqlContext
    val sc = sqlContext.sparkContext
    sqlContext.sparkSession.sessionState.conf.setConf(SHUFFLE_PARTITIONS, sc.defaultParallelism)


    def nestAsCol(df: DataFrame, name: String): Column = {
        struct(df.columns.map(c => df(c)) :_*).as(name)
    }
    /* ********************* */

    val fromDF = g.vertices.filter(from)
    val toDF = g.vertices.filter(to)
    
    if (fromDF.take(1).isEmpty || toDF.take(1).isEmpty) {
      // Return empty DataFrame
      return sqlContext.createDataFrame(
        sc.parallelize(Seq.empty[Row]),
        g.vertices.schema)
    }

    val fromEqualsToDF = fromDF.filter(to)
    if (fromEqualsToDF.take(1).nonEmpty) {
      // from == to, so return matching vertices
      return fromEqualsToDF.select(
        nestAsCol(fromEqualsToDF, "from"), nestAsCol(fromEqualsToDF, "to"))
    }

    // We handled edge cases above, so now we do BFS.

    // Edges a->b, to be reused for each iteration
    val a2b: DataFrame = {
      val a2b = g.find("(a)-[e]->(b)")
      edgeFilter match {
        case Some(ef) =>
          val efExpr = applyExprToCol(ef, "e")
          a2b.filter(efExpr) //CACHE
        case None =>
          a2b //CACHE
      }
    }

    sc.broadcast(a2b)

    // We will always apply fromExpr to column "a"
    val fromAExpr = applyExprToCol(from, "a")

    // DataFrame of current search paths
    var paths: DataFrame = null

    var iter = 0
    var foundPath = false

    while (iter < maxPathLength && !foundPath) {
      val nextVertex = s"v${iter + 1}"
      val nextEdge = s"e$iter"
      // Take another step
      if (iter == 0) {
        // Note: We could avoid this special case by initializing paths with just 1 "from" column,
        // but that would create a longer lineage for the result DataFrame.
        paths = a2b.filter(fromAExpr)
          .filter(col("a.id") !== col("b.id"))  // remove self-loops
          .withColumnRenamed("a", "from")
          .withColumnRenamed("e", nextEdge)
          .withColumnRenamed("b", nextVertex)
      } else {
        val prevVertex = s"v$iter"
        val nextLinks = a2b
          .withColumnRenamed("a", prevVertex)
          .withColumnRenamed("e", nextEdge)
          .withColumnRenamed("b", nextVertex)

        paths = paths.join(nextLinks, paths(prevVertex + ".id") === nextLinks(prevVertex + ".id"))
          .drop(paths(prevVertex))
        
        // Make sure we are not backtracking within each path.
        // TODO: Avoid crossing paths; i.e., touch each vertex at most once.
        val previousVertexChecks = Range(1, iter + 1)
          .map(i => paths(s"v$i.id") !== paths(nextVertex + ".id"))
          .foldLeft(paths(s"from.id") !== paths(nextVertex + ".id"))((c1, c2) => c1 && c2)

        paths = paths.filter(previousVertexChecks)
      }
      // Check if done by applying toExpr to column nextVertex
      val toVExpr = applyExprToCol(to, nextVertex)
      val foundPathDF = paths.filter(toVExpr)
      if (foundPathDF.take(1).nonEmpty) {
        // Found path
        paths = foundPathDF.withColumnRenamed(nextVertex, "to")
        foundPath = true
      }
      iter += 1
    }
    if (foundPath) {
      a2b.unpersist()

      logInfo(s"GraphFrame.bfs found path of length $iter.")
      def rank(c: String): Double = {
        // from < e0 < v1 < e1 < ... < to
        c match {
          case "from" => 0.0
          case "to" => Double.PositiveInfinity
          case _ if c.startsWith("e") => 0.6 + c.substring(1).toInt
          case _ if c.startsWith("v") => 0.3 + c.substring(1).toInt
        }
      }
      val ordered = paths.columns.sortBy(rank _)
      paths.select(ordered.map(col): _*)
    } else {
      a2b.unpersist()
      logInfo(s"GraphFrame.bfs failed to find a path of length <= $maxPathLength.")
      // Return empty DataFrame
        sqlContext.createDataFrame(
        sqlContext.sparkContext.parallelize(Seq.empty[Row]),
        g.vertices.schema)
    }
  }


  /**
   * Apply the given SQL expression (such as `id = 3`) to the field in a column,
   * rather than to the column itself.
   *
   * @param expr  SQL expression, such as `id = 3`
   * @param colName  Column name, such as `myVertex`
   * @return  SQL expression applied to the column fields, such as `myVertex.id = 3`
   */
  private def applyExprToCol(expr: Column, colName: String) = {
    new Column(getExpr(expr).transform {
      case UnresolvedAttribute(nameParts) => UnresolvedAttribute(colName +: nameParts)
    })
  }
}
