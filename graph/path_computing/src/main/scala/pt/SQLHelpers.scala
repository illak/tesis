package pt

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions

object SQLHelpers {
  def getExpr(col: Column): Expression = col.expr

  def expr(e: String): Column = functions.expr(e)

  def callUDF(f: Function1[_, _], returnType: DataType, arg1: Column): Column = {
    val u = udf(f, returnType)
    u(arg1)
  }
}
