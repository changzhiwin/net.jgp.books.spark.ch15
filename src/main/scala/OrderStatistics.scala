package net.jgp.books.spark.ch13.lab100_orders

import org.apache.spark.sql.functions.{ avg, col, sum }
import org.apache.spark.sql.{ SparkSession, DataFrame }

import net.jgp.books.spark.basic.Basic

object OrderStatistics extends Basic {

  def run(mode: String): Unit = {
    val spark = getSession("Orders analytics")

    val df = spark.read.format("csv").
      option("header", true).
      option("inferSchema", true).
      load("data/orders/orders.csv")
    df.printSchema

    mode.toUpperCase match {
      case "API"  => aggApi(df)
      case _      => aggSql(df, spark)
    }
  }

  private def aggApi(df: DataFrame): Unit = {
    val apiDF = df.
      groupBy(col("firstName"), col("lastName"), col("state")).
      agg(
        sum("quantity"),
        sum("revenue"),
        avg("revenue")
      )

    apiDF.show(20, false)
  }

  private def aggSql(df: DataFrame, spark: SparkSession): Unit = {
    df.createOrReplaceTempView("orders")

    val sql = """
      select firstName, lastName, state, 
        SUM(quantity), SUM(revenue), AVG(revenue)
      from orders
      group by firstName, lastName, state
    """

    val sqlDF = spark.sql(sql)
    sqlDF.show(20, false)
  }
}