package net.jgp.books.spark.ch13.lab400_udaf

import org.apache.spark.sql.{ Encoder, Encoders }
import org.apache.spark.sql.functions.{ udaf, col, sum, call_udf }
import org.apache.spark.sql.expressions.Aggregator

import net.jgp.books.spark.basic.Basic

case class Data(i: Int)

object PointsPerOrder extends Basic {

  def run(): Unit = {
    val spark = getSession("Orders loyalty point")

    // abstract classAggregator[-IN, BUF, OUT] extends Serializable
    spark.udf.register("pointAttribution", udaf( new Aggregator[Data, Int, Int] {
      def zero: Int = 0
      
      def reduce(b: Int, a: Data): Int = b + (if (a.i > 3) 3 else a.i)

      def merge(b1: Int, b2: Int): Int = b1 + b2

      def finish(r: Int): Int = r

      def bufferEncoder: Encoder[Int] = Encoders.scalaInt

      def outputEncoder: Encoder[Int] = Encoders.scalaInt
    }))

    val df = spark.read.
      format("csv").
      option("header", true).
      option("inferSchema", true).
      load("data/orders/orders.csv")

    val aggDF = df.groupBy(col("firstName"), col("lastName"), col("state")).
      agg(
        sum("quantity"),
        call_udf("pointAttribution", col("quantity")).as("point")
      )

    aggDF.show(20, false)
  }

}