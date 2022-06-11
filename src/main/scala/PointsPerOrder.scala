package net.jgp.books.spark.ch13.lab400_udaf

import org.apache.spark.sql.{ Encoder, Encoders }
import org.apache.spark.sql.functions.{ udaf, col, sum, call_udf }
import org.apache.spark.sql.expressions.Aggregator

import net.jgp.books.spark.basic.Basic

case class Data(quantity: Int, revenue: Int)

object PointsPerOrder extends Basic {

  def run(): Unit = {
    val spark = getSession("Orders loyalty point")

    // abstract classAggregator[-IN, BUF, OUT] extends Serializable

    // eg: In = Int
    spark.udf.register("pointAttribution", udaf( new Aggregator[Int, Int, Int] {
      def zero: Int = 0
      
      def reduce(b: Int, a: Int): Int = b + (if (a > 3) 3 else a)

      def merge(b1: Int, b2: Int): Int = b1 + b2

      def finish(r: Int): Int = r

      def bufferEncoder: Encoder[Int] = Encoders.scalaInt

      def outputEncoder: Encoder[Int] = Encoders.scalaInt
    }))

    //eg : In = case class(quantity: Int, revenue: Int)
    spark.udf.register("demoAttribution", udaf( new Aggregator[Data, Data, Double] {
      def zero: Data = Data(0, 0)
      
      def reduce(buf: Data, data: Data): Data = Data(buf.quantity + (if (data.quantity > 3) 3 else data.quantity), buf.revenue + data.revenue)

      def merge(b1: Data, b2: Data): Data = Data(b1.quantity + b2.quantity, b1.revenue + b2.revenue)

      def finish(r: Data): Double = r.revenue.toDouble / r.quantity

      def bufferEncoder: Encoder[Data] = Encoders.product

      def outputEncoder: Encoder[Double] = Encoders.scalaDouble
    }))

    val df = spark.read.
      format("csv").
      option("header", true).
      option("inferSchema", true).
      load("data/orders/orders.csv")

    df.printSchema

    val aggDF = df.groupBy(col("firstName"), col("lastName"), col("state")).
      agg(
        sum("quantity"),
        call_udf("pointAttribution", col("quantity")).as("point"),
        call_udf("demoAttribution", col("quantity"), col("revenue")).as("demo")
      )

    aggDF.show(20, false)
  }

}