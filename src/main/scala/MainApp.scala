package net.jgp.books.spark

import net.jgp.books.spark.ch13.lab100_orders.OrderStatistics
import net.jgp.books.spark.ch13.lab300_nyc_school_stats.NewYorkSchoolStatistics
import net.jgp.books.spark.ch13.lab400_udaf.PointsPerOrder

object MainApp {
  def main(args: Array[String]) = {

    val (whichCase, otherArg) = args.length match {
      case 1 => (args(0).toUpperCase, "")
      case 2 => (args(0).toUpperCase, args(1).toUpperCase)
      case _ => ("UDAF", "")
    }

    println(s"=========== whichCase = $whichCase, otherArg = $otherArg ===========")

    whichCase match {
      case "UDAF"     => PointsPerOrder.run()
      case "NYC"      => NewYorkSchoolStatistics.run()
      case _          => OrderStatistics.run(otherArg)
    }
  }
}