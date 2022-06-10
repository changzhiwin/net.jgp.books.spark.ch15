package net.jgp.books.spark

import net.jgp.books.spark.ch13.lab100_orders.OrderStatistics
import net.jgp.books.spark.ch13.lab300_nyc_school_stats.NewYorkSchoolStatistics

object MainApp {
  def main(args: Array[String]) = {

    val (whichCase, otherArg) = args.length match {
      case 1 => (args(0).toUpperCase, "")
      case 2 => (args(0).toUpperCase, args(1).toUpperCase)
      case _ => ("NYC", "")
    }

    println(s"=========== whichCase = $whichCase, otherArg = $otherArg ===========")

    whichCase match {
      case "NYC"      => NewYorkSchoolStatistics.run()
      case _          => OrderStatistics.run(otherArg)
    }
  }
}