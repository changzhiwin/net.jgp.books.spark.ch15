package net.jgp.books.spark.ch13.lab300_nyc_school_stats

import org.apache.spark.sql.functions.{ avg, col, sum, substring, year, floor, lit }
import org.apache.spark.sql.{ SparkSession, DataFrame }
import com.github.mrpowers.spark.daria.sql.transformations.snakeCaseColumns

import net.jgp.books.spark.basic.Basic

object NewYorkSchoolStatistics extends Basic {

  def run(): Unit = {
    val spark = getSession("NYC’s school district")

    val df = processData2012Format(spark, "data/nyc_school_attendance/2012_-_2015_Historical_Daily_Attendance_By_School.csv").
      unionByName( processData2012Format(spark, "data/nyc_school_attendance/200*.csv") ).
      unionByName( processData2015Format(spark) ).
      unionByName( processData2018Format(spark) )

    df.printSchema
    // println(s"---------------------------> Record count = ${df.count}")
    // println(s"---------------------------> School count = ${df.select("school").distinct.count}")

    val enrolledDF = avgEnrolledEachSchool(df)

    val studentCountPerYearDF = evolutionOfStudents(enrolledDF)

    spark.close
  }

  // WHAT IS THE AVERAGE ENROLLMENT FOR EACH SCHOOL?
  private def avgEnrolledEachSchool(df: DataFrame): DataFrame = {
    val enrolledDF = df.groupBy(col("school"), col("schoolyear")).
      agg( avg("enrolled"), avg("present"), avg("absent") ).
      orderBy("schoolyear", "school")

    enrolledDF.printSchema
    enrolledDF.show(10, false)

    enrolledDF
  }

  // WHAT IS THE EVOLUTION OF THE NUMBER OF STUDENTS? 2006 - 2018
  private def evolutionOfStudents(df: DataFrame): DataFrame = {
    val studentCountPerYearDF = df.withColumnRenamed("avg(enrolled)", "enrolled").
      groupBy(col("schoolyear")).
      agg( sum("enrolled").as("enrolled") ).
      withColumn("enrolled", floor("enrolled")).
      orderBy("schoolyear")

    studentCountPerYearDF.printSchema
    studentCountPerYearDF.show(20, false)

    val row  = studentCountPerYearDF.orderBy(col("enrolled").desc).first
    val year = row.getString(0)
    val max  = row.get(1)

    println(s"---------------------------> ${year} was the year with most students, the district served ${max} students.")

    studentCountPerYearDF.withColumn("max", lit(max)).
      withColumn("delta", col("max") - col("enrolled")).
      drop("max").
      orderBy("schoolyear").
      show(20, false)

    studentCountPerYearDF
  }

  // TODO:

  // WHAT IS THE HIGHER ENROLLMENT PER SCHOOL AND YEAR?

  // WHAT IS THE MINIMAL ABSENTEEISM PER SCHOOL?

  // WHICH ARE THE FIVE SCHOOLS WITH THE LEAST AND MOST ABSENTEEISM?

  // schoolId, date, schoolYear, enrolled, present, absent, released
  private def processData2018Format(spark: SparkSession): DataFrame = {
    val schema = "`School DBN` STRING, Date DATE, Enrolled INTEGER, Absent INTEGER, Present INTEGER, Released INTEGER"
    val df = spark.read.
      format("csv").
      schema(schema).
      option("header", "true").
      option("dateFormat", "yyyyMMdd").
      load("data/nyc_school_attendance/2018-2019_Daily_Attendance.csv")

    //df.withColumn("schoolYear", )
    df.withColumnRenamed("School DBN", "School").
      withColumn("SchoolYear", year(col("Date").cast("STRING"))).
      transform(snakeCaseColumns())
  }

  private def processData2012Format(spark: SparkSession, path: String): DataFrame = {
    val schema = "School STRING, Date DATE, SchoolYear STRING, Enrolled INTEGER, Present INTEGER, Absent INTEGER, Released INTEGER"
    val df = spark.read.
      format("csv").
      schema(schema).
      option("header", "true").
      option("dateFormat", "yyyyMMdd").
      load(path)

    df.withColumn("SchoolYear", substring(col("SchoolYear"), 1, 4)).
      transform(snakeCaseColumns())
  }

  private def processData2015Format(spark: SparkSession): DataFrame = {
    val schema = "School STRING, Date DATE, SchoolYear STRING, Enrolled INTEGER, Present INTEGER, Absent INTEGER, Released INTEGER"
    val df = spark.read.
      format("csv").
      schema(schema).
      option("header", "true").
      option("dateFormat", "MM-dd-yyyy").
      load("data/nyc_school_attendance/2015-2018_Historical_Daily_Attendance_By_School.csv")

    df.withColumn("SchoolYear", substring(col("SchoolYear"), 5, 4)).
      transform(snakeCaseColumns())
  }

}