# Purpose
pure scala version of https://github.com/jgperrin/net.jgp.books.spark.ch15

# Environment
- Java 8
- Scala 2.13.8
- Spark 3.2.1

# How to run
## 1, sbt package, in project root dir
When success, there a jar file at ./target/scala-2.13. The name is `main-scala-ch15_2.13-1.0.jar` (the same as name property in sbt file)


## 2, submit jar file, in project root dir
I run this task with two laptop(cluster mode).
```
// common jar, need --jars option
$ YOUR_SPARK_HOME/bin/spark-submit    \
  --class net.jgp.books.spark.MainApp  \
  --master "spark://10.11.1.235:7077"   \
  --jars jars/spark-daria_2.13-1.2.3.jar \
  target/scala-2.13/main-scala-ch15_2.13-1.0.jar
```

## 3, print

### Case: OrderStatistics
```
root
 |-- firstName: string (nullable = true)
 |-- lastName: string (nullable = true)
 |-- state: string (nullable = true)
 |-- quantity: integer (nullable = true)
 |-- revenue: integer (nullable = true)
 |-- timestamp: integer (nullable = true)

+------------+--------+-----+-------------+------------+------------+
|firstName   |lastName|state|sum(quantity)|sum(revenue)|avg(revenue)|
+------------+--------+-----+-------------+------------+------------+
|Ginni       |Rometty |NY   |7            |91          |91.0        |
|Jean-Georges|Perrin  |CA   |4            |75          |75.0        |
|Holden      |Karau   |CA   |10           |190         |95.0        |
|Jean-Georges|Perrin  |NC   |3            |420         |210.0       |
+------------+--------+-----+-------------+------------+------------+
```

### Case: NewYorkSchoolStatistics
```
root
 |-- school: string (nullable = true)
 |-- date: date (nullable = true)
 |-- schoolyear: string (nullable = true)
 |-- enrolled: integer (nullable = true)
 |-- present: integer (nullable = true)
 |-- absent: integer (nullable = true)
 |-- released: integer (nullable = true)

---------------------------> Record count = 3398803
---------------------------> School count = 1865

+------+----------+------------------+------------------+------------------+
|school|schoolyear|avg(enrolled)     |avg(present)      |avg(absent)       |
+------+----------+------------------+------------------+------------------+
|01M015|2006      |248.68279569892474|223.90860215053763|24.774193548387096|
|01M019|2006      |310.4193548387097 |282.86559139784947|27.123655913978496|
|01M020|2006      |654.1397849462365 |610.7795698924731 |42.68279569892473 |
|01M034|2006      |402.8279569892473 |359.60215053763443|42.725806451612904|
|01M056|2006      |2.0               |2.0               |0.0               |
|01M063|2006      |221.8494623655914 |198.36021505376345|22.731182795698924|
|01M064|2006      |287.40860215053766|260.51075268817203|25.978494623655912|
|01M110|2006      |466.48924731182797|433.48387096774195|32.66129032258065 |
|01M134|2006      |347.28494623655916|316.2903225806452 |30.99462365591398 |
|01M137|2006      |259.35483870967744|234.8709677419355 |23.827956989247312|
+------+----------+------------------+------------------+------------------+
only showing top 10 rows

+----------+--------+
|schoolyear|enrolled|
+----------+--------+
|2006      |994597  |
|2007      |978064  |
|2008      |976091  |
|2009      |987968  |
|2010      |990097  |
|2011      |990235  |
|2012      |986900  |
|2013      |985040  |
|2014      |983189  |
|2016      |977576  |
|2017      |971130  |
|2018      |964318  |
+----------+--------+

---------------------------> 2006 was the year with most students, the district served 994597 students.

+----------+--------+-----+
|schoolyear|enrolled|delta|
+----------+--------+-----+
|2006      |994597  |0    |
|2007      |978064  |16533|
|2008      |976091  |18506|
|2009      |987968  |6629 |
|2010      |990097  |4500 |
|2011      |990235  |4362 |
|2012      |986900  |7697 |
|2013      |985040  |9557 |
|2014      |983189  |11408|
|2016      |977576  |17021|
|2017      |971130  |23467|
|2018      |964318  |30279|
+----------+--------+-----+

```

### Case: PointsPerOrder udaf
```
root
 |-- firstName: string (nullable = true)
 |-- lastName: string (nullable = true)
 |-- state: string (nullable = true)
 |-- quantity: integer (nullable = true)
 |-- revenue: integer (nullable = true)
 |-- timestamp: integer (nullable = true)
 
+------------+--------+-----+-------------+-----+------------------+
|firstName   |lastName|state|sum(quantity)|point|demo              |
+------------+--------+-----+-------------+-----+------------------+
|Ginni       |Rometty |NY   |7            |3    |30.333333333333332|
|Jean-Georges|Perrin  |CA   |4            |3    |25.0              |
|Holden      |Karau   |CA   |10           |6    |31.666666666666668|
|Jean-Georges|Perrin  |NC   |3            |3    |140.0             |
+------------+--------+-----+-------------+-----+------------------+
```

## 4, Some diffcult case

### substring function, index start 1
```
.withColumn("SchoolYear", substring(col("SchoolYear"), 1, 4))
```

### what is udaf?
> User-Defined Aggregate Functions (UDAFs) are user-programmable routines that act on multiple rows at once and return a single aggregated value as a result. [here](https://spark.apache.org/docs/3.2.1/sql-ref-functions-udf-aggregate.html) and [here](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/expressions/Aggregator.html)
```
abstract classAggregator[-IN, BUF, OUT] extends Serializable

case class Data(i: Int)

val customSummer =  new Aggregator[Data, Int, Int] {
  def zero: Int = 0
  def reduce(b: Int, a: Data): Int = b + a.i
  def merge(b1: Int, b2: Int): Int = b1 + b2
  def finish(r: Int): Int = r
  def bufferEncoder: Encoder[Int] = Encoders.scalaInt
  def outputEncoder: Encoder[Int] = Encoders.scalaInt
}.toColumn()

val ds: Dataset[Data] = ...
val aggregated = ds.select(customSummer)
```
