package org.apache.spark.sql.simba.example

import org.apache.spark.sql.simba.{SparkSession}


object BasicSpatialOpts {
  case class PointData(x: Double, y: Double, z: Double, other: String)

  def main(args: Array[String]): Unit = {

    val simbaSession = SparkSession
      .builder()
      .master("local[4]")
      .appName("SparkSessionForSimba")
      .getOrCreate()

      runRangeQuery(simbaSession)
      runKnnQuery(simbaSession)
      runJoinQUery(simbaSession)
      simbaSession.stop()
  }

  private def runKnnQuery(spark: SparkSession): Unit = {

    import spark.implicits._
    val caseClassDS = Seq(PointData(1.0, 1.0, 3.0, "1"),  PointData(2.0, 2.0, 3.0, "2"), PointData(2.0, 2.0, 3.0, "3"),
      PointData(2.0, 2.0, 3.0, "4"),PointData(3.0, 3.0, 3.0, "5"),PointData(4.0, 4.0, 3.0, "6")).toDS()

    import spark.SimbaImplicits._
    caseClassDS.knn(Array("x", "y"),Array(1.0, 1.0),4).show(4)

  }

  private def runRangeQuery(spark: SparkSession): Unit = {

    import spark.implicits._
    val caseClassDS = Seq(PointData(1.0, 1.0, 3.0, "1"),  PointData(2.0, 2.0, 3.0, "2"), PointData(2.0, 2.0, 3.0, "3"),
      PointData(2.0, 2.0, 3.0, "4"),PointData(3.0, 3.0, 3.0, "5"),PointData(4.0, 4.0, 3.0, "6")).toDS()

    import spark.SimbaImplicits._
    caseClassDS.range(Array("x", "y"),Array(1.0, 1.0),Array(2.0, 3.0)).show(10)

  }

  private def runJoinQUery(spark: SparkSession): Unit = {

    import spark.implicits._
    val caseClassDS = Seq(PointData(1.0, 1.0, 3.0, "1"),  PointData(2.0, 2.0, 3.0, "2"), PointData(2.0, 2.0, 3.0, "3"),
      PointData(2.0, 2.0, 3.0, "4"),PointData(3.0, 3.0, 3.0, "5"),PointData(4.0, 4.0, 3.0, "6")).toDS()

    val caseClassDS2 = Seq(PointData(1.0, 1.0, 3.0, "b1"),  PointData(2.0, 2.0, 3.0, "b2"), PointData(2.0, 2.0, 3.0, "b3"),
      PointData(1.0, 1.0, 3.0, "b4"), PointData(2.0, 2.0, 3.0, "b5"),PointData(3.0, 3.0, 3.0, "b6"),
      PointData(4.0, 4.0, 3.0, "b7")).toDF()

    import spark.SimbaImplicits._

    caseClassDS.knnJoin(caseClassDS2, Array("x", "y"),Array("x", "y"),2 ).show(6)

    caseClassDS.distanceJoin(caseClassDS2, Array("x", "y"),Array("x", "y"),2 ).show(6)

  }


}
