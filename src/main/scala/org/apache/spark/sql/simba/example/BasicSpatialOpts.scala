package org.apache.spark.sql.simba.example

import org.apache.spark.sql.simba.{SparkSession}

/**
  * Created by mtang on 1/17/17.
  */
object BasicSpatialOpts {
  case class PointData(x: Double, y: Double, z: Double, other: String)

  def main(args: Array[String]): Unit = {

    val simbaSession = SparkSession
      .builder()
      .master("local[4]")
      .appName("SparkSessionForSimba")
      .getOrCreate()

      runRangeQuery(simbaSession)

      simbaSession.stop()
  }

  private def runRangeQuery(spark: SparkSession): Unit = {

    import spark.implicits._
    val caseClassDS = Seq(PointData(1.0, 1.0, 3.0, "1"), PointData(2.0, 2.0, 3.0, "2"),PointData(3.0, 3.0, 3.0, "3"),PointData(4.0, 4.0, 3.0, "4")).toDS()

    //caseClassDS.show(3)

    import spark.SimbaImplicits._

    caseClassDS.range(Array("x", "y"), Array(1.0, 1.0), Array(3.0, 3.0)).collect().foreach(println)
  }


}
