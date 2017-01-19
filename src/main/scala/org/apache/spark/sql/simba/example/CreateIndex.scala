package org.apache.spark.sql.simba.example

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.simba.{Dataset, SparkSession}
import org.apache.spark.sql.simba.example.BasicSpatialOpts.PointData
import org.apache.spark.sql.simba.index.RTreeType

/**
  * Created by mtang on 1/17/17.
  */
object CreateIndex {

  def main(args: Array[String]): Unit = {

    val simbaSession = SparkSession
      .builder()
      .master("local[4]")
      .appName("SparkSessionForSimba")
      .getOrCreate()

    buildIndex(simbaSession)
    useIndex(simbaSession)
    simbaSession.stop()
  }

  private def buildIndex(spark: SparkSession): Unit = {

    import spark.implicits._
    val datapoints = Seq(PointData(1.0, 1.0, 3.0, "1"),  PointData(2.0, 2.0, 3.0, "2"), PointData(2.0, 2.0, 3.0, "3"),
      PointData(2.0, 2.0, 3.0, "4"),PointData(3.0, 3.0, 3.0, "5"),PointData(4.0, 4.0, 3.0, "6")).toDF()

    datapoints.createOrReplaceTempView("a")

    spark.indexTable("a", RTreeType, "testqtree",  Array("x", "y") )

    spark.showIndex("a")

  }

  private def useIndex(spark: SparkSession): Unit = {

    import spark.implicits._
    val datapoints = Seq(PointData(1.0, 1.0, 3.0, "1"),  PointData(2.0, 2.0, 3.0, "2"), PointData(2.0, 2.0, 3.0, "3"),
      PointData(2.0, 2.0, 3.0, "4"),PointData(3.0, 3.0, 3.0, "5"),PointData(4.0, 4.0, 3.0, "6")).toDF()

    datapoints.createOrReplaceTempView("b")

    spark.indexTable("b", RTreeType, "RtreeForData",  Array("x", "y") )

    spark.showIndex("b")

    val dataframe = spark.sql("SELECT * FROM b")

    // val pointEncoder = Encoders.product[PointData]

    val dataset = Dataset[PointData](spark, dataframe.logicalPlan)

    dataset.knn(Array("x", "y"),Array(1.0, 1.0),4).show(4)


  }

}
