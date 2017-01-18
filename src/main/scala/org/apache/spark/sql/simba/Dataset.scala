package org.apache.spark.sql.simba

import org.apache.spark.sql.simba.execution.QueryExecution
import org.apache.spark.sql.simba.expression.{InCircleRange, InKNN, InRange, PointWrapper}
import org.apache.spark.sql.simba.plans.{DistanceJoin, KNNJoin, SpatialJoin}
import org.apache.spark.sql.simba.spatial.Point
import org.apache.spark.sql.simba.util.LiteralUtil
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.simba.index.IndexType
import org.apache.spark.sql.{simba, Dataset => SQLDataSet, _}
import org.apache.spark.storage.StorageLevel

/**
  * Created by mtang on 1/9/17.
  */


private[simba] object Dataset {

  def apply[T: Encoder](sparkSession: simba.SparkSession, logicalPlan: LogicalPlan): Dataset[T] = {
    new Dataset(sparkSession, logicalPlan, implicitly[Encoder[T]])
  }

  def ofRows(sparkSession: simba.SparkSession, logicalPlan: LogicalPlan): DataFrame = {
    val qe = sparkSession.sessionState.executePlan(logicalPlan)
    qe.assertAnalyzed()
    new Dataset[Row](sparkSession, qe, RowEncoder(qe.analyzed.schema))
  }
}


class Dataset[T] private[simba]
        (@transient val simbaSession: simba.SparkSession,
         @transient override val queryExecution: QueryExecution,
         encoder: Encoder[T])
  extends SQLDataSet[T](simbaSession, queryExecution.logical, encoder)
{

    def this(sparkSession: simba.SparkSession, logicalPlan: LogicalPlan, encoder: Encoder[T]) = {
      this(sparkSession, {
        val qe = sparkSession.sessionState.executePlan(logicalPlan)
        qe
      }, encoder)
  }


  /**
    * Spatial operation, range query.
    * {{{
    *   point.range(Array("x", "y"), Array(10, 10), Array(20, 20))
    *   point.filter($"x" >= 10 && $"x" <= 20 && $"y" >= 10 && $"y" <= 20)
    * }}}
    */
  def range(keys: Array[String], point1: Array[Double], point2: Array[Double]): DataFrame = withPlan {
    val attrs = getAttributes(keys)
    attrs.foreach(attr => assert(attr != null, "column not found"))

    Filter(InRange(PointWrapper(attrs),
      LiteralUtil(new Point(point1)),
      LiteralUtil(new Point(point2))), logicalPlan)
  }

  /**
    * Spatial operation, range query
    * {{{
    *   point.range(p, Array(10, 10), Array(20, 20))
    * }}}
    */
  def range(key: String, point1: Array[Double], point2: Array[Double]): DataFrame = withPlan {
    val attrs = getAttributes(Array(key))
    assert(attrs.head != null, "column not found")

    Filter(InRange(attrs.head,
      LiteralUtil(new Point(point1)),
      LiteralUtil(new Point(point2))), logicalPlan)
  }

  /**
    * Spatial operation knn
    * Find k nearest neighbor of a given point
    */
  def knn(keys: Array[String], point: Array[Double], k: Int): DataFrame = withPlan{
    val attrs = getAttributes(keys)
    attrs.foreach(attr => assert(attr != null, "column not found"))
    Filter(InKNN(PointWrapper(attrs),
      LiteralUtil(new Point(point)), LiteralUtil(k)), logicalPlan)
  }

  def knn(key: String, point: Array[Double], k: Int): DataFrame = withPlan{
    val attrs = getAttributes(Array(key))
    assert(attrs.head != null, "column not found")
    Filter(InKNN(attrs.head,
      LiteralUtil(new Point(point)), LiteralUtil(k)), logicalPlan)
  }

  /**
    * Spatial operation circle range query
    * {{{
    *   point.circleRange(Array("x", "y"), Array(10, 10), 5)
    *   point.filter(($"x" - 10) * ($"x" - 10) + ($"y" - 10) * ($"y" - 10) <= 5 * 5)
    * }}}
    */
  def circleRange(keys: Array[String], point: Array[Double], r: Double): DataFrame = withPlan {
    val attrs = getAttributes(keys)
    attrs.foreach(attr => assert(attr != null, "column not found"))
    Filter(InCircleRange(PointWrapper(attrs),
      LiteralUtil(new Point(point)),
      LiteralUtil(r)), logicalPlan)
  }

  /**
    * Spatial operation circle range query
    * {{{
    *   point.circleRange(p, Point(10, 10), 5)
    * }}}
    */
  def circleRange(key: String, point: Array[Double], r: Double): DataFrame = withPlan {
    val attrs = getAttributes(Array(key))
    assert(attrs.head != null, "column not found")

    Filter(InCircleRange(attrs.head,
      LiteralUtil(new Point(point)),
      LiteralUtil(r)), logicalPlan)
  }

  /**
    * Spatial operation DistanceJoin
    */
  def distanceJoin(right: DataFrame, leftKeys: Array[String],
                   rightKeys: Array[String], r: Double) : DataFrame = withPlan {
    val leftAttrs = getAttributes(leftKeys)
    val rightAttrs = getAttributes(rightKeys, right.queryExecution.analyzed.output)
    SpatialJoin(this.logicalPlan, right.logicalPlan, DistanceJoin,
      Some(InCircleRange(PointWrapper(rightAttrs),
        PointWrapper(leftAttrs),
        LiteralUtil(r))))
  }

  def distanceJoin(right: DataFrame, leftKey: String,
                   rightKey: String, r: Double) : DataFrame = withPlan {
    val leftAttrs = getAttributes(Array(leftKey))
    val rightAttrs = getAttributes(Array(rightKey), right.queryExecution.analyzed.output)
    SpatialJoin(this.logicalPlan, right.logicalPlan, DistanceJoin,
      Some(InCircleRange(rightAttrs.head,
        leftAttrs.head,
        LiteralUtil(r))))
  }

  /**
    * Spatial operation KNNJoin
    */
  def knnJoin(right: DataFrame, leftKeys: Array[String],
              rightKeys: Array[String], k : Int) : DataFrame = withPlan {
    val leftAttrs = getAttributes(leftKeys)
    val rightAttrs = getAttributes(rightKeys, right.queryExecution.analyzed.output)
    SpatialJoin(this.logicalPlan, right.logicalPlan, KNNJoin,
      Some(InKNN(PointWrapper(rightAttrs),
        PointWrapper(leftAttrs), LiteralUtil(k))))
  }

  def knnJoin(right: DataFrame, leftKey: String,
              rightKey: String, k : Int) : DataFrame = withPlan {
    val leftAttrs = getAttributes(Array(leftKey))
    val rightAttrs = getAttributes(Array(rightKey), right.queryExecution.analyzed.output)
    SpatialJoin(this.logicalPlan, right.logicalPlan, KNNJoin,
      Some(InKNN(rightAttrs.head,
        leftAttrs.head, LiteralUtil(k))))
  }

  /////////////////////////////////////////////////////////////////////////////
  // Index operations
  /////////////////////////////////////////////////////////////////////////////
  /**
    * @group extended
    */
  def index(indexType: IndexType, indexName: String, column: Array[String]): this.type = {
    simbaSession.indexManager.createIndexQuery(this.toDF(), indexType, indexName, getAttributes(column).toList)
    this
  }

  /**
    * @group extended
    */
  def setStorageLevel(indexName: String, level: StorageLevel): this.type = {
    simbaSession.indexManager.setStorageLevel(this.toDF(), indexName, level)
    this
  }

  /**
    * @group extended
    */
  def dropIndex(blocking: Boolean): this.type = {
    simbaSession.indexManager.tryDropIndexQuery(this.toDF(), blocking)
    this
  }

  /**
    * @group extended
    */
  def dropIndex(): this.type = dropIndex(blocking = false)

  /**
    * @group extended
    */
  def dropIndexByName(indexName : String) : this.type = {
    simbaSession.indexManager.tryDropIndexByNameQuery(this.toDF(), indexName, blocking = false)
    this
  }

  /**
    * @group extended
    */
  def persistIndex(indexName: String, fileName: String): this.type = {
    simbaSession.indexManager.persistIndex(this.simbaSession, indexName, fileName)
    this
  }

  /**
    * @group extended
    */
  def loadIndex(indexName: String, fileName: String): this.type = {
    simbaSession.indexManager.loadIndex(this.simbaSession, indexName, fileName)
    this
  }




  private def getAttributes(keys: Array[String], attrs: Seq[Attribute] = this.queryExecution.analyzed.output)
  : Array[Attribute] = {
    keys.map(key => {
      val temp = attrs.indexWhere(_.name == key)
      if (temp >= 0) attrs(temp)
      else null
    })
  }

  /** A convenient function to wrap a logical plan and produce a DataFrame. */
  @inline private def withPlan(logicalPlan: => LogicalPlan): DataFrame = {
    Dataset.ofRows(simbaSession, logicalPlan)
  }

}
