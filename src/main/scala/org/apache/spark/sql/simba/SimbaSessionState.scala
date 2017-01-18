package org.apache.spark.sql.simba

import org.apache.spark.sql.simba.execution.SimbaPlanner
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, FileSourceStrategy}
import org.apache.spark.sql.{execution => sparkexecution, _}
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.simba.plans.{SimbaOptimizer}

import scala.collection.immutable



/**
  * A class that holds all session-specific state in a given [[SparkSession]] backed by Simba.
  */
private[simba] class SimbaSessionState
(sparkSession: SparkSession,
 @transient protected[simba] val indexManager: IndexManager
 )
  extends SessionState(sparkSession) {

  self =>

  protected[simba] lazy val simbaConf = new SimbaConf

  protected[simba] def getSQLOptimizer = optimizer


  protected[simba] lazy val simbaOptimizer: SimbaOptimizer = new SimbaOptimizer

 /**
   * Planner that takes into account spatial opt strategies.
   */
  protected[simba] val simbaPlanner: sparkexecution.SparkPlanner = {
   new SimbaPlanner(sparkSession, conf, experimentalMethods.extraStrategies)

    /**
      * todo: move the logoical plan opt to here from the simbaPlanner
       */
    /*
     with SimbaStrategies {
     val sparkSession: SparkSession = self.sparkSession

     override def strategies: Seq[Strategy] = {
      experimentalMethods.extraStrategies ++ Seq(
        IndexRelationScans,
        SpatialJoinExtractor
       )
     }
    }*/
  }

  override def executePlan(plan: LogicalPlan) =
    new execution.QueryExecution(sparkSession, plan)


  def setConf(key: String, value: String): Unit = {
    if (key.startsWith("simba.")) simbaConf.setConfString(key, value)
    else conf.setConfString(key, value)
  }

   def getConf(key: String): String = {
    if (key.startsWith("simba.")) simbaConf.getConfString(key)
    else conf.getConfString(key)
  }

   def getConf(key: String, defaultValue: String): String = {
    if (key.startsWith("simba.")) conf.getConfString(key, defaultValue)
    else conf.getConfString(key, defaultValue)
  }

   def getAllConfs: immutable.Map[String, String] = {
    conf.getAllConfs ++ simbaConf.getAllConfs
  }

}
