package org.apache.spark.sql.simba.execution

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, QueryExecution => SQLQueryExecution}
import org.apache.spark.sql.simba.SparkSession

/**
  * Created by dongx on 11/12/2016.
  */
class QueryExecution(val simbaSession: SparkSession, val simbaLogical: LogicalPlan)
  extends SQLQueryExecution(simbaSession, simbaLogical) {

  lazy val withIndexedData: LogicalPlan = {
    assertAnalyzed()
    simbaSession.indexManager.useIndexedData(withCachedData)
  }

  override lazy val optimizedPlan: LogicalPlan = {
    simbaSession.sessionState.simbaOptimizer.execute(simbaSession.sessionState.getSQLOptimizer.execute(withIndexedData))
  }

  override lazy val sparkPlan: SparkPlan ={

    SparkSession.setActiveSession(simbaSession)
    simbaSession.sessionState.simbaPlanner.plan(optimizedPlan).next()
  }

}
