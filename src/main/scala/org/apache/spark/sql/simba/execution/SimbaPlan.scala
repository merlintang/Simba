/*
 * Copyright 2016 by Simba Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.spark.sql.simba.execution

import org.apache.spark.sql.simba.SparkSession
import org.apache.spark.sql.execution.SparkPlan

/**
  * Created by dongx on 11/11/16.
  */
abstract class SimbaPlan extends SparkPlan{

  @transient
  protected[simba] final val simbaSesstionState =  SparkSession.getActiveSession.map(_.sessionState).orNull

  protected override def sparkContext = SparkSession.getActiveSession.map(_.sparkContext).orNull

}
