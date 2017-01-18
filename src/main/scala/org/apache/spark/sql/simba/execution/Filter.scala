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
 */

package org.apache.spark.sql.simba.execution

import org.apache.spark.sql.simba.expression._
import org.apache.spark.sql.simba.spatial.Point
import org.apache.spark.sql.simba.util.ShapeUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, ExpressionCanonicalizer}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, BindReferences, Expression, IsNotNull, Literal, NullIntolerant, PredicateHelper, SortOrder, And => SQLAnd, Not => SQLNot, Or => SQLOr}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.{CodegenSupport, SparkPlan}

case class Filter(condition: Expression, child: SparkPlan)
  extends SimbaPlan  with CodegenSupport with PredicateHelper {

  // Split out all the IsNotNulls from condition.
  private val (notNullPreds, otherPreds) = splitConjunctivePredicates(condition).partition {
    case IsNotNull(a) => isNullIntolerant(a) && a.references.subsetOf(child.outputSet)
    case _ => false
  }

  // If one expression and its children are null intolerant, it is null intolerant.
  private def isNullIntolerant(expr: Expression): Boolean = expr match {
    case e: NullIntolerant => e.children.forall(isNullIntolerant)
    case _ => false
  }

  // The columns that will filtered out by `IsNotNull` could be considered as not nullable.
  private val notNullAttributes = notNullPreds.flatMap(_.references).distinct.map(_.exprId)

  // Mark this as empty. We'll evaluate the input during doConsume(). We don't want to evaluate
  // all the variables at the beginning to take advantage of short circuiting.
  override def usedInputs: AttributeSet = AttributeSet.empty

  override def output: Seq[Attribute] = {
    child.output.map { a =>
      if (a.nullable && notNullAttributes.contains(a.exprId)) {
        a.withNullability(false)
      } else {
        a
      }
    }
  }

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val numOutput = metricTerm(ctx, "numOutputRows")

    /**
      * Generates code for `c`, using `in` for input attributes and `attrs` for nullability.
      */
    def genPredicate(c: Expression, in: Seq[ExprCode], attrs: Seq[Attribute]): String = {
      val bound = BindReferences.bindReference(c, attrs)
      val evaluated = evaluateRequiredVariables(child.output, in, c.references)

      // Generate the code for the predicate.
      val ev = ExpressionCanonicalizer.execute(bound).genCode(ctx)
      val nullCheck = if (bound.nullable) {
        s"${ev.isNull} || "
      } else {
        s""
      }

      s"""
         |$evaluated
         |${ev.code}
         |if (${nullCheck}!${ev.value}) continue;
       """.stripMargin
    }

    ctx.currentVars = input

    // To generate the predicates we will follow this algorithm.
    // For each predicate that is not IsNotNull, we will generate them one by one loading attributes
    // as necessary. For each of both attributes, if there is an IsNotNull predicate we will
    // generate that check *before* the predicate. After all of these predicates, we will generate
    // the remaining IsNotNull checks that were not part of other predicates.
    // This has the property of not doing redundant IsNotNull checks and taking better advantage of
    // short-circuiting, not loading attributes until they are needed.
    // This is very perf sensitive.
    // TODO: revisit this. We can consider reordering predicates as well.
    val generatedIsNotNullChecks = new Array[Boolean](notNullPreds.length)
    val generated = otherPreds.map { c =>
      val nullChecks = c.references.map { r =>
        val idx = notNullPreds.indexWhere { n => n.asInstanceOf[IsNotNull].child.semanticEquals(r)}
        if (idx != -1 && !generatedIsNotNullChecks(idx)) {
          generatedIsNotNullChecks(idx) = true
          // Use the child's output. The nullability is what the child produced.
          genPredicate(notNullPreds(idx), input, child.output)
        } else {
          ""
        }
      }.mkString("\n").trim

      // Here we use *this* operator's output with this output's nullability since we already
      // enforced them with the IsNotNull checks above.
      s"""
         |$nullChecks
         |${genPredicate(c, input, output)}
       """.stripMargin.trim
    }.mkString("\n")

    val nullChecks = notNullPreds.zipWithIndex.map { case (c, idx) =>
      if (!generatedIsNotNullChecks(idx)) {
        genPredicate(c, input, child.output)
      } else {
        ""
      }
    }.mkString("\n")

    // Reset the isNull to false for the not-null columns, then the followed operators could
    // generate better code (remove dead branches).
    val resultVars = input.zipWithIndex.map { case (ev, i) =>
      if (notNullAttributes.contains(child.output(i).exprId)) {
        ev.isNull = "false"
      }
      ev
    }

    s"""
       |$generated
       |$nullChecks
       |$numOutput.add(1);
       |${consume(ctx, resultVars)}
     """.stripMargin
  }


  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning


  private class DistanceOrdering(point: Expression, target: Point) extends Ordering[InternalRow] {
    override def compare(x: InternalRow, y: InternalRow): Int = {
      val shape_x = ShapeUtils.getShape(point, child.output, x)
      val shape_y = ShapeUtils.getShape(point, child.output, y)
      val dis_x = target.minDist(shape_x)
      val dis_y = target.minDist(shape_y)
      dis_x.compare(dis_y)
    }
  }

  // TODO change target partition from 1 to some good value
  // Note that target here must be an point literal in WHERE clause,
  // hence we can consider it as Point safely
  def knn(rdd: RDD[InternalRow], point: Expression, target: Point, k: Int): RDD[InternalRow] =
    sparkContext.parallelize(rdd.map(_.copy()).takeOrdered(k)(new DistanceOrdering(point, target)), 1)

  def applyCondition(rdd: RDD[InternalRow], condition: Expression): RDD[InternalRow] = {
    condition match {
      case InKNN(point, target, k) =>
        val _target = target.asInstanceOf[Literal].value.asInstanceOf[Point]
        knn(rdd, point, _target, k.value.asInstanceOf[Number].intValue())
      case now@And(left, right) =>
        if (!now.hasKNN)
          {
            val predicate = newPredicate(condition, child.output)
            rdd.mapPartitions{ iter => iter.filter( row => predicate.eval(row))}
          }
        else applyCondition(rdd, left).map(_.copy()).intersection(applyCondition(rdd, right).map(_.copy()))
      case now@Or(left, right) =>
        if (!now.hasKNN)
          {
            val predicate =newPredicate(condition, child.output)
            rdd.mapPartitions{ iter => iter.filter(row => predicate.eval(row))}
          }
        else applyCondition(rdd, left).map(_.copy()).union(applyCondition(rdd, right).map(_.copy())).distinct()
      case now@Not(c) =>
        if (!now.hasKNN)
          {
            val predicate =newPredicate(condition, child.output)
            rdd.mapPartitions{ iter => iter.filter(row => predicate.eval(row))}
          }
        else rdd.map(_.copy()).subtract(applyCondition(rdd, c).map(_.copy()))
      case _ =>
        {
          @transient val predicate =newPredicate(condition, child.output)
          rdd.mapPartitions{ iter => iter.filter(row => predicate.eval(row))}
        }
    }
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val root_rdd = child.execute()
    condition transformUp {
      case SQLAnd(left, right) => And(left, right)
      case SQLOr(left, right)=> Or(left, right)
      case SQLNot(c) => Not(c)
    }
    applyCondition(root_rdd, condition)
  }

  override def children: Seq[SparkPlan] = child :: Nil

}
