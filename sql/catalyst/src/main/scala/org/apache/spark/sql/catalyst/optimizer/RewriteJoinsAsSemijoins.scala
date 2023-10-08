/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.optimizer

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.{Inner, InnerLike, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern
import org.apache.spark.sql.types.IntegerType

object RewriteJoinsAsSemijoins extends Rule[LogicalPlan] with PredicateHelper {
  def rewritePlan(agg: Aggregate, groupingExpressions: Seq[Expression],
                  aggExpressions: Seq[NamedExpression], projectList: Seq[NamedExpression],
                  join: Join, keyRefs: Seq[Seq[Expression]],
                  uniqueConstraints: Seq[Seq[Expression]]) : LogicalPlan = {
    logWarning("applying yannakakis rewriting to join: " + agg)
    // TODO allow different joins
    // logWarning("join type: " + joinType)
    val (items, conditions) = extractInnerJoins(join)
    logWarning("agg(project(join))")
    logWarning("items: " + items.toString())
    logWarning("conditions: " + conditions)
    for (agg <- aggExpressions) {
      logWarning("agg: " + agg)
      logWarning("is 0MA: " + is0MA(agg))
      logWarning("is counting: " + isCounting(agg))
      logWarning("is percentile: " + isPercentile(agg))
      logWarning("is sum: " + isSum(agg))
      logWarning("is avg: " + isAverage(agg))
      logWarning("is non-agg: " + isNonAgg(agg))
    }

    val aggregateAttributes = aggExpressions.map(expr => expr.references)
      .reduce((a1, a2) => a1 ++ a2)
    logWarning("aggregate attributes: " + aggregateAttributes)
    logWarning("groupingExpressions: " + groupingExpressions)
    val groupAttributes = if (groupingExpressions.isEmpty) {
      AttributeSet.empty
    }
    else {
      groupingExpressions
        .map(g => g.references)
        .reduce((g1, g2) => g1 ++ g2)
    }

    // 0MA queries can be evaluated purely by bottom-up semi joins
    // Currently, they are limited to Min and Max queries
    // For all aggregates (0MA or counting-based), check if there are no references to attributes
    // (e.g., COUNT(1)) or the references are not part of the grouping attributes
    // TODO remove duplicated code. Use enum for representing query types?
    val zeroMAAggregates = aggExpressions
      .filter(agg => agg.references.isEmpty || !(agg.references subsetOf groupAttributes))
      .filter(agg => is0MA(agg))
    val percentileAggregates = aggExpressions
      .filter(agg => agg.references.isEmpty || !(agg.references subsetOf groupAttributes))
      .filter(agg => isPercentile(agg))
    val countingAggregates = aggExpressions
      .filter(agg => agg.references.isEmpty || !(agg.references subsetOf groupAttributes))
      .filter(agg => isCounting(agg))
    val sumAggregates = aggExpressions
      .filter(agg => agg.references.isEmpty || !(agg.references subsetOf groupAttributes))
      .filter(agg => isSum(agg))
    val averageAggregates = aggExpressions
      .filter(agg => agg.references.isEmpty || !(agg.references subsetOf groupAttributes))
      .filter(agg => isAverage(agg))
    val projectExpressions = aggExpressions
      .filter(agg => isNonAgg(agg))

    if (zeroMAAggregates.isEmpty
      && percentileAggregates.isEmpty
      && countingAggregates.isEmpty
      && sumAggregates.isEmpty
      && averageAggregates.isEmpty) {
      logWarning("query is not applicable (0MA, counting, percentile, sum)")
      agg
    }
    else {
      logWarning("group attributes: " + groupAttributes)
      logWarning("counting aggregates: " + countingAggregates)

      val allAggAttributes = aggregateAttributes ++ groupAttributes
      val hg = new Hypergraph(items, conditions)
      val jointree = hg.flatGYO

      if (jointree == null) {
        logWarning("join is cyclic")
        agg
      }
      else {
        logWarning("join tree: \n" + jointree)
        // First check if there is a single tree node, i.e., relation that contains all attributes
        // contained in the aggregate functions and the GROUP BY clause
        val nodeContainingAttributes = jointree.findNodeContainingAttributes(allAggAttributes)
        if (nodeContainingAttributes == null) {
          logWarning("query is not 0MA")
          agg
        }
        else {
          val root = nodeContainingAttributes.reroot
          logWarning("rerooted: \n" + root)

          if (countingAggregates.isEmpty
            && percentileAggregates.isEmpty
            && sumAggregates.isEmpty
            && averageAggregates.isEmpty) {
            // If the query is a 0MA query, only perform bottom-up semijoins
            val yannakakisJoins = root.buildBottomUpJoins

            val newAgg = Aggregate(groupingExpressions, aggExpressions,
              yannakakisJoins)
            logWarning("new aggregate: " + newAgg)
            newAgg
          }
          else {
            // Otherwise, use the counting plan as a basis
            if (countingAggregates.nonEmpty) {
              val starCountingAggregates = countingAggregates
                .filter(agg => agg.references.isEmpty)
              logWarning("star counting aggregates: " + starCountingAggregates)
              val (yannakakisJoins, countingAttribute, _, _) =
                root.buildBottomUpJoinsCounting(aggregateAttributes, keyRefs, uniqueConstraints,
                  conf.yannakakisCountGroupInLeavesEnabled)

              val newCountingAggregates = starCountingAggregates
                .map(agg => agg.transformDown {
                  case AggregateExpression(aggFn, mode, isDistinct, filter, resultId)
                  => aggFn match {
                    case Count(s) => AggregateExpression(
                      Sum(countingAttribute), mode, isDistinct, filter, resultId)
                  }
                }).asInstanceOf[Seq[NamedExpression]]
              val newAgg = Aggregate(groupingExpressions,
                newCountingAggregates ++ projectExpressions,
                Project(projectList ++ Seq(countingAttribute), yannakakisJoins))
              logWarning("new aggregate: " + newAgg)
              newAgg
            }
            else if (percentileAggregates.nonEmpty) {
              logWarning("percentile aggregates: " + percentileAggregates)
              val (yannakakisJoins, countingAttribute, _, lastJoinWasSemijoin) =
                root.buildBottomUpJoinsCounting(aggregateAttributes, keyRefs, uniqueConstraints,
                  conf.yannakakisCountGroupInLeavesEnabled)

              val newPercentileAggregates = percentileAggregates
                .map(agg => agg.transformDown {
                  case AggregateExpression(aggFn, mode, isDistinct, filter, resultId)
                  => aggFn match {
                    case Percentile(c, percExp, freqExp, mutableAggBufferOffset,
                    inputAggBufferOffset, reverse) =>
                      val freqExpr = if (lastJoinWasSemijoin) Literal(1) else countingAttribute
                      AggregateExpression(
                      Percentile(c, percExp, freqExpr, mutableAggBufferOffset,
                        inputAggBufferOffset, reverse), mode, isDistinct, filter, resultId)
                  }
                }).asInstanceOf[Seq[NamedExpression]]
              val newAgg = Aggregate(groupingExpressions,
                newPercentileAggregates ++ projectExpressions,
                Project(projectList ++ Seq(countingAttribute), yannakakisJoins))
              logWarning("new aggregate: " + newAgg)
              newAgg
            }
            else if (averageAggregates.nonEmpty) {
              logWarning("average aggregates: " + averageAggregates)
              logWarning("project exprs: " + projectExpressions)
              val (yannakakisJoins, countingAttribute, _, _) =
                root.buildBottomUpJoinsCounting(aggregateAttributes, keyRefs, uniqueConstraints,
                  conf.yannakakisCountGroupInLeavesEnabled)

              val newAverageAggregates = averageAggregates
                .map(agg => agg.transformUp {
                  case AggregateExpression(aggFn, mode, isDistinct, filter, resultId) =>
                    val avgAggregateExpr = AggregateExpression(aggFn.transformUp {
                      case a@Average(c, evalMode) =>
                        logWarning("avg: " + a)
                        Sum(c.transformUp {
                          case att: Attribute => Multiply(att,
                            Cast(countingAttribute, att.dataType), evalMode)
                        }, evalMode)
                    }.asInstanceOf[AggregateFunction], mode, isDistinct, filter, resultId)
                    val sumAggregateExpr = Sum(countingAttribute).toAggregateExpression()
                  Divide(avgAggregateExpr,
                    Cast(sumAggregateExpr, avgAggregateExpr.dataType))
                }).asInstanceOf[Seq[NamedExpression]]
              val newAgg = Aggregate(groupingExpressions, newAverageAggregates ++
                projectExpressions,
                Project(projectList ++ Seq(countingAttribute), yannakakisJoins))
              logWarning("new aggregate: " + newAgg)
              newAgg
            }
            else {
              logWarning("sum aggregates: " + sumAggregates)
              logWarning("project exprs: " + projectExpressions)
              val (yannakakisJoins, countingAttribute, _, _) =
                root.buildBottomUpJoinsCounting(aggregateAttributes, keyRefs, uniqueConstraints,
                  conf.yannakakisCountGroupInLeavesEnabled)

              val newSumAggregates = sumAggregates
                .map(agg => agg.transformDown {
                  case AggregateExpression(aggFn, mode, isDistinct, filter, resultId) =>
                    AggregateExpression(aggFn.transformUp {
                    case s @ Sum(c, evalMode) =>
                      logWarning("sum: " + s)
                      Sum(c.transformUp {
                      case att: Attribute => Multiply(att,
                        Cast(countingAttribute, att.dataType), evalMode)
                    }, evalMode)
                  }.asInstanceOf[AggregateFunction], mode, isDistinct, filter, resultId)
                }).asInstanceOf[Seq[NamedExpression]]
              val newAgg = Aggregate(groupingExpressions, newSumAggregates ++ projectExpressions,
              //  yannakakisJoins)
              Project(projectList ++ Seq(countingAttribute), yannakakisJoins))
              logWarning("new aggregate: " + newAgg)
              newAgg
            }
          }
        }
      }
    }
  }
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.yannakakisEnabled) {
      plan
    }
    else {
      plan.transformDownWithPruning(_.containsPattern(TreePattern.AGGREGATE), ruleId) {
        case agg@Aggregate(groupingExpressions, aggExpressions,
        join@Join(_, _, Inner, _, _)) => agg
        case agg@Aggregate(groupingExpressions, aggExpressions,
        filter@Filter(filterConds,
        join@Join(_, _, Inner, _, _))) => agg
        case agg@Aggregate(groupingExpressions, aggExpressions,
        project@Project(projectList,
        join@Join(_, _, Inner, _, _))) =>
          rewritePlan(agg, groupingExpressions, aggExpressions, projectList,
            join, keyRefs = Seq(), uniqueConstraints = Seq())
        case agg@Aggregate(groupingExpressions, aggExpressions,
        project@Project(projectList,
        FKHint(join@Join(_, _, Inner, _, _), keyRefs, uniqueConstraints))) =>
          rewritePlan(agg, groupingExpressions, aggExpressions, projectList,
            join, keyRefs, uniqueConstraints)
        case agg@Aggregate(_, _, _) =>
          logWarning("not applicable to aggregate: " + agg)
          agg
      }
    }
  }
  def is0MA(expr: Expression): Boolean = {
    expr match {
      case Alias(child, name) => is0MA(child)
      case AggregateExpression(aggFn, mode, isDistinct, filter, resultId) => aggFn match {
        case Min(c) => true
        case Max(c) => true
        case _ => false
      }
      case _: Attribute => true
      case _ => false
    }
  }
  def isCounting(expr: Expression): Boolean = {
    expr match {
      case Alias(child, name) => isCounting(child)
      case ToPrettyString(child, tz) => isCounting(child)
      case AggregateExpression(aggFn, mode, isDistinct, filter, resultId) => aggFn match {
        case Count(s) => true
        case _ => false
      }
      case _ => false
    }
  }

  def isPercentile(expr: Expression): Boolean = {
    expr match {
      case Alias(child, name) => isPercentile(child)
      case ToPrettyString(child, tz) => isPercentile(child)
      case Multiply(l, r, _) => isPercentile(l) || isPercentile(r)
      case Divide(l, r, _) => isPercentile(l) || isPercentile(r)
      case Add(l, r, _) => isPercentile(l) || isPercentile(r)
      case Subtract(l, r, _) => isPercentile(l) || isPercentile(r)
      case AggregateExpression(aggFn, mode, isDistinct, filter, resultId) => aggFn match {
        case Percentile(_, _, _, _, _, _) => true
        case _ => false
      }
      case _ => false
    }
  }

  def isSum(expr: Expression): Boolean = {
    expr match {
      case Alias(child, name) => isSum(child)
      case ToPrettyString(child, tz) => isSum(child)
      case Multiply(l, r, _) => isSum(l) || isSum(r)
      case Divide(l, r, _) => isSum(l) || isSum(r)
      case Add(l, r, _ ) => isSum(l) || isSum(r)
      case Subtract(l, r, _) => isSum(l) || isSum(r)
      case AggregateExpression(aggFn, mode, isDistinct, filter, resultId) => aggFn match {
        case Sum(_, _) => true
        case _ => false
      }
      case _ => false
    }
  }

  def isAverage(expr: Expression): Boolean = {
    expr match {
      case Alias(child, name) => isAverage(child)
      case ToPrettyString(child, tz) => isAverage(child)
      case Multiply(l, r, _) => isAverage(l) || isAverage(r)
      case Divide(l, r, _) => isAverage(l) || isAverage(r)
      case Add(l, r, _) => isAverage(l) || isAverage(r)
      case Subtract(l, r, _) => isAverage(l) || isAverage(r)
      case AggregateExpression(aggFn, mode, isDistinct, filter, resultId) => aggFn match {
        case Average(_, _) => true
        case _ => false
      }
      case _ => false
    }
  }

  def isNonAgg(expr: Expression): Boolean = {
    expr match {
      case Alias(child, name) => isNonAgg(child)
      case ToPrettyString(child, tz) => isNonAgg(child)
      case Multiply(l, r, _) => isNonAgg(l) && isNonAgg(r)
      case Divide(l, r, _) => isNonAgg(l) && isNonAgg(r)
      case Add(l, r, _) => isNonAgg(l) && isNonAgg(r)
      case Subtract(l, r, _) => isNonAgg(l) && isNonAgg(r)
      case _: Attribute => true
      case _: Literal => true
      case _ => false
    }
  }

  /**
   * Extracts items of consecutive inner joins and join conditions.
   * This method works for bushy trees and left/right deep trees.
   */
  private def extractInnerJoins(plan: LogicalPlan): (Seq[LogicalPlan], ExpressionSet) = {
    plan match {
      // replace innerlike by more general join type?
      case Join(left, right, _: InnerLike, Some(cond), _) =>
        val (leftPlans, leftConditions) = extractInnerJoins(left)
        val (rightPlans, rightConditions) = extractInnerJoins(right)
        (leftPlans ++ rightPlans, leftConditions ++ rightConditions ++
          splitConjunctivePredicates(cond))
      case Project(projectList, j@Join(_, _, _: InnerLike, Some(cond), _))
        if projectList.forall(_.isInstanceOf[Attribute]) =>
        extractInnerJoins(j)
      case _ =>
        (Seq(plan), ExpressionSet())
    }
  }
}

class HGEdge(val vertices: Set[String], val name: String, val planReference: LogicalPlan,
             val attributeToVertex: mutable.Map[ExprId, String]) {
  val vertexToAttribute: Map[String, Attribute] = planReference.outputSet.map(
      att => (attributeToVertex.getOrElse(att.exprId, null), att))
    .toMap
  def attributes: AttributeSet = {
    planReference.outputSet
  }
  def contains(other: HGEdge): Boolean = {
    other.vertices subsetOf vertices
  }
  def containsNotEqual (other: HGEdge): Boolean = {
    contains(other) && !(vertices subsetOf other.vertices)
  }
  def outputSet: AttributeSet = planReference.outputSet
  def copy(newVertices: Set[String] = vertices,
           newName: String = name,
           newPlanReference: LogicalPlan = planReference): HGEdge =
    new HGEdge(newVertices, newName, newPlanReference, attributeToVertex)
  override def toString: String = s"""${name}(${vertices.mkString(", ")})"""
}
class HTNode(val edges: Set[HGEdge], var children: Set[HTNode], var parent: HTNode)
  extends Logging {
  def buildBottomUpJoins: LogicalPlan = {
    val edge = edges.head
    val scanPlan = edge.planReference
    val vertices = edge.vertices
    var prevJoin: LogicalPlan = scanPlan
    for (c <- children) {
      val childEdge = c.edges.head
      val childVertices = childEdge.vertices
      val overlappingVertices = vertices intersect childVertices
      val joinConditions = overlappingVertices
        .map(vertex => (edge.vertexToAttribute(vertex), childEdge.vertexToAttribute(vertex)))
        .map(atts => EqualTo(atts._1, Cast(atts._2, atts._1.dataType)).asInstanceOf[Expression])
        .reduceLeft((e1, e2) => And(e1, e2).asInstanceOf[Expression])
      val semijoin = Join(prevJoin, c.buildBottomUpJoins,
        LeftSemi, Option(joinConditions), JoinHint(Option.empty, Option.empty))
      prevJoin = semijoin
    }
    prevJoin
  }

  def buildBottomUpJoinsCounting(aggregateAttributes: AttributeSet, keyRefs: Seq[Seq[Expression]],
                                 uniqueConstraints: Seq[Seq[Expression]], groupInLeaves: Boolean):
  (LogicalPlan, NamedExpression, Boolean, Boolean) = {

    val edge = edges.head
    val scanPlan = edge.planReference
    val vertices = edge.vertices
    val primaryKeys = AttributeSet(keyRefs.map(ref => ref.last.references.head))
    val uniqueSets = uniqueConstraints.map(constraint => AttributeSet(constraint))

    var prevCountExpr: NamedExpression = if (groupInLeaves) {
      Alias(Count(Literal(1, IntegerType)).toAggregateExpression(), "c")()
    }
    else {
      Alias(Literal(1, IntegerType), "c")()
    }
    // Only group counts in leaves if it is explicitly enabled and there are no known
    // primary keys in the leaf
    var prevPlan: LogicalPlan = if (groupInLeaves
      && !scanPlan.output.exists(att => primaryKeys.contains(att))
      && !uniqueSets.exists(uniqueSet => uniqueSet subsetOf scanPlan.outputSet)) {
      Aggregate(scanPlan.output, Seq(prevCountExpr) ++ scanPlan.output, scanPlan)
    }
    else {
      Project(
        scanPlan.output ++ Seq(prevCountExpr), scanPlan)
    }
    var isLeafNode = true
    var prevSemijoined = false

    for (c <- children) {
      val childEdge = c.edges.head
      val childVertices = childEdge.vertices
      val overlappingVertices = vertices intersect childVertices
      val (bottomUpJoins, childCountExpr, rightPlanIsLeaf, childWasSemijoined) =
        c.buildBottomUpJoinsCounting(aggregateAttributes, keyRefs, uniqueConstraints, groupInLeaves)

      val countExpressionLeft = Alias(Sum(prevCountExpr.toAttribute).toAggregateExpression(), "c")()
      val countExpressionRight = Alias(
        Sum(childCountExpr.toAttribute).toAggregateExpression(), "c")()

      val countGroupLeft = vertices.map(v => edge.vertexToAttribute(v)).toSeq
      val countGroupRight = overlappingVertices.map(v => childEdge.vertexToAttribute(v)).toSeq

      // Grouping directly after each leaf node results in bad performance.
      // Possible solution: make use of primary keys to determine if grouping is necessary
      val (leftPlan, leftCountAttribute) = if (isLeafNode) {
        (prevPlan, prevCountExpr.toAttribute)
      }
      else {
        val outputAggregateAttributes = prevPlan.outputSet intersect aggregateAttributes
        val groupAttributes = countGroupLeft ++ outputAggregateAttributes
        // Check if the grouping attributes contain a primary key.
        // In this case, grouping would not remove any tuples, hence do not aggregate.
        if (groupAttributes.exists(att => primaryKeys contains att)
        || uniqueSets.exists(uniqueSet => uniqueSet subsetOf AttributeSet(groupAttributes))) {
          (prevPlan, prevCountExpr.toAttribute)
        }
        else {
          (Aggregate(groupAttributes,
            Seq(countExpressionLeft) ++ groupAttributes, prevPlan),
            countExpressionLeft.toAttribute)
        }
      }

      val (rightPlan, rightCountAttribute) = if (rightPlanIsLeaf) {
        (bottomUpJoins, childCountExpr.toAttribute)
      }
      else {
        if (countGroupRight.exists(att => primaryKeys contains att)
        || uniqueSets.exists(uniqueSet => uniqueSet subsetOf AttributeSet(countGroupRight))) {
          (bottomUpJoins, childCountExpr.toAttribute)
        }
        else {
          (Aggregate(countGroupRight,
            Seq(countExpressionRight) ++ countGroupRight, bottomUpJoins),
            countExpressionRight.toAttribute)
        }
      }

      val joinConditions = overlappingVertices
        .map(vertex => (edge.vertexToAttribute(vertex), childEdge.vertexToAttribute(vertex)))
        .map(atts => EqualTo(atts._1, Cast(atts._2, atts._1.dataType)).asInstanceOf[Expression])
        .reduceLeft((e1, e2) => And(e1, e2).asInstanceOf[Expression])

      //      val joinHint = JoinHint(Option(HintInfo(Option(PREFER_SHUFFLE_HASH))),
      //        Option(HintInfo(Option(PREFER_SHUFFLE_HASH))))
      val joinHint = JoinHint(Option.empty, Option.empty)
      // val joinHint = JoinHint(Option(HintInfo(Option(SHUFFLE_MERGE))),
      //  Option(HintInfo(Option(SHUFFLE_MERGE))))
      // Each keyref [fk, pk] represents a reference from a foreign key fk to a primary key pk
      val canSemiJoin: Boolean = overlappingVertices
        .map(vertex => (edge.vertexToAttribute(vertex), childEdge.vertexToAttribute(vertex)))
        // Check if the fk is contained in the parent node attributes
        .forall(atts => keyRefs.exists(ref => ref.head.references.head.exprId == atts._1.exprId
          // and if the pk is contained in the child node attributes
        && ref.last.references.head.exprId == atts._2.exprId))

      if (canSemiJoin) {
        prevSemijoined = canSemiJoin
      }

      logWarning("keyRefs: " + keyRefs)
      logWarning("edge: " + edge)
      logWarning("overlapping vertices: " + overlappingVertices
        .map(vertex => (edge.vertexToAttribute(vertex), childEdge.vertexToAttribute(vertex))))
      logWarning("can semi join: " + canSemiJoin)

      val join = Join(leftPlan, rightPlan,
        if (canSemiJoin) LeftSemi else Inner, Option(joinConditions), joinHint)
      logWarning("join output: " + join.output)
      val finalCountExpr = if (canSemiJoin) {
        leftCountAttribute
      } else {
        Alias(Multiply(
          Cast(leftCountAttribute, rightCountAttribute.dataType),
          rightCountAttribute), "c")()
      }
      val multiplication = Project(
        (if (canSemiJoin) Seq() else Seq(finalCountExpr)) ++ join.output, join)

      prevPlan = multiplication
      prevCountExpr = finalCountExpr
      isLeafNode = false
    }
    (prevPlan, prevCountExpr.toAttribute, isLeafNode, prevSemijoined)
  }
  def reroot: HTNode = {
    if (parent == null) {
      this
    }
    else {
      logWarning("parent: " + parent)
      var current = this
      var newCurrent = this.copy(newParent = null)
      val root = newCurrent
      while (current.parent != null) {
        val p = current.parent
        logWarning("p: " + p)
        val newChild = p.copy(newChildren = p.children - current, newParent = null)
        logWarning("new child: " + newChild)
        newCurrent.children += newChild
        logWarning("c: " + current)
        current = p
        newCurrent = newChild
      }
      root.setParentReferences
      root
    }
  }
  def findNodeContainingAttributes(aggAttributes: AttributeSet): HTNode = {
    val nodeAttributes = edges
      .map(e => e.planReference.outputSet)
      .reduce((e1, e2) => e1 ++ e2)
    logWarning("aggAttributes: " + aggAttributes)
    logWarning("nodeAttributes: " + nodeAttributes)
    if (aggAttributes subsetOf nodeAttributes) {
      logWarning("found subset in:\n" + this)
      this
    } else {
      for (c <- children) {
        val node = c.findNodeContainingAttributes(aggAttributes)
        if (node != null) {
          return node
        }
      }
      null
    }
  }

  def setParentReferences: Unit = {
    for (c <- children) {
      c.parent = this
      c.setParentReferences
    }
  }
  def copy(newEdges: Set[HGEdge] = edges, newChildren: Set[HTNode] = children,
           newParent: HTNode = parent): HTNode =
    new HTNode(newEdges, newChildren, newParent)
  private def toString(level: Int = 0): String =
    s"""${"-- ".repeat(level)}TreeNode(${edges})""" +
      s"""[${edges.map(e => e.planReference.outputSet)}] [[parent: ${parent != null}]]
         |${children.map(c => c.toString(level + 1)).mkString("\n")}""".stripMargin
  override def toString: String = toString(0)
}
class Hypergraph (private val items: Seq[LogicalPlan],
                  private val conditions: ExpressionSet) extends Logging {

  private var vertices: mutable.Set[String] = mutable.Set.empty
  private var edges: mutable.Set[HGEdge] = mutable.Set.empty
  private var vertexToAttributes: mutable.Map[String, Set[Attribute]] = mutable.Map.empty
  private var attributeToVertex: mutable.Map[ExprId, String] = mutable.Map.empty

  private var equivalenceClasses: Set[Set[Attribute]] = Set.empty

  for (cond <- conditions) {
    // logWarning("condition: " + cond)
    cond match {
      case EqualTo(lhs, rhs) =>
        // logWarning("equality condition: " + lhs.references + " , " + rhs.references)
        val lAtt = lhs.references.head
        val rAtt = rhs.references.head
        equivalenceClasses += Set(lAtt, rAtt)
      case other =>
        logWarning("other")
    }
  }

  // Compute the equivalence classes
  while (combineEquivalenceClasses) {

  }

  logWarning("equivalence classes: " + equivalenceClasses)

  for (equivalenceClass <- equivalenceClasses) {
    val attName = equivalenceClass.head.toString
    vertices.add(attName)
    vertexToAttributes.put(attName, equivalenceClass)
    for (equivAtt <- equivalenceClass) {
      attributeToVertex.put(equivAtt.exprId, attName)
    }
  }

  logWarning("vertex to attribute mapping: " + vertexToAttributes)
  logWarning("attribute to vertex mapping: " + attributeToVertex)

  var tableIndex = 1
  for (item <- items) {
    logWarning("join item: " + item)

    val projectAttributes = item.outputSet
    val hyperedgeVertices = projectAttributes
      .map(att => attributeToVertex.getOrElse(att.exprId, ""))
      .filterNot(att => att.equals("")).toSet

    val hyperedge = new HGEdge(hyperedgeVertices, s"E${tableIndex}", item, attributeToVertex)
    tableIndex += 1
    edges.add(hyperedge)
  }

  logWarning("hyperedges: " + edges)

  private def combineEquivalenceClasses: Boolean = {
    for (set <- equivalenceClasses) {
      for (otherSet <- (equivalenceClasses - set)) {
        if ((set intersect otherSet).nonEmpty) {
          val unionSet = (set union otherSet)
          equivalenceClasses -= set
          equivalenceClasses -= otherSet
          equivalenceClasses += unionSet
          return true
        }
      }
    }
    false
  }

  def isAcyclic: Boolean = {
    flatGYO == null
  }

  def flatGYO: HTNode = {
    var gyoEdges: mutable.Set[HGEdge] = mutable.Set.empty
    var mapping: mutable.Map[String, HGEdge] = mutable.Map.empty
    var root: HTNode = null
    var treeNodes: mutable.Map[String, HTNode] = mutable.Map.empty

    for (edge <- edges) {
      mapping.put(edge.name, edge)
      gyoEdges.add(edge.copy())
    }

    var progress = true
    while (gyoEdges.size > 1 && progress) {
      for (e <- gyoEdges) {
        // logWarning("gyo edge: " + e)
        // Remove vertices that only occur in this edge
        val allOtherVertices = (gyoEdges - e).map(o => o.vertices)
          .reduce((o1, o2) => o1 union o2)
        val singleNodeVertices = e.vertices -- allOtherVertices

        // logWarning("single vertices: " + singleNodeVertices)

        val eNew = e.copy(newVertices = e.vertices -- singleNodeVertices)
        gyoEdges = (gyoEdges - e) + eNew

        // logWarning("removed single vertices: " + gyoEdges)
      }

      var nodeAdded = false
      for (e <- gyoEdges) {
        logWarning("gyo edge: " + e)
        val supersets = gyoEdges.filter(o => o containsNotEqual e)
        logWarning("supersets: " + supersets)

        // For each edge e, check if it is not contained in another edge
        if (supersets.isEmpty) {
          // Append the contained edges as children in the tree
          val containedEdges = gyoEdges.filter(o => (e contains o) && (e.name != o.name))
          val parentNode = treeNodes.getOrElse(e.name, new HTNode(Set(e), Set(), null))
          val childNodes = containedEdges
            .map(c => treeNodes.getOrElse(c.name, new HTNode(Set(c), Set(), null)))
            .toSet
          logWarning("parentNode: " + parentNode)
          parentNode.children ++= childNodes
          logWarning("subsets: " + childNodes)
          if (childNodes.nonEmpty) {
            nodeAdded = true
          }

          treeNodes.put(e.name, parentNode)
          childNodes.foreach(c => treeNodes.put(c.edges.head.name, c))
          root = parentNode
          root.setParentReferences
          gyoEdges --= containedEdges
        }
      }
      if (!nodeAdded) progress = false
    }

    if (gyoEdges.size > 1) {
      return null
    }

    root
  }
}

