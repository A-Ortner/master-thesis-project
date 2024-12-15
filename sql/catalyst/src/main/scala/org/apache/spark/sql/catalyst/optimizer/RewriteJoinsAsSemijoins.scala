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
import org.apache.spark.sql.catalyst.dsl.expressions.DslExpression
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.{Inner, InnerLike, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DecimalType.DoubleDecimal

object RewriteJoinsAsSemijoins extends Rule[LogicalPlan] with PredicateHelper {
  /** As in [[PhysicalAggregation]] the aggregate  expressions are extracted from the
   * outputExpressions ([[NamedExpression]]. Then these are replaced in the output expressions
   * by new references.
   * */
  def rewritePlan(agg: Aggregate, unnamedGroupingExpressions: Seq[Expression],
                  resultExpressions: Seq[NamedExpression], projectList: Seq[NamedExpression],
                  join: Join, keyRefs: Seq[Seq[Expression]],
                  uniqueConstraints: Seq[Seq[Expression]]) : LogicalPlan = {
    val startTime = System.nanoTime()
    logWarning("applying rewriting to join: " + agg)
    // Extract the join items (including any filters, etc.)
    val (items, conditions) = extractInnerJoins(join)

    val equivalentAggregateExpressions = new EquivalentExpressions
    // Extract the AggregateExpressions from the result expressions
    // e.g., SUM(x) from SUM(x) + 1
    val aggregateExpressions = resultExpressions.flatMap { expr =>
      expr.collect {
        // addExpr() always returns false for non-deterministic expressions and do not add them.
        case a: AggregateExpression if !equivalentAggregateExpressions.addExpr(a) =>
          a
      }
    }

    // Maps the resultAttribute of the first AggregateExpression to the resulAttribute of
    // the last one. This is needed for constructing the final rewritten result exprs
    val lastAggMap = new mutable.HashMap[Attribute, AggregateExpression]()
    // We store the last sum, when a sum aggregate is propagated upwards
    val lastSumMap = new mutable.HashMap[Attribute, Attribute]()
    // Pull the multiplication of the sum by the count into the next sum aggregate
    val nextMultiplicationMap = new mutable.HashMap[Attribute, Expression]()

    val namedGroupingExpressions = unnamedGroupingExpressions.map {
      case ne: NamedExpression => ne -> ne
      // If the expression is not a NamedExpressions, we add an alias.
      // So, when we generate the result of the operator, the Aggregate Operator
      // can directly get the Seq of attributes representing the grouping expressions.
      case other =>
        val withAlias = Alias(other, other.toString)()
        other -> withAlias
    }
    val groupingExpressions = namedGroupingExpressions.map(_._2)
    val groupExpressionMap = namedGroupingExpressions.toMap
    val aggregateAttributes = resultExpressions.map(expr => expr.references)
      .reduce((a1, a2) => a1 ++ a2)
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
    val zeroMAAggregates = resultExpressions
      .filter(agg => agg.references.isEmpty || !(agg.references subsetOf groupAttributes))
      .filter(agg => is0MA(agg))
    val percentileAggregates = resultExpressions
      .filter(agg => agg.references.isEmpty || !(agg.references subsetOf groupAttributes))
      .filter(agg => isPercentile(agg))
    val countingAggregates = resultExpressions
      .filter(agg => agg.references.isEmpty || !(agg.references subsetOf groupAttributes))
      .filter(agg => isCounting(agg))
    val sumAggregates = resultExpressions
      .filter(agg => agg.references.isEmpty || !(agg.references subsetOf groupAttributes))
      .filter(agg => isSum(agg))
    val averageAggregates = resultExpressions
      .filter(agg => agg.references.isEmpty || !(agg.references subsetOf groupAttributes))
      .filter(agg => isAverage(agg))
    val projectExpressions = resultExpressions
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
//      logWarning("group attributes: " + groupAttributes)
//      logWarning("counting aggregates: " + countingAggregates)

      val hg = new Hypergraph(items, conditions)
//      logWarning("hypergraph:\n" + hg.toString)
      val jointree = hg.flatGYO

      if (jointree == null) {
        logWarning("join is cyclic")
        logWarning("time difference: " + (System.nanoTime() - startTime))
        agg
      }
      else {
        logWarning("join tree: \n" + jointree)
        // First check if there is a single tree node, i.e., relation that contains all attributes
        // contained in the GROUP BY clause and agg expressions
        val nodeContainingAllAttributes = jointree
          .findNodeContainingAttributes(aggregateAttributes ++ groupAttributes)
        if (nodeContainingAllAttributes == null) {
          logWarning("not guarded! there is no node containing all agg and group attributes")
          logWarning("time difference: " + (System.nanoTime() - startTime))

          var root = jointree
          val nodeContainingGroupAttributes = jointree.findNodeContainingAttributes(groupAttributes)

          var piecewiseGuarded = false
          if (nodeContainingGroupAttributes != null) {
            piecewiseGuarded = true
            logWarning("piecewise-guarded!")
            root = nodeContainingGroupAttributes.reroot
            // Choose the root containing the group attributes, if one exists
            // If none contains all of them, choose any join tree
          }
          else {
            if (!conf.yannakakisUnguardedEnabled) {
              logWarning("unguarded. plan is not changed")
              return agg
            }
          }
          logWarning("applicable query (joins=" + (items.size - 1) + ")")

          val (yannakakisJoins, countingAttribute, _, _) =
            root.buildBottomUpJoinsCounting(aggregateAttributes, groupingExpressions,
              aggregateExpressions, lastAggMap, lastSumMap, nextMultiplicationMap,
              keyRefs, uniqueConstraints,
              conf.yannakakisCountGroupInLeavesEnabled,
              usePhysicalCountJoin = conf.yannakakisPhysicalCountEnabled)

          var projectList: mutable.MutableList[NamedExpression] =
            new mutable.MutableList[NamedExpression]

//          logWarning("lastAggMap: " + lastAggMap)
//          logWarning("lastSumMap: " + lastSumMap)

          val rewrittenResultExpressions = resultExpressions.map {
            expr =>
              expr.transformDown {
                case ae: AggregateExpression =>
                  val resultAtt = equivalentAggregateExpressions.getExprState(ae).map(_.expr)
                    .getOrElse(ae).asInstanceOf[AggregateExpression].resultAttribute
                  ae.aggregateFunction match {
                    case a: Count =>
                      if (lastSumMap.contains(resultAtt)) {
                        val lastSumAtt = lastSumMap(resultAtt)
                        projectList += lastSumAtt
                        Sum(lastSumAtt).toAggregateExpression()
                      }
                      else {
                        projectList ++= a.references
                        Sum(Multiply(
                          a.children.head, Cast(countingAttribute, a.children.head.dataType)))
                          .toAggregateExpression()
                      }
                    case _ =>
                      // The final aggregation buffer's attributes will be
                      // `finalAggregationAttributes`,
                      // so replace each aggregate expression by its corresponding
                      // attribute in the set:
                      ae.transformDown {
                        case a: AggregateFunction =>
                          a match {
                            // TODO this could be simplified by merging Sum and Count cases
                            case Sum(_, _) =>
                              if (lastSumMap.contains(resultAtt)) {
                                val lastSumAtt = lastSumMap(resultAtt)
                                //       val lastMultiplyExpr = nextMultiplicationMap(resultAtt)
                                projectList += lastSumAtt
                                a.withNewChildren(Seq(lastSumAtt))
                              }
                              else {
                                projectList ++= a.references
                                a.withNewChildren(
                                  Seq(Multiply(a.children.head,
                                    Cast(countingAttribute, a.children.head.dataType))))
                              }
                            case _ =>
                              // MIN, MAX
                              if (lastAggMap.contains(resultAtt)) {
                                val lastResultAtt = lastAggMap(resultAtt).resultAttribute
                                projectList += lastResultAtt

                                a.withNewChildren(
                                  Seq(lastResultAtt))

                              }
                              else {
                                // If the att is not in the lastAggMap, it means that the attribute
                                // occurred in the root of the tree - and only gets propagated
                                // up on the left.
                                //  Therefore, there is no intermediate aggregation function
                                //  in-between and we can directly access the attribute.
                                projectList ++= a.references

                                a
                              }
                          }
                      }
                  }

              case expression if !expression.foldable =>
                // Since we're using `namedGroupingAttributes` to extract the grouping key
                // columns, we need to replace grouping key expressions with their corresponding
                // attributes. We do not rely on the equality check at here since attributes may
                // differ cosmetically. Instead, we use semanticEquals.
                groupExpressionMap.collectFirst {
                  case (expr, ne) if expr semanticEquals expression => ne.toAttribute
                }.getOrElse(expression)
            }.asInstanceOf[NamedExpression]
          }
//          logWarning("rewritten result expressions: " + rewrittenResultExpressions)

          projectList ++= groupingExpressions

          val newAgg = Aggregate(groupingExpressions,
            rewrittenResultExpressions,
            Project(projectList
              ++ Seq(countingAttribute), yannakakisJoins))
//          val newAgg = Aggregate(groupingExpressions,
//            newCountingAggregates ++ rewrittenResultExpressions ++ projectExpressions,
//            yannakakisJoins)
          val queryClass = if (piecewiseGuarded) "piecewise-guarded" else "unguarded"
          logWarning(f"new aggregate ($queryClass): " + newAgg)
          logWarning("time difference: " + (System.nanoTime() - startTime))
          newAgg
        }
        else {
          val root = nodeContainingAllAttributes.reroot
          logWarning("applicable query (joins=" + (items.size - 1) + ")")

          if (countingAggregates.isEmpty
            && percentileAggregates.isEmpty
            && sumAggregates.isEmpty
            && averageAggregates.isEmpty) {
            // If the query is a 0MA query, only perform bottom-up semijoins
            val yannakakisJoins = root.buildBottomUpJoins

            val newAgg = Aggregate(groupingExpressions, resultExpressions,
              yannakakisJoins)
            logWarning("new aggregate (0MA): " + newAgg)
            logWarning("time difference: " + (System.nanoTime() - startTime))
            newAgg
          }
          else {
            val (yannakakisJoins, countingAttribute, _, _) =
              root.buildBottomUpJoinsCounting(aggregateAttributes,
                groupingExpressions,
                aggregateExpressions, lastAggMap, lastSumMap, nextMultiplicationMap,
                keyRefs, uniqueConstraints,
                conf.yannakakisCountGroupInLeavesEnabled,
                usePhysicalCountJoin = conf.yannakakisPhysicalCountEnabled)

            val rewrittenResultExpressions = resultExpressions.map {
              expr =>
                expr.transformDown {
                  case aggExpr @ AggregateExpression(aggFn, mode, isDistinct, filter, resultId) =>
                    aggFn match {
                      case a: Count =>
                        AggregateExpression(
                        Sum(countingAttribute), mode, isDistinct, filter, resultId)

                      case Percentile(c, percExp, freqExp, mutableAggBufferOffset,
                      inputAggBufferOffset, reverse) =>
                        val freqExpr = countingAttribute
                        AggregateExpression(
                          Percentile(c, percExp, freqExpr, mutableAggBufferOffset,
                            inputAggBufferOffset, reverse), mode, isDistinct, filter, resultId)

                      case Average(_, _) =>
                        val aggAttribute = aggFn.references.head
                        val sumAggregateExpr = aggFn.transformUp {
                          case a@Average(c, evalMode) =>
                            Sum(c.transformUp {
                              case att: Attribute => Multiply(att,
                                Cast(countingAttribute, att.dataType), evalMode)
                            }, evalMode)
                        }.asInstanceOf[AggregateFunction].toAggregateExpression()

                        val countAggregateExpr = Sum(
                          If(aggAttribute.isNull,
                            Literal(0L, LongType), countingAttribute))
                          .toAggregateExpression()
                        Cast(
                          if (DoubleType.acceptsType(sumAggregateExpr.dataType) &&
                            DoubleType.acceptsType(countAggregateExpr.dataType)) {
                            Divide(sumAggregateExpr, countAggregateExpr)
                          } else {
                            // TODO check if there is a better way than casting to DoubleDecimal?
                            Divide(Cast(sumAggregateExpr, DoubleDecimal),
                              Cast(countAggregateExpr, DoubleDecimal))
                          }, aggExpr.dataType)

                      case Sum(_, _) =>
                        AggregateExpression(aggFn.transformUp {
                          case s @ Sum(c, evalMode) =>
                            Sum(c.transformUp {
                              case att: Attribute => Multiply(att,
                                Cast(countingAttribute, att.dataType), evalMode)
                            }, evalMode)
                        }.asInstanceOf[AggregateFunction], mode, isDistinct, filter, resultId)
                    }
                }.asInstanceOf[NamedExpression]
            }

            val newAgg = Aggregate(groupingExpressions,
              rewrittenResultExpressions, yannakakisJoins)

            logWarning("new aggregate (guarded): " + newAgg)
            logWarning("time difference: " + (System.nanoTime() - startTime))
            newAgg
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
        case agg@Aggregate(groupingExpressions, aggExpressions,
        project@Project(projectList,
        FKHint(
        project2@Project(projectList2,
        join@Join(_, _, Inner, _, _)), keyRefs, uniqueConstraints))) =>
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
      case ToPrettyString(child, tz) => is0MA(child)
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

  def buildBottomUpJoinsCounting(aggregateAttributes: AttributeSet,
                                 groupingExpressions: Seq[NamedExpression],
                                 aggExpressions: Seq[AggregateExpression],
                                 lastAggMap: mutable.HashMap[Attribute, AggregateExpression],
                                 lastSumMap: mutable.HashMap[Attribute, Attribute],
                                 nextMultiplicationMap: mutable.HashMap[Attribute, Expression],
                                 keyRefs: Seq[Seq[Expression]],
                                 uniqueConstraints: Seq[Seq[Expression]], groupInLeaves: Boolean,
                                 usePhysicalCountJoin: Boolean = false):
  (LogicalPlan, NamedExpression, Boolean, Boolean) = {

    val edge = edges.head
    val scanPlan = edge.planReference
    val vertices = edge.vertices
    val primaryKeys = AttributeSet(keyRefs.map(ref => ref.last.references.head))
    val uniqueSets = uniqueConstraints.map(constraint => AttributeSet(constraint))
    // Get the attributes as part of the join tree
    val nodeAttributes = AttributeSet(vertices.map(v => edge.vertexToAttribute(v)))

    // Check if grouping in leaves is enabled, and no primary keys are part of the leaf
    // Also avoid grouping when the leaves contain output atts, since they are most likely
    // randomly distributed and would lead to a high selectivity
    val groupHere = groupInLeaves &&
      ! nodeAttributes.exists(att => primaryKeys contains att) &&
      ! scanPlan.output.exists(att => aggregateAttributes contains att) &&
      ! uniqueSets.exists(uniqueSet => uniqueSet subsetOf scanPlan.outputSet)
//    logWarning("group here: " + groupHere)

    var prevCountExpr: NamedExpression = if (groupHere) {
      Alias(Count(Literal(1L, LongType)).toAggregateExpression(), "c")()
    }
    else {
      Alias(Literal(1L, LongType), "c")()
    }

    // Only group counts in leaves if it is explicitly enabled and there are no known
    // primary keys in the leaf
    val outputAttributes = scanPlan.output.filter(att => (nodeAttributes contains att)
      || (aggregateAttributes contains att) || (groupingExpressions contains att))
    var prevPlan: LogicalPlan = if (groupHere) {
      Aggregate(outputAttributes, Seq(prevCountExpr) ++
        outputAttributes, scanPlan)
    }
    else {
      // Make sure to project the output only to the attributes as part of the join tree
      // This can occur when we have a FKHint before the join nodes which leads to
      // Spark SQL not projecting away the attributes mentioned in the hints

//      Project(
//        outputAttributes ++ Seq(prevCountExpr), scanPlan)

      scanPlan
    }
    var isLeafNode = true
    var prevSemijoined = false

    var prevChildEdge: HGEdge = edge
    for (c <- children) {
      val childEdge = c.edges.head
      val childVertices = childEdge.vertices
      val overlappingVertices = vertices intersect childVertices
      val (bottomUpJoins, childCountExpr, rightPlanIsLeaf, childWasSemijoined) =
        c.buildBottomUpJoinsCounting(aggregateAttributes,
          groupingExpressions, aggExpressions, lastAggMap, lastSumMap,
          nextMultiplicationMap, keyRefs, uniqueConstraints, groupInLeaves,
          usePhysicalCountJoin = usePhysicalCountJoin)

      val countExpressionLeft = Alias(Sum(prevCountExpr.toAttribute).toAggregateExpression(), "c")()
      val countExpressionRight = Alias(
        Sum(childCountExpr.toAttribute).toAggregateExpression(), "c")()

      val countGroupLeft = vertices.map(v => edge.vertexToAttribute(v)).toSeq
      val countGroupRight = overlappingVertices.map(v => childEdge.vertexToAttribute(v)).toSeq

      // Grouping directly after each leaf node results in bad performance.
      // Possible solution: make use of primary keys to determine if grouping is necessary
      // Construct the left subplan
      val (leftPlan, leftCountAttribute) = if (isLeafNode) {
        (prevPlan, prevCountExpr.toAttribute)
      }
      else {
        logWarning("prevPlan: " + prevPlan)
        logWarning("output: " + prevPlan.output)
        val outputAggregateAttributes = prevPlan.outputSet intersect aggregateAttributes
        val groupAttributes = countGroupLeft ++ outputAggregateAttributes
        val prevChildAttributes = AttributeSet(
          prevChildEdge.vertices.map(v => prevChildEdge.vertexToAttribute(v)))
        // Check if the grouping attributes contain a primary key.
        // In this case, grouping would not remove any tuples, hence do not aggregate.

        // Don't perform aggregation afterwards if a countjoin was performed
        if (usePhysicalCountJoin
          || (prevSemijoined && primaryKeys.exists(att => nodeAttributes contains att))
          || uniqueSets.exists(uniqueSet => AttributeSet(groupAttributes) subsetOf uniqueSet )) {
          (prevPlan, prevCountExpr.toAttribute)
        }
        else {
          (Aggregate(groupAttributes,
            Seq(countExpressionLeft) ++ groupAttributes, prevPlan),
            countExpressionLeft.toAttribute)
        }
      }

      // Construct the right subplan
      val (rightPlan, rightCountAttribute) = if (rightPlanIsLeaf) {
        (bottomUpJoins, childCountExpr.toAttribute)
      }
      else {
        if (usePhysicalCountJoin
          || countGroupRight.forall(att => primaryKeys contains att)
          || uniqueSets.exists(uniqueSet => AttributeSet(countGroupRight) subsetOf uniqueSet )) {
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

      // Currently unused (can be used to e.g., force hash/merge joins)
      val joinHint = JoinHint(Option.empty, Option.empty)

      // Each keyref [fk, pk] represents a reference from a foreign key fk to a primary key pk
      val canSemiJoin: Boolean = overlappingVertices
        .map(vertex => (edge.vertexToAttribute(vertex), childEdge.vertexToAttribute(vertex)))
        // Check if the fk is contained in the parent node attributes
        .forall(atts => keyRefs.exists(ref => ref.head.references.head.exprId == atts._1.exprId
          // and if the pk is contained in the child node attributes
        && ref.last.references.head.exprId == atts._2.exprId))

      val newRightCount = Alias(Literal(1L, LongType), "c")()

      val join = if (usePhysicalCountJoin && !canSemiJoin) {
        var applicableAggExpressions = {
          new mutable.MutableList[AggregateExpression]()
        }
        var multiplySumExpressions = new mutable.MutableList[NamedExpression]()

        def createMultiplication(a: Expression, b: Expression): Expression = {
          val multiplication = if (a.dataType.acceptsType(b.dataType)) {
            Multiply(a, b)
          }
          else {
            a.dataType match {
              case _: DecimalType => Multiply(a, Cast(b, DecimalType(20, 0)))
              case _ => Multiply(a, Cast(b, a.dataType))
            }
          }

          if (multiplication.dataType == a.dataType) {
            multiplication
          }
          else {
            Cast(multiplication, a.dataType)
          }
        }

        def sumOrCountCase(agg: AggregateExpression) = {
          if (lastSumMap.contains(agg.resultAttribute)) {
            val lastSumAtt = lastSumMap(agg.resultAttribute)

            if (rightPlan.outputSet.contains(lastSumAtt)) {
              //         |
              //       Project(ac<-a*c)
              //         |
              //       Y, a<-SUM(a)
              //      /   \
              //    Y(c)      Z(a)
              //
              val newAgg = Sum(lastSumAtt).toAggregateExpression()
              applicableAggExpressions = applicableAggExpressions :+ newAgg

              if (leftPlan.outputSet.contains(leftCountAttribute)) {
                val newSum = Alias(createMultiplication(newAgg.resultAttribute,
                  leftCountAttribute), "sum")()

                multiplySumExpressions = multiplySumExpressions :+ newSum
                lastSumMap.put(agg.resultAttribute, newSum.toAttribute)
              }
              else {
                lastSumMap.put(agg.resultAttribute, newAgg.resultAttribute)
              }
            }

            if (leftPlan.outputSet.contains(lastSumAtt)) {
              //         |
              //       Project(ac<-a*(sc/Y.c))
              //         |
              //       Y, sc<-SUM(Z.c)*Y.c
              //      /     \
              //    Y(a,c)     Z(c)

              val countRightAgg = Count(Literal(1L)).toAggregateExpression()
              applicableAggExpressions = applicableAggExpressions :+ countRightAgg

              val newSum = Alias(createMultiplication(lastSumAtt,
                countRightAgg.resultAttribute), "sum")()

              multiplySumExpressions = multiplySumExpressions :+ newSum
              lastSumMap.put(agg.resultAttribute, newSum.toAttribute)
            }
          }
          else {
            // SUM/COUNT aggregate has not yet occurred somewhere in the tree -
            // check if it starts here
            if (agg.references.subsetOf(rightPlan.outputSet)) {

              // logWarning("is subset")
              //         |
              //       Project(ac<-ac*Y.c)
              //         |
              //       Y, ac <- SUM(a*Z.c)
              //        /   \
              //       /     \
              //    Y(c)     Z(a,c)
              //              /  \
              //             /    \
              //           R(a)   S(c)

              val newAgg = if (rightPlanIsLeaf) {
                agg
              } else {
                agg.transformUp {
                  case a: AggregateFunction =>
                    a.withNewChildren(Seq(createMultiplication(a.children.head,
                      rightCountAttribute)))
                }.asInstanceOf[AggregateExpression]
              }

              applicableAggExpressions = applicableAggExpressions :+ newAgg

              // Left plan is not a leaf
              if (leftPlan.outputSet.contains(leftCountAttribute)) {
                val newSum = Alias(createMultiplication(newAgg.resultAttribute,
                  leftCountAttribute), "sum")()
                multiplySumExpressions = multiplySumExpressions :+ newSum

                lastSumMap.put(agg.resultAttribute, newSum.toAttribute)
              }
              else {
                lastSumMap.put(agg.resultAttribute, newAgg.resultAttribute)
              }
            }
          }
        }

        aggExpressions.foreach(agg => {
          agg.aggregateFunction match {
            case Sum(_, _) =>
              sumOrCountCase(agg)
            case Count(_) =>
              sumOrCountCase(agg)
              // Non-SUM aggregates (MIN/MAX)
            case _ =>
              if (lastAggMap.contains(agg.resultAttribute)) {
                // The aggregate function has already been applied further down the tree
                // and we have to aggregate over it again
                val lastAgg = lastAggMap(agg.resultAttribute)

                // Check if the output of the last aggregate is in the right output
                if (rightPlan.outputSet.contains(lastAgg.resultAttribute)) {
                  val newAgg = lastAgg.transformDown {
                    case a: AggregateFunction => a.withNewChildren(Seq(lastAgg.resultAttribute))
                  }.asInstanceOf[AggregateExpression]
                  lastAggMap.put(agg.resultAttribute, newAgg)

                  applicableAggExpressions = applicableAggExpressions :+ newAgg
                }
              }
              else {
                // This is the first time the aggregate function is applied.
                // Therefore, store the first aggregation in the map
                if (agg.references.subsetOf(rightPlan.outputSet)) {
                  lastAggMap.put(agg.resultAttribute, agg)
                  applicableAggExpressions = applicableAggExpressions :+ agg
                }
              }
          }
        })
//        logWarning("applicable agg expressions: " + applicableAggExpressions)

        val applicableGroupAttributes = groupingExpressions.filter(
          groupExpr => {groupExpr.references.subsetOf(rightPlan.outputSet)}
        )
//        logWarning("applicable group atts: " + applicableGroupAttributes)

        val right = rightPlan

        val countJoin = CountJoin(leftPlan, right,
          if (canSemiJoin) LeftSemi else Inner, Option(joinConditions),
          Option(if (isLeafNode) prevCountExpr else leftCountAttribute),
          Option(if (rightPlanIsLeaf) newRightCount else rightCountAttribute),
          applicableAggExpressions, applicableGroupAttributes, joinHint)

        if (multiplySumExpressions.isEmpty) {
          countJoin
        }
        else {
          Project(countJoin.output ++ multiplySumExpressions, countJoin)
        }
      } else {
        Join(leftPlan, rightPlan,
          if (canSemiJoin) LeftSemi else Inner, Option(joinConditions), joinHint)
      }
//      logWarning("join output: " + join.output)
      val finalCountExpr = if (canSemiJoin) {
        // When semi-joining we only have one count attribute
        leftCountAttribute
      } else {
        if (usePhysicalCountJoin) {
          // The summed-up and multiplied result is already in the right count attribute
          if (rightPlanIsLeaf) {
            newRightCount
          }
          else {
            rightCountAttribute
          }
        }
        else {
          // Multiply the left count with the right count
          Alias(Multiply(
            Cast(leftCountAttribute, rightCountAttribute.dataType),
            rightCountAttribute), "c")()
        }
      }
//      logWarning("join output: " + join.output)
      val finalProjection = join

      prevPlan = finalProjection
      prevCountExpr = finalCountExpr
      isLeafNode = false
      prevChildEdge = childEdge
      prevSemijoined = canSemiJoin
    }
    (prevPlan, prevCountExpr.toAttribute, isLeafNode, prevSemijoined)
  }
  def reroot: HTNode = {
    if (parent == null) {
      this
    }
    else {
//      logWarning("parent: " + parent)
      var current = this
      var newCurrent = this.copy(newParent = null)
      val root = newCurrent
      while (current.parent != null) {
        val p = current.parent
        logWarning("p: " + p)
        val newChild = p.copy(newChildren = p.children - current, newParent = null)
//        logWarning("new child: " + newChild)
        newCurrent.children += newChild
//        logWarning("c: " + current)
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
//    logWarning("aggAttributes: " + aggAttributes)
//    logWarning("nodeAttributes: " + nodeAttributes)
    if (aggAttributes subsetOf nodeAttributes) {
//      logWarning("found subset in:\n" + this)
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
//        logWarning("other")
    }
  }

  // Compute the equivalence classes
  while (combineEquivalenceClasses) {

  }

//  logWarning("equivalence classes: " + equivalenceClasses)

  for (equivalenceClass <- equivalenceClasses) {
    val attName = equivalenceClass.head.toString
    vertices.add(attName)
    vertexToAttributes.put(attName, equivalenceClass)
    for (equivAtt <- equivalenceClass) {
      attributeToVertex.put(equivAtt.exprId, attName)
    }
  }

//  logWarning("vertex to attribute mapping: " + vertexToAttributes)
//  logWarning("attribute to vertex mapping: " + attributeToVertex)

  var tableIndex = 1
  for (item <- items) {
//    logWarning("join item: " + item)

    val projectAttributes = item.outputSet
    val hyperedgeVertices = projectAttributes
      .map(att => attributeToVertex.getOrElse(att.exprId, ""))
      .filterNot(att => att.equals("")).toSet

    val hyperedge = new HGEdge(hyperedgeVertices, s"E${tableIndex}", item, attributeToVertex)
    tableIndex += 1
    edges.add(hyperedge)
  }

//  logWarning("hyperedges: " + edges)

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
//        logWarning("gyo edge: " + e)
        val supersets = gyoEdges.filter(o => o containsNotEqual e)
//        logWarning("supersets: " + supersets)

        // For each edge e, check if it is not contained in another edge
        if (supersets.isEmpty) {
          // Append the contained edges as children in the tree
          val containedEdges = gyoEdges.filter(o => (e contains o) && (e.name != o.name))
          val parentNode = treeNodes.getOrElse(e.name, new HTNode(Set(e), Set(), null))
          val childNodes = containedEdges
            .map(c => treeNodes.getOrElse(c.name, new HTNode(Set(c), Set(), null)))
            .toSet
//          logWarning("parentNode: " + parentNode)
          parentNode.children ++= childNodes
//          logWarning("subsets: " + childNodes)
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
  override def toString: String = {
    edges.map(edge => s"""${edge.name}(${edge.vertices.map(v => v.replace("#", "_"))
      .mkString(",")})""").mkString(",\n")
  }
}

