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

package org.apache.spark.sql.execution.joins

import scala.collection.JavaConverters._

import org.apache.spark.{PartitionEvaluator, PartitionEvaluatorFactory}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GenericInternalRow, JoinedRow, Predicate, Projection, RowOrdering, SpecificInternalRow, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, Complete, DeclarativeAggregate, Final, NoOp, Partial, PartialMerge}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution.{RowIterator, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{LongType, StructField, StructType}

class SortMergeCountJoinEvaluatorFactory(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    countLeft: Option[Expression],
    countRight: Option[Expression],
    aggregatesRight: Seq[AggregateExpression],
    groupRight: Seq[NamedExpression],
    output: Seq[Attribute],
    inMemoryThreshold: Int,
    spillThreshold: Int,
    numOutputRows: SQLMetric,
    spillSize: SQLMetric,
    onlyBufferFirstMatchedRow: Boolean)
    extends PartitionEvaluatorFactory[InternalRow, InternalRow] with Logging {
  override def createEvaluator(): PartitionEvaluator[InternalRow, InternalRow] =
    new SortMergeCountJoinEvaluator

  private class SortMergeCountJoinEvaluator extends PartitionEvaluator[InternalRow, InternalRow] {

    private def cleanupResources(): Unit = {
      IndexedSeq(left, right).foreach(_.cleanupResources())
    }
    private def createLeftKeyGenerator(): Projection =
      UnsafeProjection.create(leftKeys, left.output)

    private def createRightKeyGenerator(): Projection =
      UnsafeProjection.create(rightKeys, right.output)

    override def eval(
        partitionIndex: Int,
        inputs: Iterator[InternalRow]*): Iterator[InternalRow] = {
      assert(inputs.length == 2)
      val leftIter = inputs(0)
      val rightIter = inputs(1)

     logWarning("join output: " + output)

      val boundCondition: InternalRow => Boolean = {
        condition.map { cond =>
          Predicate.create(cond, left.output ++ right.output).eval _
        }.getOrElse {
          (r: InternalRow) => true
        }
      }

      // An ordering that can be used to compare keys from both sides.
      val keyOrdering = RowOrdering.createNaturalAscendingOrdering(leftKeys.map(_.dataType))
      val resultProj: InternalRow => InternalRow = UnsafeProjection.create(output, output)

      val leftCountOrdinal = AttributeSeq(left.output)
        .indexOf(countLeft.get.references.head.exprId)
      val rightCountOrdinal = AttributeSeq(right.output)
        .indexOf(countRight.get.references.head.exprId)

      val doAggregation = aggregatesRight.nonEmpty
      val doGrouping = groupRight.nonEmpty

      val aggregateFunctions = aggregatesRight.map(_.aggregateFunction).toArray

      // Aggregate functions can only be DeclarativeAggregates (Sum, Min, Max)
      val expressionAggInitialProjection = {
        val initExpressions = aggregateFunctions.flatMap {
          case ae: DeclarativeAggregate => ae.initialValues
        }
        MutableProjection.create(initExpressions, Nil)
      }

      val bufferSchema = aggregateFunctions.flatMap(_.aggBufferAttributes)
      val initialAggregationBuffer: UnsafeRow =
        UnsafeProjection.create(bufferSchema.map(_.dataType))
        .apply(new GenericInternalRow(bufferSchema.length))
      // Initialize declarative aggregates' buffer values
      expressionAggInitialProjection.target(initialAggregationBuffer)(EmptyRow)

      val evalExpressions = aggregateFunctions.map {
        case ae: DeclarativeAggregate => ae.evaluateExpression
        case agg: AggregateFunction => NoOp
      }

      val aggregateResult = new SpecificInternalRow(aggregatesRight.map(_.dataType))
      val expressionAggEvalProjection = MutableProjection.create(evalExpressions, bufferSchema)
      expressionAggEvalProjection.target(aggregateResult)

      val mergeExpressions =
        aggregateFunctions.zip(
          aggregatesRight.map(ae => (ae.mode, ae.isDistinct, ae.filter))).flatMap {
          case (ae: DeclarativeAggregate, (mode, isDistinct, filter)) =>
            mode match {
              case Partial | Complete =>
                if (filter.isDefined) {
                  ae.updateExpressions.zip(ae.aggBufferAttributes).map {
                    case (updateExpr, attr) => If(filter.get, updateExpr, attr)
                  }
                } else {
                  ae.updateExpressions
                }
              case PartialMerge | Final => ae.mergeExpressions
            }
          case (agg: AggregateFunction, _) => Seq.fill(agg.aggBufferAttributes.length)(NoOp)
        }

      val updateProjection =
        MutableProjection.create(mergeExpressions, bufferSchema ++ right.output)

      val joinedRow2 = new JoinedRow
      val joinedRow3 = new JoinedRow
      val aggRow = new JoinedRow

      val sumRowSchema = StructType(StructField("c", LongType) :: Nil)
      val groupingProjection: UnsafeProjection =
        UnsafeProjection.create(groupRight, right.output)

      val aggRowProjection: UnsafeProjection =
        UnsafeProjection.create(groupRight, right.output)

//      logWarning("agregate functions: " + aggregateFunctions.mkString("Array(", ", ", ")"))
//      logWarning("groupRight: " + groupRight)
//      logWarning("right output: " + right.output)
//      logWarning("left output: " + left.output)

      joinType match {
        // TODO remove other join types as they get ignored in the countjoin
        case _: InnerLike =>
          new RowIterator {
            private[this] var currentLeftRow: InternalRow = _
            private[this] val smjScanner = new SortMergeJoinScanner(
              createLeftKeyGenerator(),
              createRightKeyGenerator(),
              keyOrdering,
              RowIterator.fromScala(leftIter),
              RowIterator.fromScala(rightIter),
              inMemoryThreshold,
              spillThreshold,
              spillSize,
              cleanupResources)
            private[this] val joinRow = new JoinedRow

            var rightCountSum = 0L
            var buffer: InternalRow = null
            var sumMap: java.util.LinkedHashMap[UnsafeRow, Long] = null
            var bufferIterator: Iterator[(UnsafeRow, InternalRow)] = null

            override def advanceNext(): Boolean = {
              if (bufferIterator != null && bufferIterator.hasNext) {
                // If there are still rows in the iterator, return true, such that
                // these rows are fetched
                true
              }
              else {
                sumMap = new java.util.LinkedHashMap[UnsafeRow, Long]
//                sumMap = new mutable.LinkedHashMap[UnsafeRow, Long]

                while (smjScanner.findNextInnerJoinRows()) {
                  currentLeftRow = smjScanner.getStreamedRow
                  val currentRightMatches = smjScanner.getBufferedMatches

                  if (currentRightMatches != null && currentRightMatches.length > 0) {
                    val bufferMap = new java.util.LinkedHashMap[UnsafeRow, InternalRow]
                    if (!doGrouping) {
                      // In case we do not group, create one buffer and use it for all right matches
                      buffer = new SpecificInternalRow(bufferSchema.map(_.dataType))
                      expressionAggInitialProjection.target(buffer)(EmptyRow)
                    }

                    val rightMatchesIterator = currentRightMatches.generateIterator()

                    rightCountSum = 0L
                    while (rightMatchesIterator.hasNext) {
                      val rightRow = rightMatchesIterator.next()
                      joinRow(currentLeftRow, rightRow)
                      if (boundCondition(joinRow)) {
                        val rightCount = rightRow.getLong(rightCountOrdinal)
                        rightCountSum += rightCount
                        if (doAggregation) {
                          if (doGrouping) {
                            // We need to create a copy of the grouping key because rightRow changes
                            val groupingKey = groupingProjection(rightRow).copy()
                            var sum: Long = 0

//                            logWarning("rightrow: " + rightRow)
//                            logWarning("bufferMap: " + bufferMap + " groupingKey: " + groupingKey)
//                            logWarning("contains: " + bufferMap.containsKey(groupingKey))
                            if (bufferMap.containsKey(groupingKey)) {
                              buffer = bufferMap.get(groupingKey)
                              sum = sumMap.get(groupingKey)
                              sum += rightCount
                              sumMap.put(groupingKey, sum)
                            }
                            else {
                              // Initialize a new buffer
//                              logWarning("aggregate functions: " +
//                                aggregateFunctions.mkString("Array(", ", ", ")"))
//                              logWarning("aggregates: " + aggregatesRight)
//                              logWarning("bufferSchema: " +
//                                bufferSchema.mkString("Array(", ", ", ")"))
                              val useUnsafeBuffer = bufferSchema
                                .map(_.dataType).forall(UnsafeRow.isMutable)
//                              logWarning("useUnsafeBuffer: " + useUnsafeBuffer)
                              // buffer = new SpecificInternalRow(aggregatesRight.map(_.dataType))
                              buffer = new SpecificInternalRow(bufferSchema.map(_.dataType))
//                              logWarning("new buffer: " + buffer)
                              expressionAggInitialProjection.target(buffer)(EmptyRow)
//                              logWarning("buffer after projection: " + buffer +
//                                " , groupingKey: " + groupingKey)
                              bufferMap.put(groupingKey, buffer)
//                              logWarning("new bufferMap: " + bufferMap)
                              sum = rightCount
                              sumMap.put(groupingKey, sum)
                            }
                          }

//                          logWarning("bufferMap before update: " + bufferMap
//                            + ", buffer: " + buffer)
                          aggRow(buffer, rightRow)
//                          logWarning("aggregates: " + aggregatesRight)
//                          logWarning("buffer schema: " + aggregatesRight.map(_.dataType))
//                          logWarning("aggRow: " + aggRow)
//                          logWarning("buffer: " + buffer)
                          updateProjection.target(buffer)(aggRow)
//                          logWarning("bufferMap after update: " + bufferMap)
                        }
                        numOutputRows += 1
                      }
                    }
                    if (doGrouping) {
//                      logWarning("setting buffer iterator: " + bufferMap)
                      bufferIterator = bufferMap.asScala.iterator
                    }
                    return true
                  }
                }
                false
              }
            }

            val aggResultAttributes = aggregatesRight.map(_.resultAttribute)

            protected val aggProjection = UnsafeProjection.create(
              aggResultAttributes ++ groupRight,
              aggResultAttributes++ groupRight.map(_.toAttribute))

            protected val countAggGroupProjection = UnsafeProjection.create(
              Seq(countRight.get) ++ aggResultAttributes ++ groupRight,
              Seq(countRight.get.asInstanceOf[Attribute])
                ++ aggResultAttributes ++ groupRight.map(_.toAttribute))

            protected val resultProjection = UnsafeProjection.create(
              left.output ++ Seq(countRight.get) ++ aggResultAttributes ++ groupRight,
              left.output ++ Seq(countRight.get.asInstanceOf[Attribute])
                ++ aggResultAttributes ++ groupRight.map(_.toAttribute))

            logWarning("agg buffer atts: " + bufferSchema.mkString("Array(", ", ", ")"))
            logWarning("agg results: " + aggResultAttributes)
            logWarning("evaluate expressions: " + evalExpressions.mkString("Array(", ", ", ")"))
            logWarning("output types: " + (left.output ++
              Seq(countRight.get.asInstanceOf[Attribute])
              ++ aggResultAttributes ++ groupRight.map(_.toAttribute)).map(_.dataType))

            override def getRow: InternalRow = {
//              logWarning("getRow (doaggregation = " + doAggregation +
//                ", dogrouping = " + doGrouping)
//              logWarning("partition: " + partitionIndex)
//              logWarning("currentLeftRow: " + currentLeftRow)
//              logWarning("rightCountSum: " + rightCountSum)
              if (doGrouping) {
                val (groupingKey, buf) = bufferIterator.next()
//                logWarning("grouping key: " + groupingKey + ", buffer: " + buf)
                val sum = sumMap.get(groupingKey)
                expressionAggEvalProjection(buf)

//                logWarning("buffer: " + buf)
//                logWarning("aggregateResult: " + aggregateResult)

                val sumRow = new SpecificInternalRow(sumRowSchema)
                val leftCount = currentLeftRow.getLong(leftCountOrdinal)
                sumRow.setLong(0, sum * leftCount)

                val aggResult = aggProjection(joinedRow3(aggregateResult, groupingKey))
                joinRow.withRight(countAggGroupProjection(joinedRow2(sumRow, aggResult)))
//                logWarning("output: " + resultProjection(joinRow))

//                logWarning("resultProjection: " + resultProjection(joinRow))

                val outputAtts = left.output ++ Seq(countRight.get.asInstanceOf[Attribute]) ++
                  aggResultAttributes ++ groupRight.map(_.toAttribute)

                def printRow(ur: UnsafeRow): Unit = {
                  for ((att, i) <- outputAtts.zipWithIndex) {
                    logWarning("idx " + i + ": " + ur.get(i, att.dataType))
                  }
                }

//                printRow(resultProjection(joinRow))

                resultProjection(joinRow)
              }
              else {
//                logWarning("no grouping buffer: " + buffer)
                expressionAggEvalProjection(buffer)
                val sumRow = new SpecificInternalRow(sumRowSchema)
                val leftCount = currentLeftRow.getLong(leftCountOrdinal)
                sumRow.setLong(0, rightCountSum * leftCount)
                if (doAggregation) {
                  joinRow.withRight(countAggGroupProjection(joinedRow2(sumRow, aggregateResult)))
//                  logWarning("output: " + resultProjection(joinRow))
                  resultProjection(joinRow)
                }
                else {
                  joinRow.withRight(sumRow)
//                  logWarning("output: " + resultProjection(joinRow))
                  resultProjection(joinRow)
                }
              }
            }
          }.toScala

        case LeftOuter =>
          val smjScanner = new SortMergeJoinScanner(
            streamedKeyGenerator = createLeftKeyGenerator(),
            bufferedKeyGenerator = createRightKeyGenerator(),
            keyOrdering,
            streamedIter = RowIterator.fromScala(leftIter),
            bufferedIter = RowIterator.fromScala(rightIter),
            inMemoryThreshold,
            spillThreshold,
            spillSize,
            cleanupResources)
          val rightNullRow = new GenericInternalRow(right.output.length)
          new LeftOuterIterator(
            smjScanner,
            rightNullRow,
            boundCondition,
            resultProj,
            numOutputRows).toScala

        case RightOuter =>
          val smjScanner = new SortMergeJoinScanner(
            streamedKeyGenerator = createRightKeyGenerator(),
            bufferedKeyGenerator = createLeftKeyGenerator(),
            keyOrdering,
            streamedIter = RowIterator.fromScala(rightIter),
            bufferedIter = RowIterator.fromScala(leftIter),
            inMemoryThreshold,
            spillThreshold,
            spillSize,
            cleanupResources)
          val leftNullRow = new GenericInternalRow(left.output.length)
          new RightOuterIterator(
            smjScanner,
            leftNullRow,
            boundCondition,
            resultProj,
            numOutputRows).toScala

        case FullOuter =>
          val leftNullRow = new GenericInternalRow(left.output.length)
          val rightNullRow = new GenericInternalRow(right.output.length)
          val smjScanner = new SortMergeFullOuterJoinScanner(
            leftKeyGenerator = createLeftKeyGenerator(),
            rightKeyGenerator = createRightKeyGenerator(),
            keyOrdering,
            leftIter = RowIterator.fromScala(leftIter),
            rightIter = RowIterator.fromScala(rightIter),
            boundCondition,
            leftNullRow,
            rightNullRow)

          new FullOuterIterator(smjScanner, resultProj, numOutputRows).toScala

        case LeftSemi =>
          new RowIterator {
            private[this] var currentLeftRow: InternalRow = _
            private[this] val smjScanner = new SortMergeJoinScanner(
              createLeftKeyGenerator(),
              createRightKeyGenerator(),
              keyOrdering,
              RowIterator.fromScala(leftIter),
              RowIterator.fromScala(rightIter),
              inMemoryThreshold,
              spillThreshold,
              spillSize,
              cleanupResources,
              onlyBufferFirstMatchedRow)
            private[this] val joinRow = new JoinedRow

            override def advanceNext(): Boolean = {
              while (smjScanner.findNextInnerJoinRows()) {
                val currentRightMatches = smjScanner.getBufferedMatches
                currentLeftRow = smjScanner.getStreamedRow
                if (currentRightMatches != null && currentRightMatches.length > 0) {
                  val rightMatchesIterator = currentRightMatches.generateIterator()
                  while (rightMatchesIterator.hasNext) {
                    joinRow(currentLeftRow, rightMatchesIterator.next())
                    if (boundCondition(joinRow)) {
                      numOutputRows += 1
                      return true
                    }
                  }
                }
              }
              false
            }

            override def getRow: InternalRow = currentLeftRow
          }.toScala

        case LeftAnti =>
          new RowIterator {
            private[this] var currentLeftRow: InternalRow = _
            private[this] val smjScanner = new SortMergeJoinScanner(
              createLeftKeyGenerator(),
              createRightKeyGenerator(),
              keyOrdering,
              RowIterator.fromScala(leftIter),
              RowIterator.fromScala(rightIter),
              inMemoryThreshold,
              spillThreshold,
              spillSize,
              cleanupResources,
              onlyBufferFirstMatchedRow)
            private[this] val joinRow = new JoinedRow

            override def advanceNext(): Boolean = {
              while (smjScanner.findNextOuterJoinRows()) {
                currentLeftRow = smjScanner.getStreamedRow
                val currentRightMatches = smjScanner.getBufferedMatches
                if (currentRightMatches == null || currentRightMatches.length == 0) {
                  numOutputRows += 1
                  return true
                }
                var found = false
                val rightMatchesIterator = currentRightMatches.generateIterator()
                while (!found && rightMatchesIterator.hasNext) {
                  joinRow(currentLeftRow, rightMatchesIterator.next())
                  if (boundCondition(joinRow)) {
                    found = true
                  }
                }
                if (!found) {
                  numOutputRows += 1
                  return true
                }
              }
              false
            }

            override def getRow: InternalRow = currentLeftRow
          }.toScala

        case j: ExistenceJoin =>
          new RowIterator {
            private[this] var currentLeftRow: InternalRow = _
            private[this] val result: InternalRow = new GenericInternalRow(Array[Any](null))
            private[this] val smjScanner = new SortMergeJoinScanner(
              createLeftKeyGenerator(),
              createRightKeyGenerator(),
              keyOrdering,
              RowIterator.fromScala(leftIter),
              RowIterator.fromScala(rightIter),
              inMemoryThreshold,
              spillThreshold,
              spillSize,
              cleanupResources,
              onlyBufferFirstMatchedRow)
            private[this] val joinRow = new JoinedRow

            override def advanceNext(): Boolean = {
              while (smjScanner.findNextOuterJoinRows()) {
                currentLeftRow = smjScanner.getStreamedRow
                val currentRightMatches = smjScanner.getBufferedMatches
                var found = false
                if (currentRightMatches != null && currentRightMatches.length > 0) {
                  val rightMatchesIterator = currentRightMatches.generateIterator()
                  while (!found && rightMatchesIterator.hasNext) {
                    joinRow(currentLeftRow, rightMatchesIterator.next())
                    if (boundCondition(joinRow)) {
                      found = true
                    }
                  }
                }
                result.setBoolean(0, found)
                numOutputRows += 1
                return true
              }
              false
            }

            override def getRow: InternalRow = resultProj(joinRow(currentLeftRow, result))
          }.toScala

        case x =>
          throw new IllegalArgumentException(s"SortMergeJoin should not take $x as the JoinType")
      }

    }
  }
}
