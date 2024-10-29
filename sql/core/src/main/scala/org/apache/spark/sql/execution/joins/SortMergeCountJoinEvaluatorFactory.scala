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

import org.apache.spark.{PartitionEvaluator, PartitionEvaluatorFactory, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GenericInternalRow, JoinedRow, Predicate, Projection, RowOrdering, SpecificInternalRow, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, Complete, DeclarativeAggregate, Final, NoOp, Partial, PartialMerge}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution.{RowIterator, SparkPlan, UnsafeFixedWidthAggregationMap}
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

//      logWarning("join output: " + output)

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

      val bufferAttributes = aggregateFunctions.flatMap(_.aggBufferAttributes)
      val aggregateResult = new SpecificInternalRow(aggregatesRight.map(_.dataType))
      val expressionAggEvalProjection = MutableProjection.create(evalExpressions, bufferAttributes)
      expressionAggEvalProjection.target(aggregateResult)

      val mergeExpressions =
        aggregateFunctions.zip(
          aggregatesRight.map(ae => (ae.mode, ae.isDistinct, ae.filter))).flatMap {
          case (ae: DeclarativeAggregate, (mode, isDistinct, filter)) =>
//            logWarning("mode: " + mode + ", agg: " + ae)
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

      // val aggregationBufferSchema = aggregateFunctions.flatMap(_.aggBufferAttributes)
      val aggregationBufferSchema = aggregateFunctions.flatMap(_.aggBufferAttributes)
//      logWarning("aggregate functions: " + aggregateFunctions.mkString("Array(", ", ", ")"))
//      logWarning("agg buffer schema: " + aggregationBufferSchema.mkString("Array(", ", ", ")"))
//      logWarning("merge expressions: " + mergeExpressions.mkString("Array(", ", ", ")"))
      val updateProjection =
        MutableProjection.create(mergeExpressions, aggregationBufferSchema ++ right.output)

      val joinedRow2 = new JoinedRow
      val aggRow = new JoinedRow

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

            if (doAggregation) {
              // Right now this is not really needed as we do not do grouping
              val hashMap = new UnsafeFixedWidthAggregationMap(
                initialAggregationBuffer,
                StructType.fromAttributes(aggregateFunctions.flatMap(_.aggBufferAttributes)),
                StructType.fromAttributes(groupRight.map(_.toAttribute)),
                TaskContext.get(),
                // 1024 * 16, // initial capacity
                1,
                TaskContext.get().taskMemoryManager().pageSizeBytes
              )

              val groupingProjection: UnsafeProjection =
                UnsafeProjection.create(groupRight, right.output)

              // No grouping - just get the same buffer each time
              val groupingKey = groupingProjection.apply(null)
              // buffer = hashMap.getAggregationBufferFromUnsafeRow(groupingKey)
              //  .asInstanceOf[InternalRow]

              // buffer = new GenericInternalRow(aggregationBufferSchema.length)
              // buffer = new SpecificInternalRow(aggregatesRight.map(_.dataType))
              val bufferSchema = aggregateFunctions.flatMap(_.aggBufferAttributes)
//              buffer = InterpretedSafeProjection.createProjection(bufferSchema)
//                .apply(new GenericInternalRow(bufferSchema.length))

            }

            override def advanceNext(): Boolean = {
              // logWarning("aggregate functions: " + aggregateFunctions.map(_.sql(false)))

              // Create new buffer
              buffer = new SpecificInternalRow(aggregatesRight.map(_.dataType))
              expressionAggInitialProjection.target(buffer)

              while (smjScanner.findNextInnerJoinRows()) {

                val currentRightMatches = smjScanner.getBufferedMatches
                currentLeftRow = smjScanner.getStreamedRow

                if (currentRightMatches != null && currentRightMatches.length > 0) {
                  val rightMatchesIterator = currentRightMatches.generateIterator()

                  rightCountSum = 0L
                  while (rightMatchesIterator.hasNext) {
                    val rightRow = rightMatchesIterator.next()
                    joinRow(currentLeftRow, rightRow)
                    if (boundCondition(joinRow)) {
                      rightCountSum += joinRow.getRight.getLong(rightCountOrdinal)
                      if (doAggregation) {
//                        logWarning("buffer: " + buffer)
//                        logWarning("joinRow: " + joinRow)
                        aggRow(buffer, rightRow)

                        // logWarning("updateProjection: " + updateProjection)
//                        logWarning("aggRow: " + aggRow)
                        updateProjection.target(buffer)(aggRow)
//                        logWarning("new buffer: " + buffer)
                      }
                      numOutputRows += 1
                    }
                  }
                  return true
                }
              }
              false
            }

            override def getRow: InternalRow = {
              val schema = StructType(StructField("c", LongType) :: Nil)
              val sumRow = new SpecificInternalRow(schema)
              val leftCount = currentLeftRow.getLong(leftCountOrdinal)
              sumRow.setLong(0, rightCountSum * leftCount)
              if (doAggregation) {
                joinRow.withRight(joinedRow2(sumRow, buffer))
              }
              else {
                joinRow.withRight(sumRow)
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
