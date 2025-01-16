
package org.apache.spark.sql

import org.apache.spark.SparkFunSuite
// import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.optimizer.RewriteJoinsAsSemijoins
import org.apache.spark.sql.catalyst.optimizer.RewriteJoinsAsSemijoins.isUnguarded
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, FKHint, Join, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.RuleIdCollection
import org.apache.spark.sql.catalyst.trees.TreePattern
// import org.scalatest.BeforeAndAfterAll

class RewriteJoinsAsSemijoinsSuite extends SparkFunSuite {

  // https://phanikumaryadavilli.medium.com/
  // writing-tests-for-your-spark-code-using-funsuite-71a554f92106
//  private val master = "local"
//  private val appName = "UnguardedTest"
//  private var spark: SparkSession = _

  // SQL Query
  val query1 =
    """
      |SELECT
      |    s_name AS Supplier_Name,
      |    SUM(l_extendedprice * (1 - l_discount)) AS Total_Revenue,
      |    AVG(l_discount) AS Avg_Discount
      |FROM
      |    lineitem
      |JOIN
      |    supplier
      |    ON lineitem.l_suppkey = supplier.s_suppkey
      |GROUP BY
      |    s_name
      |ORDER BY
      |    Total_Revenue DESC
      |""".stripMargin

  /*// Setup method to initialize SparkSession
  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName(appName)
      .master(master) // Use all available cores with local[*]
      .getOrCreate()
  }

  // Cleanup method to stop SparkSession
  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }*/

  // Helper method to return the LogicalPlan for a given query
  private def getLogicalPlan(sqlQuery: String): LogicalPlan = {
    // spark.sql(sqlQuery).queryExecution.logical
    CatalystSqlParser.parseQuery(sqlQuery)
  }

  test("get logical plan") {
    val plan = getLogicalPlan(query1)
    println(plan)
  }
  private def processQueryPlan(plan: LogicalPlan): Boolean = {
    val ruleId = RuleIdCollection.getRuleId(RewriteJoinsAsSemijoins.ruleName)

    plan.transformDownWithPruning(_.containsPattern(TreePattern.AGGREGATE), ruleId) {
      case agg@Aggregate(groupingExpressions, aggExpressions,
      join@Join(_, _, Inner, _, _)) => agg

      case agg@Aggregate(groupingExpressions, aggExpressions,
      filter@Filter(filterConds,
      join@Join(_, _, Inner, _, _))) => agg

      case agg@Aggregate(groupingExpressions, aggExpressions,
      project@Project(projectList,
      join@Join(_, _, Inner, _, _))) =>
        val unguarded = isUnguarded(plan, groupingExpressions, aggExpressions)
        if (unguarded) {
          logWarning("query unguarded. ")
          return true
        } else {
//          rewritePlan(agg, groupingExpressions, aggExpressions, projectList,
//            join, keyRefs = Seq(), uniqueConstraints = Seq())
          return false
        }
      // FK/PK optimizations (to be removed at some point)

      case agg@Aggregate(groupingExpressions, aggExpressions,
      project@Project(projectList,
      FKHint(join@Join(_, _, Inner, _, _), keyRefs, uniqueConstraints))) =>
        val unguarded = isUnguarded(plan, groupingExpressions, aggExpressions)
        if (unguarded) {
          logWarning("query unguarded. ")
          return true
        } else {
//          rewritePlan(agg, groupingExpressions, aggExpressions, projectList,
//            join, keyRefs, uniqueConstraints)
          return false
        }

      case agg@Aggregate(groupingExpressions, aggExpressions,
      project@Project(projectList,
      FKHint(
      project2@Project(projectList2,
      join@Join(_, _, Inner, _, _)), keyRefs, uniqueConstraints))) =>
        val unguarded = isUnguarded(plan, groupingExpressions, aggExpressions)
        if (unguarded) {
          logWarning("query unguarded. ")
          return true
        } else {
//          rewritePlan(agg, groupingExpressions, aggExpressions, projectList,
//            join, keyRefs, uniqueConstraints)
          return false
        }

      case agg@Aggregate(_, _, _) =>
        logWarning("not applicable to aggregate: " + agg)
        agg
    }
    return false
  }

  test("query1ShouldReturnTrue") {
    val plan = getLogicalPlan(query1)

    val unguarded = processQueryPlan(plan)

  }



}
