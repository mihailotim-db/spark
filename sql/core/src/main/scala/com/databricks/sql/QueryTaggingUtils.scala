/*
 * DATABRICKS CONFIDENTIAL & PROPRIETARY
 * __________________
 *
 * Copyright 2025-present Databricks, Inc.
 * All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains the property of Databricks, Inc.
 * and its suppliers, if any.  The intellectual and technical concepts contained herein are
 * proprietary to Databricks, Inc. and its suppliers and may be covered by U.S. and foreign Patents,
 * patents in process, and are protected by trade secret and/or copyright law. Dissemination, use,
 * or reproduction of this information is strictly forbidden unless prior written permission is
 * obtained from Databricks, Inc.
 *
 * If you view or obtain a copy of this information and believe Databricks, Inc. may not have
 * intended it to be made available, please promptly report it to Databricks Legal Department
 * @ legal@databricks.com.
 */
package com.databricks.sql

import java.util.IdentityHashMap

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Command => LogicalCommand, LogicalPlan}
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, CreateDataSourceTableCommand}
import org.apache.spark.sql.types.{DataType, NullType}

/**
 * Utility functions for query tagging.
 */
object QueryTaggingUtils {

  /** Result of coalesced tag detection. */
  final case class TagDetection(collation: Boolean, geography: Boolean, geometry: Boolean)

  /**
   * Whether the given analyzed plan satisfies the data type or the expression predicate.
   */
  def planSatisfies(
      analyzedPlan: LogicalPlan,
      dataTypePredicate: DataType => Boolean,
      expressionPredicate: Expression => Boolean): Boolean = {
    if (analyzedPlan == null || !analyzedPlan.resolved) return false

    // Use identity-based visited sets to avoid re-traversing shared subtrees and subqueries.
    val visitedPlans = new IdentityHashMap[LogicalPlan, java.lang.Boolean]()
    val visitedExprs = new IdentityHashMap[Expression, java.lang.Boolean]()

    planSatisfiesFast(
      analyzedPlan,
      dataTypePredicate,
      expressionPredicate,
      visitedPlans,
      visitedExprs)
  }

  /**
   * Optimized traversal that:
   *  - only checks schema on leaf plans (to avoid O(nodes * width) scans), and
   *  - memoizes visited plans/expressions to avoid duplicate work across shared subtrees.
   */
  private def planSatisfiesFast(
      plan: LogicalPlan,
      dataTypePredicate: DataType => Boolean,
      expressionPredicate: Expression => Boolean,
      visitedPlans: IdentityHashMap[LogicalPlan, java.lang.Boolean],
      visitedExprs: IdentityHashMap[Expression, java.lang.Boolean]): Boolean = {
    if (visitedPlans.containsKey(plan)) return false
    visitedPlans.put(plan, java.lang.Boolean.TRUE)

    // Manually check (some) commands first, as they may embed schemas or subplans.
    plan match {
      case command: LogicalCommand
          if commandSatisfies(command, dataTypePredicate, expressionPredicate) =>
        return true
      case _ =>
    }

    // Check schema only on leaf nodes to avoid repeated full schema scans across the tree.
    if (plan.children.isEmpty && dataTypePredicate(plan.schema)) return true

    // Check all expressions in this node; short-circuit on first match.
    val exprIter = plan.expressions.iterator
    while (exprIter.hasNext) {
      if (expressionSatisfiesFast(
          exprIter.next(), dataTypePredicate, expressionPredicate, visitedPlans, visitedExprs)) {
        return true
      }
    }

    // Recurse into children plans.
    val childIter = plan.children.iterator
    while (childIter.hasNext) {
      if (planSatisfiesFast(
          childIter.next(), dataTypePredicate, expressionPredicate, visitedPlans, visitedExprs)) {
        return true
      }
    }

    false
  }

  /**
   * Whether the given expression satisfies the data type or the expression predicate.
   */
  def expressionSatisfies(
      expr: Expression,
      dataTypePredicate: DataType => Boolean,
      expressionPredicate: Expression => Boolean): Boolean = {
    val visitedPlans = new IdentityHashMap[LogicalPlan, java.lang.Boolean]()
    val visitedExprs = new IdentityHashMap[Expression, java.lang.Boolean]()
    expressionSatisfiesFast(
      expr, dataTypePredicate, expressionPredicate, visitedPlans, visitedExprs)
  }

  private def expressionSatisfiesFast(
      expr: Expression,
      dataTypePredicate: DataType => Boolean,
      expressionPredicate: Expression => Boolean,
      visitedPlans: IdentityHashMap[LogicalPlan, java.lang.Boolean],
      visitedExprs: IdentityHashMap[Expression, java.lang.Boolean]): Boolean = {
    if (visitedExprs.containsKey(expr)) return false
    visitedExprs.put(expr, java.lang.Boolean.TRUE)

    expr match {
      case subquery: SubqueryExpression =>
        planSatisfiesFast(subquery.plan, dataTypePredicate, expressionPredicate,
          visitedPlans, visitedExprs)
      case alias: Alias if alias.child.isInstanceOf[SubqueryExpression] =>
        planSatisfiesFast(
          alias.child.asInstanceOf[SubqueryExpression].plan,
          dataTypePredicate,
          expressionPredicate,
          visitedPlans,
          visitedExprs)
      case other =>
        if (expressionSatisfiesInternal(other, dataTypePredicate, expressionPredicate)) {
          true
        } else {
          val it = other.children.iterator
          var found = false
          while (!found && it.hasNext) {
            found = expressionSatisfiesFast(
              it.next(), dataTypePredicate, expressionPredicate, visitedPlans, visitedExprs)
          }
          found
        }
    }
  }

  private def expressionSatisfiesInternal(
      expr: Expression,
      dataTypePredicate: DataType => Boolean,
      expressionPredicate: Expression => Boolean): Boolean = {
    try {
      expressionPredicate(expr) || dataTypePredicate(expr.dataType)
    } catch {
      // Some expressions may not have a valid data type, so we catch `dataType` exceptions here.
      // The list is not exhaustive, but most common exceptions include: UnresolvedException,
      // QueryCompilationErrors.dataTypeOperationUnsupportedError, SparkException.internalError,
      // and SparkUnsupportedOperationException. We catch all of these exceptions just to be safe.
      case _ =>
        false
    }
  }

  private def commandSatisfies(
      command: LogicalCommand,
      dataTypePredicate: DataType => Boolean,
      expressionPredicate: Expression => Boolean): Boolean = {
    command match {
      case cmd: CreateDataSourceTableCommand =>
        dataTypePredicate(cmd.table.schema)
      case cmd: CreateDataSourceTableAsSelectCommand =>
        planSatisfies(cmd.query, dataTypePredicate, expressionPredicate)
      case _ =>
        false
    }
  }

  /**
   * Coalesced single-pass detector for Collation/Geography/Geometry tags.
   * Performs one traversal with memoization and returns which tags should be set.
   */
  def detectTags(analyzedPlan: LogicalPlan): TagDetection = {
    if (analyzedPlan == null || !analyzedPlan.resolved) return TagDetection(false, false, false)

    val visitedPlans = new IdentityHashMap[LogicalPlan, java.lang.Boolean]()
    val visitedExprs = new IdentityHashMap[Expression, java.lang.Boolean]()

    var hasCollation = false
    var hasGeography = false
    var hasGeometry = false

    def dataTypeHasAny(dt: DataType): Boolean = {
      if (!hasCollation && CollationUtils.dataTypePredicate(dt)) {
        hasCollation = true
      }
      if (!hasGeography && GeoSpatialTypeUtils.geographyTypePredicate(dt)) {
        hasGeography = true
      }
      if (!hasGeometry && GeoSpatialTypeUtils.geometryTypePredicate(dt)) {
        hasGeometry = true
      }
      hasCollation || hasGeography || hasGeometry
    }

    def exprHasAny(expr: Expression): Boolean = {
      if (visitedExprs.containsKey(expr)) return false
      visitedExprs.put(expr, java.lang.Boolean.TRUE)

      // Collation expressions are positive regardless of datatype checks.
      if (!hasCollation && CollationUtils.expressionPredicate(expr)) {
        hasCollation = true
      }

      expr match {
        case subq: SubqueryExpression =>
          planHasAny(subq.plan)
        case a: Alias if a.child.isInstanceOf[SubqueryExpression] =>
          planHasAny(a.child.asInstanceOf[SubqueryExpression].plan)
        case other =>
          if (dataTypeHasAny(safeDataType(other))) return true
          val it = other.children.iterator
          var found = false
          while (!found && it.hasNext) {
            found = exprHasAny(it.next())
          }
          found
      }
    }

    def safeDataType(e: Expression): DataType = {
      try e.dataType catch { case _ : Throwable => NullType }
    }

    def planHasAny(plan: LogicalPlan): Boolean = {
      if (visitedPlans.containsKey(plan)) return false
      visitedPlans.put(plan, java.lang.Boolean.TRUE)

      // Commands may embed schemas or subplans
      plan match {
        case cmd: org.apache.spark.sql.execution.command.CreateDataSourceTableCommand =>
          if (dataTypeHasAny(cmd.table.schema)) return true
        case cmd: org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand =>
          if (planHasAny(cmd.query)) return true
        case _ =>
      }

      // Check schema only on leaves to avoid repeated full scans on every node
      if (plan.children.isEmpty && dataTypeHasAny(plan.schema)) return true

      val exprIt = plan.expressions.iterator
      var matched = false
      while (!matched && exprIt.hasNext) {
        matched = exprHasAny(exprIt.next())
      }
      if (matched) return true

      val childIt = plan.children.iterator
      while (!matched && childIt.hasNext) {
        matched = planHasAny(childIt.next())
      }
      matched
    }

    planHasAny(analyzedPlan)
    TagDetection(hasCollation, hasGeography, hasGeometry)
  }
}
