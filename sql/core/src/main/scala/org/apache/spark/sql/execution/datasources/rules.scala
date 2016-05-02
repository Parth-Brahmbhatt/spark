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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Cast}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.sources.{BaseRelation, HadoopFsRelation, InsertableRelation}
import org.apache.spark.sql.{AnalysisException, SQLContext, SaveMode}

/**
 * Try to replaces [[UnresolvedRelation]]s with [[ResolvedDataSource]].
 */
private[sql] class ResolveDataSource(sqlContext: SQLContext) extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case u: UnresolvedRelation if u.tableIdentifier.database.isDefined =>
      try {
        val resolved = ResolvedDataSource(
          sqlContext,
          userSpecifiedSchema = None,
          partitionColumns = Array(),
          provider = u.tableIdentifier.database.get,
          options = Map("path" -> u.tableIdentifier.table))
        val plan = LogicalRelation(resolved.relation)
        u.alias.map(a => Subquery(u.alias.get, plan)).getOrElse(plan)
      } catch {
        case e: ClassNotFoundException => u
        case e: Exception =>
          // the provider is valid, but failed to create a logical plan
          u.failAnalysis(e.getMessage)
      }
  }
}

/**
 * A rule to do various checks before inserting into or writing to a data source table.
 */
private[sql] case class PreWriteCheck(catalog: Catalog) extends (LogicalPlan => Unit) {
  def failAnalysis(msg: String): Unit = { throw new AnalysisException(msg) }

  def apply(plan: LogicalPlan): Unit = {
    plan.foreach {
      case i @ logical.InsertIntoTable(
        l @ LogicalRelation(t: InsertableRelation, _),
        partition, query, overwrite, ifNotExists, _, _) =>
        // Right now, we do not support insert into a data source table with partition specs.
        if (partition.nonEmpty) {
          failAnalysis(s"Insert into a partition is not allowed because $l is not partitioned.")
        } else {
          // Get all input data source relations of the query.
          val srcRelations = query.collect {
            case LogicalRelation(src: BaseRelation, _) => src
          }
          if (srcRelations.contains(t)) {
            failAnalysis(
              "Cannot insert overwrite into table that is also being read from.")
          } else {
            // OK
          }
        }

      case logical.InsertIntoTable(
        LogicalRelation(r: HadoopFsRelation, _), part, query, overwrite, _, _, _) =>
        // We need to make sure the partition columns specified by users do match partition
        // columns of the relation.
        val existingPartitionColumns = r.partitionColumns.fieldNames.toSet
        val specifiedPartitionColumns = part.keySet
        if (existingPartitionColumns != specifiedPartitionColumns) {
          failAnalysis(s"Specified partition columns " +
            s"(${specifiedPartitionColumns.mkString(", ")}) " +
            s"do not match the partition columns of the table. Please use " +
            s"(${existingPartitionColumns.mkString(", ")}) as the partition columns.")
        } else {
          // OK
        }

        PartitioningUtils.validatePartitionColumnDataTypes(
          r.schema, part.keySet.toArray, catalog.conf.caseSensitiveAnalysis)

        // Get all input data source relations of the query.
        val srcRelations = query.collect {
          case LogicalRelation(src: BaseRelation, _) => src
        }
        if (srcRelations.contains(r)) {
          failAnalysis(
            "Cannot insert overwrite into table that is also being read from.")
        } else {
          // OK
        }

      case logical.InsertIntoTable(l: LogicalRelation, _, _, _, _, _, _) =>
        // The relation in l is not an InsertableRelation.
        failAnalysis(s"$l does not allow insertion.")

      case logical.InsertIntoTable(t, _, _, _, _, _, _) =>
        if (!t.isInstanceOf[LeafNode] || t == OneRowRelation || t.isInstanceOf[LocalRelation]) {
          failAnalysis(s"Inserting into an RDD-based table is not allowed.")
        } else {
          // OK
        }

      case CreateTableUsingAsSelect(tableIdent, _, _, partitionColumns, mode, _, query) =>
        // When the SaveMode is Overwrite, we need to check if the table is an input table of
        // the query. If so, we will throw an AnalysisException to let users know it is not allowed.
        if (mode == SaveMode.Overwrite && catalog.tableExists(tableIdent)) {
          // Need to remove SubQuery operator.
          EliminateSubQueries(catalog.lookupRelation(tableIdent)) match {
            // Only do the check if the table is a data source table
            // (the relation is a BaseRelation).
            case l @ LogicalRelation(dest: BaseRelation, _) =>
              // Get all input data source relations of the query.
              val srcRelations = query.collect {
                case LogicalRelation(src: BaseRelation, _) => src
              }
              if (srcRelations.contains(dest)) {
                failAnalysis(
                  s"Cannot overwrite table $tableIdent that is also being read from.")
              } else {
                // OK
              }

            case _ => // OK
          }
        } else {
          // OK
        }

        PartitioningUtils.validatePartitionColumnDataTypes(
          query.schema, partitionColumns, catalog.conf.caseSensitiveAnalysis)

      case _ => // OK
    }
  }
}
