/*
 * Copyright 2014 JHC Systems Limited
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

package sqlest.sql

import sqlest.ast._

trait DB2StatementBuilder extends StatementBuilder {
  override def selectSql(select: Select): String = {
    val offset = select.offset getOrElse 0L
    if (offset > 0L) {
      rowNumberSelectSql(select, offset, select.limit)
    } else {
      super.selectSql(select)
    }
  }

  override def selectLimitSql(limit: Option[Long]): Option[String] =
    limit map (limit => s"fetch first $limit rows only")

  override def selectOffsetSql(offset: Option[Long]): Option[String] =
    None

  override def joinSql(relation: Relation): String = relation match {
    case tableFunction: TableFunction => "table(" + functionSql(tableFunction.tableName, tableFunction.parameterColumns: _*) + ") as " + identifierSql(tableFunction.tableAlias)
    case _ => super.joinSql(relation)
  }

  def rowNumberSelectSql(select: Select, offset: Long, limit: Option[Long]): String = {
    val subquery = Seq(
      s"${selectWhatSql(select.columns)}, row_number() over (${selectOrderSql(select.order) getOrElse ""}) as rownum",
      selectFromSql(select.from)
    ) ++ Seq(
        selectWhereSql(select.where),
        selectGroupBySql(select.groupBy)
      ).flatten mkString " "

    val what =
      select.columns map (col => identifierSql(col.columnAlias)) mkString ", "

    val bounds = limit
      .map(limit => s"rownum between ? and ?")
      .getOrElse(s"rownum >= ?")

    s"with subquery as ($subquery) select $what from subquery where $bounds"
  }

  override def selectArgs(select: Select): List[LiteralColumn[_]] = {
    val offset = select.offset getOrElse 0L
    if (offset > 0L) {
      rowNumberSelectArgs(select, offset, select.limit)
    } else {
      super.selectArgs(select)
    }
  }

  override def selectLimitArgs(limit: Option[Long]): List[LiteralColumn[_]] =
    Nil

  override def selectOffsetArgs(limit: Option[Long]): List[LiteralColumn[_]] =
    Nil

  def rowNumberSelectArgs(select: Select, offset: Long, limit: Option[Long]): List[LiteralColumn[_]] = {
    val subqueryArgs =
      selectWhatArgs(select.columns) ++
        selectOrderArgs(select.order) ++
        selectFromArgs(select.from) ++
        selectWhereArgs(select.where)

    val boundsArgs = limit
      .map(limit => List(LiteralColumn[Long](offset + 1), LiteralColumn[Long](offset + limit)))
      .getOrElse(List(LiteralColumn[Long](offset + 1)))

    subqueryArgs ++ boundsArgs
  }
}

object DB2StatementBuilder extends DB2StatementBuilder
