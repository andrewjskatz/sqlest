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

import org.scalatest._
import org.scalatest.matchers._
import sqlest._
import sqlest.ast._

class DB2StatementBuilderSpec extends BaseStatementBuilderSpec
    with SelectStatementBuilderSpec
    with InsertStatementBuilderSpec
    with UpdateStatementBuilderSpec
    with DeleteStatementBuilderSpec {
  val statementBuilder = DB2StatementBuilder

  // DB2-specific tests:

  "select with limit only" should "produce the right sql" in {
    sql {
      select
        .from(MyTable)
        .where(MyTable.col1 === 123)
        .limit(10)
    } should equal(
      s"""
       |select mytable.col1 as mytable_col1, mytable.col2 as mytable_col2
       |from mytable
       |where (mytable.col1 = ?)
       |fetch first 10 rows only
       """.trim.stripMargin.split(lineSeparator).mkString(" "),
      List(123)
    )
  }

  "select with offset only" should "produce the right sql" in {
    sql {
      select
        .from(MyTable)
        .where(MyTable.col1 === 123)
        .offset(20)
    } should equal(
      s"""
       |with subquery as
       |(select mytable.col1 as mytable_col1, mytable.col2 as mytable_col2, row_number() over () as rownum
         |from mytable
         |where (mytable.col1 = ?))
       |select mytable_col1, mytable_col2
       |from subquery
       |where rownum >= ?
       """.trim.stripMargin.split(lineSeparator).mkString(" "),
      List(123, 2 * 10 + 1)
    )
  }

  "select with limit and offset" should "produce the right sql" in {
    sql {
      select
        .from(MyTable)
        .where(MyTable.col1 === 123)
        .page(2, 10)
    } should equal(
      s"""
       |with subquery as
       |(select mytable.col1 as mytable_col1, mytable.col2 as mytable_col2, row_number() over () as rownum
         |from mytable
         |where (mytable.col1 = ?))
       |select mytable_col1, mytable_col2
       |from subquery
       |where rownum between ? and ?
       """.trim.stripMargin.split(lineSeparator).mkString(" "),
      List(123, 2 * 10 + 1, 2 * 10 + 10)
    )
  }

  "select with limit and offset and order" should "produce the right sql" in {
    sql {
      select
        .from(MyTable)
        .where(MyTable.col1 === 123)
        .order(MyTable.col1.asc)
        .page(2, 10)
    } should equal(
      s"""
       |with subquery as
       |(select mytable.col1 as mytable_col1, mytable.col2 as mytable_col2, row_number() over (order by mytable.col1) as rownum
         |from mytable
         |where (mytable.col1 = ?))
       |select mytable_col1, mytable_col2
       |from subquery
       |where rownum between ? and ?
       """.trim.stripMargin.split(lineSeparator).mkString(" "),
      List(123, 2 * 10 + 1, 2 * 10 + 10)
    )
  }

  "select with multiple placeholders" should "produce the right sql" in {
    sql {
      select(1.constant as "a", sum(2) as "b", (3.constant + 4.column) as "c")
        .from(TableOne innerJoin TableTwo on (5.column === 6 && 7.column =!= 8))
        .where(9.column === 10 && 11.column =!= 12.column)
        .order(13.column.asc, 14.column.desc)
        .page(15, 16)
    } should equal(
      s"""
       |with subquery as
       |(select 1 as a, sum(?) as b, (3 + ?) as c, row_number() over (order by ?, ? desc) as rownum
         |from (one inner join two on ((? = ?) and (? <> ?)))
         |where ((? = ?) and (? <> ?)))
       |select a, b, c
       |from subquery
       |where rownum between ? and ?
       """.trim.stripMargin.split(lineSeparator).mkString(" "),
      List(2, 4, 13, 14, 5, 6, 7, 8, 9, 10, 11, 12, 15 * 16 + 1, 15 * 16 + 16)
    )
  }

  "select table function" should "produce the right sql" in {
    sql {
      select
        .from(TableThree.outerJoin(TestTableFunction(TableThree.col3, "abc")))
    } should equal(
      s"""
       |select three.col3 as three_col3, three.col4 as three_col4, testTableFunction.col5 as testTableFunction_col5, testTableFunction.col6 as testTableFunction_col6
       |from (three outer join table(testTableFunction(three.col3, ?)) as testTableFunction)
       """.trim.stripMargin.split(lineSeparator).mkString(" "),
      List("abc")
    )
  }

}
