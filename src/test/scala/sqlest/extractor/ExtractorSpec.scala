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

package sqlest.extractor

import org.scalatest._
import org.scalatest.matchers._
import scala.language.existentials
import sqlest._

class ExtractorSpec extends FlatSpec with Matchers with CustomMatchers {
  import TestData._

  // HACK: These declarations have been hoisted out of the tests below due to a Scala bug
  // that prevented MappedExtractor being able to locate a TypeTag for them:
  case class Inner(b: Int, c: List[Int])
  case class Outer(a: Int, b: List[Inner])
  case class Flattened(a: Int, b: List[Int], c: List[Int])
  case class AggregateOnePointFive(one: One, str: String)

  "single case class extractor" should "extract appropriate data structures" in {
    val extractor = extractNamed[One](
      "a" -> TableOne.col1,
      "b" -> TableOne.col2
    )

    extractor.extractOne(testResultSet) should equal(Some(
      One(1, "a")
    ))

    extractor.extractAll(testResultSet) should equal(List(
      One(1, "a"),
      One(3, "c"),
      One(-1, "e")
    ))
  }

  "tuple extractor" should "extract appropriate data structures" in {
    val extractor = extract(TableOne.col1, TableOne.col2)

    extractor.extractOne(testResultSet) should equal(Some(
      (1, "a")
    ))

    extractor.extractAll(testResultSet) should equal(List(
      (1, "a"),
      (3, "c"),
      (-1, "e")
    ))
  }

  "aggregate case class extractor" should "extract appropriate data structures" in {
    val extractor = extractNamed[AggregateOneTwo](
      "one" -> extractNamed[One](
        "a" -> TableOne.col1,
        "b" -> TableOne.col2
      ),
      "two" -> extractNamed[Two](
        "a" -> TableTwo.col2,
        "b" -> TableTwo.col3
      )
    )

    extractor.extractOne(testResultSet) should equal(Some(
      AggregateOneTwo(One(1, "a"), Two("b", 2))
    ))

    extractor.extractAll(testResultSet) should equal(List(
      AggregateOneTwo(One(1, "a"), Two("b", 2)),
      AggregateOneTwo(One(3, "c"), Two("d", 4)),
      AggregateOneTwo(One(-1, "e"), Two("f", 6))
    ))
  }

  "list extractor" should "extract appropriate data structures" in {
    val extractor = extractNamed[AggregateOneTwo](
      "one" -> extractNamed[One](
        "a" -> TableOne.col1,
        "b" -> TableOne.col2
      ),
      "two" -> extractNamed[Two](
        "a" -> TableTwo.col2,
        "b" -> TableTwo.col3
      )
    )

    extractor.extractOne(testResultSet) should equal(Some(
      AggregateOneTwo(One(1, "a"), Two("b", 2))
    ))

    extractor.extractAll(testResultSet) should equal(List(
      AggregateOneTwo(One(1, "a"), Two("b", 2)),
      AggregateOneTwo(One(3, "c"), Two("d", 4)),
      AggregateOneTwo(One(-1, "e"), Two("f", 6))
    ))
  }

  "option column types" should "handle nulls" in {
    val extractor = extractNamed[AggregateOneTwoThree](
      "one" -> extractNamed[One](
        "a" -> TableOne.col1,
        "b" -> TableOne.col2
      ),
      "two" -> extractNamed[Two](
        "a" -> TableTwo.col2,
        "b" -> TableTwo.col3
      ),
      "three" -> extractNamed[Three](
        "a" -> TableThree.col3,
        "b" -> TableThree.col4
      )
    )

    extractor.extractOne(testResultSet) should equal(Some(
      AggregateOneTwoThree(One(1, "a"), Two("b", 2), Three(None, Some("x")))
    ))

    extractor.extractAll(testResultSet) should equal(List(
      AggregateOneTwoThree(One(1, "a"), Two("b", 2), Three(None, Some("x"))),
      AggregateOneTwoThree(One(3, "c"), Two("d", 4), Three(Some(9), None)),
      AggregateOneTwoThree(One(-1, "e"), Two("f", 6), Three(None, None))
    ))
  }

  "option extractor" should "handle nulls" in {
    def results = TestResultSet(TableOne.columns)(
      Seq(1, "a"),
      Seq(3, "c"),
      Seq(null, "e")
    )

    val extractor =
      TableOne.col1.asOption

    extractor.extractOne(results) should equal(Some(
      Some(1)
    ))

    extractor.extractAll(results) should equal(List(
      Some(1),
      Some(3),
      None
    ))
  }

  "option extractor" should "handle nulls for compound tuples" in {
    val extractor = extract(TableThree.col3, TableThree.col4).asOption

    extractor.extractOne(testResultSet) should equal(Some(
      Some((None, Some("x")))
    ))

    extractor.extractAll(testResultSet) should equal(List(
      Some((None, Some("x"))),
      Some((Some(9), None)),
      Some((None, None))
    ))
  }

  "option extractor" should "handle nulls for compound values" in {
    val extractor = extractNamed[Three](
      "a" -> TableThree.col3,
      "b" -> TableThree.col4
    ).asOption

    extractor.extractOne(testResultSet) should equal(Some(
      Some(Three(None, Some("x")))
    ))

    extractor.extractAll(testResultSet) should equal(List(
      Some(Three(None, Some("x"))),
      Some(Three(Some(9), None)),
      Some(Three(None, None))
    ))
  }

  "option extractor" should "handle nulls for nested compound values" in {
    val extractor = extractNamed[AggregateOneTwoOptionThree](
      "one" -> extractNamed[One](
        "a" -> TableOne.col1,
        "b" -> TableOne.col2
      ),
      "two" -> extractNamed[Two](
        "a" -> TableTwo.col2,
        "b" -> TableTwo.col3
      ),
      "three" -> extractNamed[Three](
        "a" -> TableThree.col3,
        "b" -> TableThree.col4
      ).asOption
    )

    extractor.extractOne(testResultSet) should equal(Some(
      AggregateOneTwoOptionThree(One(1, "a"), Two("b", 2), Some(Three(None, Some("x"))))
    ))

    extractor.extractAll(testResultSet) should equal(List(
      AggregateOneTwoOptionThree(One(1, "a"), Two("b", 2), Some(Three(None, Some("x")))),
      AggregateOneTwoOptionThree(One(3, "c"), Two("d", 4), Some(Three(Some(9), None))),
      AggregateOneTwoOptionThree(One(-1, "e"), Two("f", 6), Some(Three(None, None)))
    ))
  }

  "column and extractor" should "be usable together as arguments to `extractor`" in {
    val extractor = extractNamed[AggregateOnePointFive](
      "one" -> extractNamed[One](
        "a" -> TableOne.col1,
        "b" -> TableOne.col2
      ),
      "str" -> TableTwo.col2
    )

    extractor.extractOne(testResultSet) should equal(Some(
      AggregateOnePointFive(One(1, "a"), "b")
    ))

    extractor.extractAll(testResultSet) should equal(List(
      AggregateOnePointFive(One(1, "a"), "b"),
      AggregateOnePointFive(One(3, "c"), "d"),
      AggregateOnePointFive(One(-1, "e"), "f")
    ))
  }

  "mapped columns" should "extract the correct type" in {
    def results = TestResultSet(TableFour.columns)(
      Seq("Y"),
      Seq("N")
    )

    val extractor = extract(TableFour.mapped)

    extractor.extractOne(results) should equal(Some(true))

    extractor.extractAll(results) should equal(List(true, false))
  }

  "nested list extractor" should "stop when the left value changes" in {
    def results = TestResultSet(TableOne.columns ++ TableTwo.columns)(
      Seq(1, "a", "a", 1),
      Seq(1, "a", "b", 2),
      Seq(2, "b", "c", 3)
    )

    val extractor = extract(
      extractNamed[One](
        "a" -> TableOne.col1,
        "b" -> TableOne.col2
      ),
      extractNamed[Two](
        "a" -> TableTwo.col2,
        "b" -> TableTwo.col3
      ).asList
    ).groupBy(extract(
        TableOne.col1,
        TableOne.col2
      ))

    extractor.extractOne(results) should equal(Some(
      (One(1, "a"), List(Two("a", 1), Two("b", 2)))
    ))

    extractor.extractAll(results) should equal(List(
      (One(1, "a"), List(Two("a", 1), Two("b", 2))),
      (One(2, "b"), List(Two("c", 3)))
    ))
  }

  "nested list extractor" should "handle columns with nullable fields" in {
    object TableThreeB extends TableThree(Some("b"))

    def results = TestResultSet(TableThree.columns ++ TableThreeB.columns)(
      Seq(1, null, 4, null),
      Seq(1, null, null, null),
      Seq(null, "b", 6, null)
    )

    val extractor = extract(
      extractNamed[Three](
        "a" -> TableThree.col3,
        "b" -> TableThree.col4
      ),
      extractNamed[Three](
        "a" -> TableThreeB.col3,
        "b" -> TableThreeB.col4
      ).asList
    ).groupBy(TableThree.col3)

    extractor.extractOne(results) should equal(Some(
      (Three(Some(1), None), List(Three(Some(4), None), Three(None, None)))
    ))

    extractor.extractAll(results) should equal(List(
      (Three(Some(1), None), List(Three(Some(4), None), Three(None, None))),
      (Three(None, Some("b")), List(Three(Some(6), None)))
    ))
  }

  "nested list extractor" should "be composable with option extractor" in {
    object TableThreeB extends TableThree(Some("b"))

    def results = TestResultSet(TableThree.columns ++ TableThreeB.columns)(
      Seq(1, null, 4, null),
      Seq(1, null, null, null),
      Seq(null, "b", 6, null)
    )

    val extractor = extract(
      extractNamed[Three](
        "a" -> TableThree.col3,
        "b" -> TableThree.col4
      ),
      extractNamed[Three](
        "a" -> TableThreeB.col3,
        "b" -> TableThreeB.col4
      ).asOption.asList
    ).groupBy(TableThree.col3)

    extractor.extractOne(results) should equal(Some(
      (Three(Some(1), None), List(Some(Three(Some(4), None)), Some(Three(None, None))))
    ))

    extractor.extractAll(results) should equal(List(
      (Three(Some(1), None), List(Some(Three(Some(4), None)), Some(Three(None, None)))),
      (Three(None, Some("b")), List(Some(Three(Some(6), None))))
    ))
  }

  "list extractor" should "work as peers within a tuple extractor" in {
    class TestTable(alias: Option[String]) extends Table("n", alias) {
      val col1 = column[Int]("col1")
      val col2 = column[Int]("col2")
      val col3 = column[Int]("col3")
    }

    object TestTable extends TestTable(None)

    import TestTable._

    def results = TestResultSet(List(col1, col2, col3))(
      Seq(1, 1, 1),
      Seq(1, 1, 2),
      Seq(1, 2, 3),
      Seq(1, 2, 4),
      Seq(2, 3, 5),
      Seq(2, 3, 6),
      Seq(2, 4, 7),
      Seq(2, 4, 8)
    )

    val extractor = extract(
      col1,
      col2.asList,
      col3.asList
    ).groupBy(col1)

    extractor.extractOne(results) should equal(Some(
      (
        1,
        List(1, 1, 2, 2),
        List(1, 2, 3, 4)
      )
    ))

    extractor.extractAll(results) should equal(List(
      (
        1,
        List(1, 1, 2, 2),
        List(1, 2, 3, 4)
      ),
      (
        2,
        List(3, 3, 4, 4),
        List(5, 6, 7, 8)
      )
    ))
  }

  "list extractor" should "work as peers within a mapped extractor" in {
    class TestTable(alias: Option[String]) extends Table("n", alias) {
      val col1 = column[Int]("col1")
      val col2 = column[Int]("col2")
      val col3 = column[Int]("col3")
    }

    object TestTable extends TestTable(None)

    import TestTable._

    def results = TestResultSet(List(col1, col2, col3))(
      Seq(1, 1, 1),
      Seq(1, 1, 2),
      Seq(1, 2, 3),
      Seq(1, 2, 4),
      Seq(2, 3, 5),
      Seq(2, 3, 6),
      Seq(2, 4, 7),
      Seq(2, 4, 8)
    )

    val extractor = extractNamed[Flattened](
      "a" -> col1,
      "b" -> col2.asList,
      "c" -> col3.asList
    ).groupBy(col1)

    extractor.extractOne(results) should equal(Some(
      Flattened(
        1,
        List(1, 1, 2, 2),
        List(1, 2, 3, 4)
      )
    ))

    extractor.extractAll(results) should equal(List(
      Flattened(
        1,
        List(1, 1, 2, 2),
        List(1, 2, 3, 4)
      ),
      Flattened(
        2,
        List(3, 3, 4, 4),
        List(5, 6, 7, 8)
      )
    ))
  }

  "list extractor" should "work in a nested left join without groupBy" in {
    class TestTable(alias: Option[String]) extends Table("n", alias) {
      val col1 = column[Int]("col1")
      val col2 = column[Int]("col2")
      val col3 = column[Int]("col3")
    }

    object TestTable extends TestTable(None)

    import TestTable._

    def results = TestResultSet(List(col1, col2, col3))(
      Seq(1, 1, 1),
      Seq(1, 1, 2),
      Seq(1, 2, 3),
      Seq(1, 2, 4),
      Seq(2, 3, 5),
      Seq(2, 3, 6),
      Seq(2, 4, 7),
      Seq(2, 4, 8)
    )

    val extractor = extractNamed[Outer](
      "a" -> col1,
      "b" -> extractNamed[Inner](
        "b" -> col2,
        "c" -> col3.asList
      ).asList
    )

    extractor.extractOne(results) should equal(Some(
      Outer(1, List(Inner(1, List(1))))
    ))

    extractor.extractAll(results) should equal(List(
      Outer(1, List(Inner(1, List(1)))),
      Outer(1, List(Inner(1, List(2)))),
      Outer(1, List(Inner(2, List(3)))),
      Outer(1, List(Inner(2, List(4)))),
      Outer(2, List(Inner(3, List(5)))),
      Outer(2, List(Inner(3, List(6)))),
      Outer(2, List(Inner(4, List(7)))),
      Outer(2, List(Inner(4, List(8))))
    ))
  }

  "list extractor" should "work in a nested left join with outer groupBy" in {
    class TestTable(alias: Option[String]) extends Table("n", alias) {
      val col1 = column[Int]("col1")
      val col2 = column[Int]("col2")
      val col3 = column[Int]("col3")
    }

    object TestTable extends TestTable(None)

    import TestTable._

    def results = TestResultSet(List(col1, col2, col3))(
      Seq(1, 1, 1),
      Seq(1, 1, 2),
      Seq(1, 2, 3),
      Seq(1, 2, 4),
      Seq(2, 3, 5),
      Seq(2, 3, 6),
      Seq(2, 4, 7),
      Seq(2, 4, 8)
    )

    val extractor = extractNamed[Outer](
      "a" -> col1,
      "b" -> extractNamed[Inner](
        "b" -> col2,
        "c" -> col3.asList
      ).asList
    ).groupBy(col1)

    extractor.extractOne(results) should equal(Some(Outer(1, List(
      Inner(1, List(1)),
      Inner(1, List(2)),
      Inner(2, List(3)),
      Inner(2, List(4))))))

    extractor.extractAll(results) should equal(List(
      Outer(
        1,
        List(
          Inner(1, List(1)),
          Inner(1, List(2)),
          Inner(2, List(3)),
          Inner(2, List(4)))),
      Outer(
        2,
        List(
          Inner(3, List(5)),
          Inner(3, List(6)),
          Inner(4, List(7)),
          Inner(4, List(8))))))
  }

  "list extractor" should "return empty list for null rows" in {
    class TestTable(alias: Option[String]) extends Table("n", alias) {
      val col1 = column[Int]("col1")
      val col2 = column[Int]("col2")
      val col3 = column[Int]("col3")
    }

    object TestTable extends TestTable(None)

    import TestTable._

    def results = TestResultSet(List(col1, col2, col3))(
      Seq(1, 1, null),
      Seq(1, 2, null),
      Seq(2, 3, null),
      Seq(2, 4, null)
    )

    val extractor = extract(
      col1,
      col2.asList,
      col3.asList
    ).groupBy(col1)

    extractor.extractOne(results) should equal(Some(
      (1, List(1, 2), List())
    ))

    extractor.extractAll(results) should equal(List(
      (1, List(1, 2), List()),
      (2, List(3, 4), List())
    ))
  }

  "scalar function extractor" should "extract value" in {
    val aliasedScalarFunctionColumn = sqlest.ast.AliasColumn[Int](null, "scalarFunction")

    def results = TestResultSet(Seq(aliasedScalarFunctionColumn))(
      Seq(10)
    )

    val extractor = extractColumnByName[Int]("scalarFunction")

    extractor.extractOne(results) should equal(Some(10))

    extractor.extractAll(results) should equal(List(10))
  }

  "scalar function extractor" should "compose with other extractors" in {
    val aliasedScalarFunctionColumn = sqlest.ast.AliasColumn[Int](null, "scalarFunction")

    def results = TestResultSet(TableThree.columns ++ Seq(aliasedScalarFunctionColumn))(
      Seq(1, "b", 10)
    )

    val extractor = extract(
      TableThree.col3,
      TableThree.col4,
      extractColumnByName[Int]("scalarFunction").asOption
    )

    extractor.extractOne(results) should equal(Some(
      (Some(1), Some("b"), Some(10))
    ))

    extractor.extractAll(results) should equal(List(
      (Some(1), Some("b"), Some(10))
    ))
  }

}
