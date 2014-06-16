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

/*
 * Some source in this file was developed in the Slick framework:
 *   - UntypedColumnHelpers.likeEncode
 *
 * Copyright 2011-2012 Typesafe, Inc.
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright notice,
 *       this list of conditions and the following disclaimer.
 *
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */  

package sqlest.untyped.ast.syntax

import org.joda.time.DateTime
import scala.reflect.runtime.{ universe => ru }
import scala.language.implicitConversions
import scala.util.Try
import sqlest.ast._
import sqlest.untyped.ast._

class UntypedColumnHelpers {
  def stringArgument(arg: String) = Some(arg)
  def intArgument(arg: String) = Try(arg.toInt).toOption
  def longArgument(arg: String) = Try(arg.toLong).toOption
  def doubleArgument(arg: String) = Try(arg.toDouble).toOption
  def booleanArgument(arg: String) = arg.trim.toLowerCase match {
    case "true" => Some(true)
    case "false" => Some(false)
    case _ => None
  }
  def bigDecimalArgument(arg: String) = Try(BigDecimal(arg)).toOption
  def dateTimeArgument(arg: String) = Iso8601.unapply(arg)
  def mappedArgument[A](arg: String, columnType: ColumnType[A]): Option[A] = (columnType.typeTag match {
    case typeTag if typeTag == ru.typeTag[Int] => intArgument(arg)
    case typeTag if typeTag == ru.typeTag[Long] => longArgument(arg)
    case typeTag if typeTag == ru.typeTag[Double] => doubleArgument(arg)
    case typeTag if typeTag == ru.typeTag[BigDecimal] => bigDecimalArgument(arg)
    case typeTag if typeTag == ru.typeTag[Boolean] => booleanArgument(arg)
    case typeTag if typeTag == ru.typeTag[String] => stringArgument(arg)
    case typeTag if typeTag == ru.typeTag[DateTime] => dateTimeArgument(arg)
    case _ => sys.error(s"Untyped operators are not implemented for non-standard mapped types: $columnType")
  }).asInstanceOf[Option[A]]

  def infixExpression[A](op: String, left: Column[A], right: String, columnType: ColumnType[A]): Option[InfixFunctionColumn[Boolean]] = columnType match {
    case IntColumnType => intArgument(right).map(right => InfixFunctionColumn[Boolean](op, left, right))
    case LongColumnType => longArgument(right).map(right => InfixFunctionColumn[Boolean](op, left, right))
    case DoubleColumnType => doubleArgument(right).map(right => InfixFunctionColumn[Boolean](op, left, right))
    case BigDecimalColumnType => bigDecimalArgument(right).map(right => InfixFunctionColumn[Boolean](op, left, right))
    case BooleanColumnType => booleanArgument(right).map(right => InfixFunctionColumn[Boolean](op, left, right))
    case StringColumnType => stringArgument(right).map(right => InfixFunctionColumn[Boolean](op, left, right))
    case DateTimeColumnType => dateTimeArgument(right).map(right => InfixFunctionColumn[Boolean](op, left, right))
    case OptionColumnType(baseType) => infixExpression(op, left, right, baseType)
    case mappedColumnType: MappedColumnType[A, _] =>
      mappedArgument(right, columnType).map { right =>
        val mappedRight = mappedColumnType.write(right)
        InfixFunctionColumn[Boolean](op, left, LiteralColumn(mappedRight)(mappedColumnType.baseType))
      }
  }

  def likeExpression(left: Column[_], right: String, columnType: ColumnType[_], formatArgument: String => String): Option[InfixFunctionColumn[Boolean]] = columnType match {
    case StringColumnType => stringArgument(right).map(right => InfixFunctionColumn[Boolean]("like", left, formatArgument(right)))
    case OptionColumnType(baseType) => likeExpression(left, right, baseType, formatArgument)
    case _ => None
  }

  def likeEncode(str: String) = {
    val b = new StringBuilder
    for (c <- str) c match {
      case '%' | '_' | '^' => b append '^' append c
      case _ => b append c
    }
    b.toString
  }
}

trait ColumnSyntax {
  implicit class UntypedColumnOps(left: Column[_]) {
    val helpers = new UntypedColumnHelpers

    def untypedEq(right: String) = helpers.infixExpression("=", left, right, left.columnType)
    def untypedNe(right: String) = helpers.infixExpression("<>", left, right, left.columnType)
    def untypedGt(right: String) = helpers.infixExpression(">", left, right, left.columnType)
    def untypedLt(right: String) = helpers.infixExpression("<", left, right, left.columnType)
    def untypedGte(right: String) = helpers.infixExpression(">=", left, right, left.columnType)
    def untypedLte(right: String) = helpers.infixExpression("<=", left, right, left.columnType)

    def untypedContains(right: String): Option[InfixFunctionColumn[Boolean]] =
      helpers.likeExpression(left, right, left.columnType, str => s"%${helpers.likeEncode(right)}%")

    def untypedStartsWith(right: String): Option[InfixFunctionColumn[Boolean]] =
      helpers.likeExpression(left, right, left.columnType, str => s"${helpers.likeEncode(right)}%")

    def untypedEndsWith(right: String): Option[InfixFunctionColumn[Boolean]] =
      helpers.likeExpression(left, right, left.columnType, str => s"%${helpers.likeEncode(right)}")

    def untypedIsNull = Some(PostfixFunctionColumn[Boolean]("is null", left))
  }
}
