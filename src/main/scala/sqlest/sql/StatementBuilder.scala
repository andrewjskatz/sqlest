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

import java.sql.{ Connection, PreparedStatement, Timestamp => JdbcTimestamp, Types => JdbcTypes, SQLException }
import org.joda.time.DateTime
import sqlest.ast._
import sqlest.util._

trait StatementBuilder extends BaseStatementBuilder
    with SelectStatementBuilder
    with InsertStatementBuilder
    with UpdateStatementBuilder
    with DeleteStatementBuilder
    with Logging {

  def apply(connection: Connection, operation: Operation) =
    prepareStatement(connection, operation)

  def prepareStatement(connection: Connection, operation: Operation) = {
    val querySql = sql(operation)
    val queryParameters = parameters(operation)
    try {
      logger.debug(s"Preparing statement: $querySql - $queryParameters")
      val statement = connection.prepareStatement(querySql)

      setParameters(operation, statement, queryParameters)

      statement
    } catch {
      case exn: SQLException =>
        logger.error(s"Error preparing statement: $querySql - $queryParameters")
        throw exn
    }
  }

  def generateRawSql(operation: Operation): String = {
    val querySql = sql(operation).split("\\?")
    val queryParameters = parameters(operation).map(parameter => constantSql(parameter.columnType, parameter.value))

    querySql.zipAll(queryParameters, "", "")
      .map { case (sql, parameter) => sql + parameter }
      .mkString
  }

  def sql(operation: Operation): String = operation match {
    case select: Select => selectSql(select)
    case insert: Insert => insertSql(insert)
    case update: Update => updateSql(update)
    case delete: Delete => deleteSql(delete)
    case other => sys.error("Unsupported operation type: " + other)
  }

  def parameters(operation: Operation): List[LiteralColumn[_]] = operation match {
    case select: Select => selectArgs(select)
    case insert: Insert => insertArgs(insert)
    case update: Update => updateArgs(update)
    case delete: Delete => deleteArgs(delete)
    case other => sys.error("Unsupported operation type: " + other)
  }

  private def setParameters(operation: Operation, statement: PreparedStatement, parameters: List[LiteralColumn[_]]) = {

    def innerSetParameters(parameters: List[LiteralColumn[_]]) = {
      var index = 0
      parameters foreach { parameter =>
        index = index + 1 // prepared statement parameter indices are 1-based
        setParameter(statement, index, parameter.columnType, parameter.value)
      }
    }

    operation match {
      case insert: InsertValues => parameters.grouped(insert.columns.size) map {
        params =>
          innerSetParameters(params)
          statement.addBatch
      }
      case _: InsertFromSelect =>
        innerSetParameters(parameters)
        statement.addBatch
      case _ => innerSetParameters(parameters)
    }
  }

  private def setParameter[A](statement: PreparedStatement, index: Int, columnType: ColumnType[A], value: Any): Unit = columnType match {
    case BooleanColumnType => statement.setBoolean(index, value.asInstanceOf[Boolean])
    case IntColumnType => statement.setInt(index, value.asInstanceOf[Int])
    case LongColumnType => statement.setLong(index, value.asInstanceOf[Long])
    case DoubleColumnType => statement.setDouble(index, value.asInstanceOf[Double])
    case BigDecimalColumnType => statement.setBigDecimal(index, value.asInstanceOf[BigDecimal].bigDecimal)
    case StringColumnType => statement.setString(index, value.asInstanceOf[String])
    case DateTimeColumnType => statement.setTimestamp(index, new JdbcTimestamp(value.asInstanceOf[DateTime].getMillis))
    case optionType: OptionColumnType[_] =>
      val option = value.asInstanceOf[Option[_]]
      if (option.isEmpty) statement.setNull(index, jdbcType(optionType.baseType)) else setParameter(statement, index, optionType.baseType, value)
    case mappedType: MappedColumnType[A, _] => setParameter(statement, index, mappedType.baseType, mappedType.write(value.asInstanceOf[A]))
  }

  private def jdbcType[A](columnType: ColumnType[A]): Int = columnType match {
    case BooleanColumnType => JdbcTypes.BOOLEAN
    case IntColumnType => JdbcTypes.INTEGER
    case LongColumnType => JdbcTypes.INTEGER
    case DoubleColumnType => JdbcTypes.DOUBLE
    case BigDecimalColumnType => JdbcTypes.DECIMAL
    case StringColumnType => JdbcTypes.CHAR
    case DateTimeColumnType => JdbcTypes.TIMESTAMP
    case optionType: OptionColumnType[_] => jdbcType(optionType.baseType)
    case mappedType: MappedColumnType[_, _] => jdbcType(mappedType.baseType)
  }

}
