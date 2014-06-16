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
 *   - Database.forDataSource
 *   - Database.forUrl
 *   - Database.getConnection
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

package sqlest.executor

import sqlest.ast._
import sqlest.extractor._
import sqlest.sql._
import sqlest.util._

import java.sql.{ Connection, DriverManager, ResultSet, Statement, SQLException }
import javax.sql.DataSource
import java.util.Properties
import scala.util.DynamicVariable

object Database {

  def forDataSource(builder: StatementBuilder, dataSource: DataSource): Database = new Database {
    val statementBuilder = builder
    def getConnection: Connection = dataSource.getConnection
  }

  def forURL(builder: StatementBuilder, url: String, user: String = null, password: String = null, properties: Properties = null, driver: String = null) = new Database {
    val statementBuilder = builder

    if (driver != null) Class.forName(driver)

    val allProperties =
      if (properties != null && user == null && password == null) properties
      else {
        val allProperties = new Properties(properties)
        if (user != null) allProperties.setProperty("user", user)
        if (password != null) allProperties.setProperty("password", password)
        allProperties
      }

    def getConnection: Connection = DriverManager.getConnection(url, allProperties)
  }
}

trait Database extends Logging {
  protected def getConnection: Connection
  protected def statementBuilder: StatementBuilder

  private val transactionConnection = new DynamicVariable[Option[Connection]](None)

  def executeSelect[A](select: Select)(extractor: ResultSet => A): A =
    executeWithConnection { connection =>
      val preparedStatement = statementBuilder(connection, select)
      try {
        logger.debug(s"Executing select")
        val resultSet = preparedStatement.executeQuery

        try {
          logger.debug(s"Extracting results")
          extractor(resultSet)
        } finally {
          try {
            if (resultSet != null) resultSet.close
          } catch {
            case e: SQLException =>
          }
        }

      } finally {
        try {
          if (preparedStatement != null) preparedStatement.close
        } catch {
          case e: SQLException =>
        }
      }
    }

  def executeInsert(insert: Insert): Int = {
    checkInTransaction
    executeWithConnection { connection =>
      val preparedStatement = statementBuilder(connection, insert)
      try {
        logger.debug(s"Executing insert")
        preparedStatement.executeUpdate
      } finally {
        try {
          if (preparedStatement != null) preparedStatement.close
        } catch {
          case e: SQLException =>
        }
      }
    }
  }

  def executeUpdate(update: Update): Int = {
    checkInTransaction
    executeWithConnection { connection =>
      val preparedStatement = statementBuilder(connection, update)
      try {
        logger.debug(s"Executing update")
        preparedStatement.executeUpdate
      } finally {
        try {
          if (preparedStatement != null) preparedStatement.close
        } catch {
          case e: SQLException =>
        }
      }
    }
  }

  def executeDelete(delete: Delete): Int = {
    checkInTransaction
    executeWithConnection { connection =>
      0
    }
  }

  def executeBatch(batchCommands: Seq[Command]): List[Int] = {
    checkInTransaction
    executeWithConnection { connection =>
      val statement = connection.createStatement
      try {
        batchCommands foreach { command =>
          val commandSql = statementBuilder.generateRawSql(command)
          logger.debug(s"Adding batch operation: $commandSql")
          statement.addBatch(commandSql)
        }

        statement.executeBatch.toList
      } finally {
        try {
          if (statement != null) statement.close
        } catch {
          case e: SQLException =>
        }
      }
    }
  }

  private def executeWithConnection[A](thunk: Connection => A): A =
    transactionConnection.value match {
      case Some(connection) => thunk(connection)
      case None =>
        val connection = getConnection
        try {
          thunk(connection)
        } finally {
          try {
            if (connection != null) connection.close
          } catch {
            case e: SQLException =>
          }
        }
    }

  def withTransaction[A](transaction: => A): A =
    if (transactionConnection.value.isDefined)
      // Already inside transaction so just run thunk
      transaction
    else
      executeWithNewTransaction(transaction)

  private def executeWithNewTransaction[A](transaction: => A): A = {
    val connection = getConnection
    try {
      try {
        connection.setAutoCommit(false)
        transactionConnection.withValue(Some(connection)) {
          val result = transaction
          connection.commit
          result
        }
      } catch {
        case e: SQLException =>
          connection.rollback
          throw e
      }
    } finally {
      try {
        if (connection != null) {
          connection.setAutoCommit(true)
          connection.close
        }
      } catch {
        case e: SQLException =>
      }
    }
  }

  private def checkInTransaction =
    if (transactionConnection.value.isEmpty)
      throw new AssertionError("Must run write operations in a transaction")
}
