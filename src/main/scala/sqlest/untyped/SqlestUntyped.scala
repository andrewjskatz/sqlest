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

package sqlest.untyped

import scala.language.experimental.macros
import sqlest.extractor._
import sqlest.untyped.ast.syntax.ColumnSyntax
import sqlest.untyped.extractor.syntax.NamedExtractSyntax
import sqlest.untyped.syntax.ColumnFinderSyntax

trait SqlestUntyped extends ColumnSyntax with ColumnFinderSyntax {
  def extractNamed[A](extractors: (String, Extractor[_])*): SingleExtractor[A] = macro NamedExtractSyntax.extractNamedImpl[A]

  val ColumnFinder = sqlest.untyped.ColumnFinder

  type ProductNames[A] = sqlest.untyped.ProductNames[A]
  val ProductNames = sqlest.untyped.ProductNames
}
