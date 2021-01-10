/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2020 Schweizerische Bundesbahnen SBB (<https://www.sbb.ch>)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package io.smartdatalake.workflow.dataobject

import io.smartdatalake.config.TestCustomFileCreator
import io.smartdatalake.testutils.DataObjectTestSuite
import io.smartdatalake.workflow.action.customlogic.CustomFileCreatorConfig
import org.scalatest.Matchers

import scala.io.Source.fromInputStream

class CustomFileDataObjectTest extends DataObjectTestSuite with Matchers {

  private val customFileCreatorClassName = classOf[TestCustomFileCreator].getName

  test("Creating an input stream should return an inputStream with the correct contents") {
    // prepare
    val config = CustomFileCreatorConfig(Option(customFileCreatorClassName))
    val customFileDataObject = CustomFileDataObject("testId", config)

    // run
    val result = customFileDataObject.createInputStream("")

    // check
    val resultString = fromInputStream(result).mkString
    assert(resultString.equals(TestCustomFileCreator.fileContents))
  }
}
