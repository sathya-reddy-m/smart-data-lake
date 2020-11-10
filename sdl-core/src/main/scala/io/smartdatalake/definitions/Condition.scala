/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2020 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.definitions

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.config.SdlConfigObject.ConfigObjectId
import io.smartdatalake.util.misc.SparkExpressionUtil
import org.apache.spark.sql.functions.expr

import scala.reflect.runtime.universe.TypeTag

import scala.util.Try

/**
 * Definition of a condition.
 *
 * @param expression Condition formualated as Spark SQL. The attributes available are dependent on the context.
 * @param description A textual description of the condition to be shown in error messages.
 */
case class Condition(expression: String, description: Option[String] = None) {

  private[smartdatalake] def syntaxCheck(id: ConfigObjectId, configName: Option[String]): Unit = {
    Try(expr(expression)) // try if spark can parse the condition expression
      .recoverWith { case e => throw ConfigurationException(s"($id) spark expression evaluation for '$expression' and config $configName failed: ${e.getMessage}", configName, e) }
  }

  private[smartdatalake] def evaluate[T<:Product:TypeTag](id: ConfigObjectId, configName: Option[String], data: T): Boolean = {
    SparkExpressionUtil.evaluateBoolean[T](id, configName, expression, data)
  }
}