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
package io.smartdatalake.config

import configs.{ConfigError, ConfigKeyNaming, ConfigReader, Result}
import io.smartdatalake.config.SdlConfigObject.{ActionObjectId, ConnectionId, DataObjectId}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

import scala.language.implicitConversions

trait ConfigImplicits {

  /**
   * default naming strategy is to allow lowerCamelCase and hypen-separated key naming, and fail on superfluous keys
   */
  implicit def sdlDefaultNaming[A]: ConfigKeyNaming[A] =
    ConfigKeyNaming.hyphenSeparated[A].or(ConfigKeyNaming.lowerCamelCase[A].apply _)
      .withFailOnSuperfluousKeys()

  /**
   * A [[ConfigReader]] reader that reads [[StructType]] values.
   *
   * This reader parses a [[StructType]] from a DDL string.
   */
  implicit val structTypeReader: ConfigReader[StructType] = ConfigReader.fromTry { (c, p) =>
    StructType.fromDDL(c.getString(p))
  }

  /**
   * A [[ConfigReader]] reader that reads [[OutputMode]].
   */
  implicit val outputModeReader: ConfigReader[OutputMode] = {
    ConfigReader.fromConfig(_.toString.toLowerCase match {
      case "append" => Result.successful(OutputMode.Append())
      case "complete" => Result.successful(OutputMode.Complete())
      case "update" => Result.successful(OutputMode.Update())
      case x => Result.failure(ConfigError(s"$x is not a value of OutputMode. Supported values are append, complete, update."))
    })
  }

  // --------------------------------------------------------------------------------
  // Config reader to circumvent problems related to a bug:
  // The problem is that kxbmap sometimes can not find the correct config reader for
  // some non-trivial nested types, e.g. List[CustomCaseClass] or Option[CustomCaseClass]
  // see: https://github.com/kxbmap/configs/issues/44
  // For now in SDL only case classes are affected. It can therefore be cirumvented by a general productReader with
  // high priority.
  // TODO: check periodically if still needed, should not be needed with scala 2.13+
  // --------------------------------------------------------------------------------
  implicit def productReader[C <: Product]: ConfigReader[C] = ConfigReader[C]


  /**
   * A [[ConfigReader]] reader that reads [[DataObjectId]] values.
   */
  implicit val connectionIdReader: ConfigReader[ConnectionId] = ConfigReader.fromTry { (c, p) => ConnectionId(c.getString(p))}

  /**
   * A [[ConfigReader]] reader that reads [[DataObjectId]] values.
   */
  implicit val dataObjectIdReader: ConfigReader[DataObjectId] = ConfigReader.fromTry { (c, p) => DataObjectId(c.getString(p))}

  /**
   * A [[ConfigReader]] reader that reads [[ActionObjectId]] values.
   */
  implicit val actionObjectIdReader: ConfigReader[ActionObjectId] = ConfigReader.fromTry { (c, p) => ActionObjectId(c.getString(p))}

}