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
package io.smartdatalake.workflow.action.customlogic

import io.smartdatalake.util.misc.{CustomCodeUtil, SmartDataLakeLogger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Same trait as [[CustomDfTransformer]] but supported multiple input and outputs.
 */
trait CustomDfsTransformer extends Serializable {

  def transform(session: SparkSession, options: Map[String,String], dfs: Map[String,DataFrame]) : Map[String,DataFrame]

}

/**
 *
 * @param className Optional class name to load transformer code from
 * @param scalaFile Optional file where scala code for transformation is loaded from
 * @param scalaCode Optional scala code for transformation
 * @param sqlCode Optional map of DataObjectId and corresponding SQL Code
 * @param options
 */
case class CustomDfsTransformerConfig( className: Option[String] = None, scalaFile: Option[String] = None, scalaCode: Option[String] = None, sqlCode: Map[String,String] = Map(), options: Map[String,String] = Map())
extends SmartDataLakeLogger {
  require(className.isDefined || scalaFile.isDefined || scalaCode.isDefined || !sqlCode.isEmpty, "Either className, scalaFile, scalaCode or sqlCode must be defined for CustomDfsTransformer")


  // Load Transformer code from appropriate location
  val impl: Option[CustomDfsTransformer] = className.map {
    clazz => CustomCodeUtil.getClassInstanceByName[CustomDfsTransformer](clazz)
  }.orElse{
    scalaFile.map {
      file =>
        val fnTransform = CustomCodeUtil.compileFromFile[(SparkSession, Map[String,String], Map[String,DataFrame]) => Map[String,DataFrame]](file)
        new CustomDfsTransformerWrapper( fnTransform )
    }
  }.orElse{
    scalaCode.map {
      code =>
        val fnTransform = CustomCodeUtil.compileCode[(SparkSession, Map[String,String], Map[String,DataFrame]) => Map[String,DataFrame]](code)
        new CustomDfsTransformerWrapper( fnTransform )
    }
  }

  override def toString: String = {
    if(className.isDefined)       "className: "+className.get
    else if(scalaFile.isDefined)  "scalaFile: "+scalaFile.get
    else if(scalaCode.isDefined)  "scalaCode: "+scalaCode.get
    else                          "sqlCode: "+sqlCode

  }

  def transform(dfs: Map[String,DataFrame])(implicit session: SparkSession) : Map[String,DataFrame] = {
    if(className.isDefined || scalaFile.isDefined || scalaCode.isDefined) {
      impl.get.transform(session, options, dfs)
    }
    // Work with SQL Transformations
    else {
      val invalidTableNameCharacters = "[^a-zA-Z0-9_]".r

      // register all input DataObjects as temporary table
      for( (dataObjectId,df) <- dfs) {
        val objectId = invalidTableNameCharacters.replaceAllIn(dataObjectId, "_")
        // TODO: Detect if multiple dataobjects end up with the same name after replacing characters
        // Using createTempView does not work because the same data object might be created more than once
        df.createOrReplaceTempView(objectId)
      }

      // execute all queries and return them under corresponding dataObjectId
      sqlCode.map {
        case (dataObjectId,sqlCode) => {
          val df = try {
            session.sql(sqlCode)
          } catch {
            case e : Throwable => throw new SQLTransformationException(s"Could not execute SQL query. Check your query and remember that special characters are replaced by underscores. Error: ${e.getMessage}")
          }
          (dataObjectId, df)
        }
      }

    }
  }
}
