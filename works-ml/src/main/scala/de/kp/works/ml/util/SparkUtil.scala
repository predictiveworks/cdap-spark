package de.kp.works.ml.util
/*
 * Copyright (c) 2019 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 * 
 */

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import scala.collection.JavaConverters._

object SparkUtil extends Log4jLogger{

  private def union(left: DataFrame, right: DataFrame): DataFrame = {
  
    val cols: Array[String] = left.columns
    val res: DataFrame = left.union(right.select(cols.head, cols.tail: _*))
    
    log.debug(
      s"""
         |Left schema ${left.schema.treeString}
         |Right schema ${right.schema.treeString}
         |Union schema ${res.schema.treeString}
       """.stripMargin)
    res
  
  }

  /**
    * DataFrame workaround for Dataset union bug.
    * 
    * Union is performed in order of the operands.
    * 
    * @see [[https://issues.apache.org/jira/browse/SPARK-21109]]
    * 
    * @param head first DataFrame
    * @param tail varargs of successive DataFrames
    * @return new DataFrame representing the union
    */
  def union(head: DataFrame, tail: DataFrame*): DataFrame = {
    val dfs: List[DataFrame] = head :: tail.toList
    dfs.reduceLeft(union)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def update(row: Row, fieldName: String, value: Any): Row = {
    val i: Int = row.fieldIndex(fieldName)
    val ovs = row.toSeq
    var vs = ovs.slice(0, i) ++ Seq(value)
    val size: Int = ovs.size
    if (i != size - 1) {
      vs ++= ovs.slice(i + 1, size)
    }
    new GenericRowWithSchema(vs.toArray, row.schema)
  }

  /* Based on https://stackoverflow.com/a/40801637/519951 */
  def toDF(rows: Array[Row], schema: StructType)(implicit spark: SparkSession): DataFrame = {
    spark.createDataFrame(rows.toList.asJava, schema)
  }
}