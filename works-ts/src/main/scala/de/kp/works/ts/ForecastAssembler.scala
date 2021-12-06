package de.kp.works.ts
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
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

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

class ForecastAssembler(timeCol:String, valueCol:String, statusCol:String) {
  /*
   * The feature value is restricted to numeric values
   */
  private def getDouble(value: Any): Double = {
    value match {
      case s: Short => s.toDouble
      case i: Int => i.toDouble
      case l: Long => l.toDouble
      case f: Float => f.toDouble
      case d: Double => d
      case _ => 0.0
    }
  }

  private val toDouble = udf((value: Any) => getDouble(value))

  def assemble(dataset:Dataset[Row], forecast:Dataset[Row]):Dataset[Row] = {
    /*
     * The time column is restricted to a Long
     * and must not be changed; the value column,
     * however, is a numeric value that has to be
     * transformed into a Double
     */
    val measured = dataset
      .select(col(timeCol), col(valueCol))
      .withColumn(valueCol, toDouble(col(valueCol)))
      .withColumn(statusCol, lit("measured"))

    val forecasted = forecast.withColumn(statusCol, lit("forecast"))

    val output = Seq(measured, forecasted)
    output.reduce(_ union _)

  }

}
