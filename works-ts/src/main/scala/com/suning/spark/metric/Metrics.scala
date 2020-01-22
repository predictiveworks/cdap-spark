package com.suning.spark.metric
/*
 * Copyright (c) 2016 Suning R&D. All rights reserved.
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
 */

import com.suning.spark.util.Identifiable
import org.apache.spark.sql.DataFrame

abstract class Metrics(val uid: String, labelCol: String, predCol: String) extends Serializable{

  def this(labelCol: String, predCol: String) =
    this(Identifiable.randomUID("Metric"), labelCol, predCol)


  def getMetrics[T](df: DataFrame): Map[String, Double]
}
