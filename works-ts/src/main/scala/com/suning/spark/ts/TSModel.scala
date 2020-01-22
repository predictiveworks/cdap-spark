package com.suning.spark.ts
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

import com.suning.spark.util.{Identifiable, Load, Model}
import org.apache.spark.sql.DataFrame

abstract class TSModel(override val uid: String, inputCol: String, timeCol: String)
  extends Model {
  def this(inputCol: String, timeCol: String) =
    this(Identifiable.randomUID("TSModel"), inputCol, timeCol)

  def forecast(df: DataFrame, numAhead: Int): List[Double]
}

object TSModel extends Load[TSModel] {

}
