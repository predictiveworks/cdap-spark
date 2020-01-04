package org.apache.spark

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

import org.apache.spark.ml.attribute._

import org.apache.spark.ml.linalg.{VectorUDT}

import org.apache.spark.sql._
import org.apache.spark.sql.types._

object WrappedUtils {

  /**
   * __KUP__ 
   * 
   * This method is extracted from v2.4.x MetadataUtils
   * 
   * 
   * Obtain the number of features in a vector column.
   * If no metadata is available, extract it from the dataset.
   */
  def getNumFeatures(dataset: Dataset[_], vectorCol: String): Int = {
    getNumFeatures(dataset.schema(vectorCol)).get
  }  
    
  /**
   * __KUP__ 
   * 
   * This method is extracted from v2.4.x MetadataUtils
   * 
   * Examine a schema to identify the number of features in a vector column.
   * Returns None if the number of features is not specified.
   */
  private def getNumFeatures(vectorSchema: StructField): Option[Int] = {

    if (vectorSchema.dataType == new VectorUDT) {
      val group = AttributeGroup.fromStructField(vectorSchema)
      val size = group.size
      if (size >= 0) {
        Some(size)
      } else {
        None
      }
    
    } else {
      None
    }
  }

}