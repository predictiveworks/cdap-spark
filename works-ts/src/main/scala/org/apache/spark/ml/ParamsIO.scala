package org.apache.spark.ml
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
import org.json4s._
import org.apache.spark.SparkContext

/*
 * This object is introduced as a wrapper for [ml] private Apache Spark 
 * objects to read and write model metadata
 */
object ParamsIO {
  
  def getAndSetParams(instance: param.Params, metadata: util.DefaultParamsReader.Metadata) = 
    util.DefaultParamsReader.getAndSetParams(instance, metadata)  
  
  def loadMetadata(path: String, sc: SparkContext, expectedClassName: String = "") = 
    util.DefaultParamsReader.loadMetadata(path, sc, expectedClassName)

  def saveMetadata(instance: param.Params, path: String, sc: SparkContext, extraMetadata: Option[JObject] = None, paramMap: Option[JValue] = None) = 
    util.DefaultParamsWriter.saveMetadata(instance, path, sc, extraMetadata, paramMap)
    
}