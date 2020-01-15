package de.kp.works.ml.feature
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
import java.util.{Map => JMap}

import org.apache.spark.ml.feature._
import org.apache.spark.sql._

class TFIDFTrainer {
      
  def train(dataset:Dataset[Row], inputCol:String, params:JMap[String,Object]):IDFModel = {
    
    /*
     * STEP #1: Leverage the Hashing TF transformer to turn
     * the sequence of words into a feature vector
     */
		val tf = new org.apache.spark.ml.feature.HashingTF()

		tf.setInputCol(inputCol);
		tf.setOutputCol("_features");

		val numFeatures = params.get("numFeatures").asInstanceOf[Int]
		tf.setNumFeatures(numFeatures)

		val transformed = tf.transform(dataset)
		/*
		 * STEP #2: Train the IDF model from the hashed feature 
		 * vectors
		 */
		val idf = new IDF()
		idf.setInputCol("_features")
		
		val minDocFreq = params.get("minDocFreq").asInstanceOf[Int]
		idf.setMinDocFreq(minDocFreq)
    
		idf.fit(transformed)
		
  }

}