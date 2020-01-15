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
import de.kp.works.ml.MLUtils

class MinHashLSHTrainer {
    
  def vectorize(trainset:Dataset[Row], featuresCol:String, vectorCol:String): Dataset[Row] = MLUtils.vectorize(trainset, featuresCol, vectorCol)
    
  def train(vectorset:Dataset[Row], vectorCol:String, params:JMap[String,Object]):MinHashLSHModel = {
    
    val minHashLSH = new org.apache.spark.ml.feature.MinHashLSH()

    val numHashTables = params.get("numHashTables").asInstanceOf[Int]
    minHashLSH.setNumHashTables(numHashTables)
    
    minHashLSH.setInputCol(vectorCol)
    minHashLSH.fit(vectorset)
    
  }
  
}

object MinHashLSHTest {
  
  import org.apache.spark.ml.linalg.Vectors
  def main(args:Array[String]) {

    val session = SparkSession.builder
      .appName("MinHashLSHTest")
      .master("local")
      .getOrCreate()

    val df = session.createDataFrame(Seq(
      (0, Vectors.sparse(6, Seq((0, 1.0), (1, 1.0), (2, 1.0)))),
      (1, Vectors.sparse(6, Seq((2, 1.0), (3, 1.0), (4, 1.0)))),
      (2, Vectors.sparse(6, Seq((0, 1.0), (2, 1.0), (4, 1.0))))
    )).toDF("id", "features")

    /** BUILD MODEL **/    
    val minHash = new org.apache.spark.ml.feature.MinHashLSH()
      .setNumHashTables(1)
      .setInputCol("features")
    
    val model = minHash.fit(df)
    model.set(model.outputCol, "myhashes")
    
    val rs = MLUtils.flattenMinHash(model.transform(df), "myhashes")
    rs.show()
    
    println(rs.schema)
    
  }
}