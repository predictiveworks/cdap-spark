package de.kp.works.ml.recommendation
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
import java.util.{ Map => JMap }

import org.apache.spark.ml.recommendation.{ALS => SparkALS, ALSModel}

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

class ALSTrainer {

  def train(dataset: Dataset[Row], userCol: String, itemCol: String, ratingCol: String, params: JMap[String, Object]): ALSModel = {

    val algo = new SparkALS()
    
    val rank = params.get("rank").asInstanceOf[Int]
    algo.setRank(rank)
    
    val maxIter = params.get("maxIter").asInstanceOf[Int]
    algo.setMaxIter(maxIter)
    
    val numUserBlocks = params.get("numUserBlocks").asInstanceOf[Int]
    algo.setNumUserBlocks(numUserBlocks)
    
    val numItemBlocks = params.get("numItemBlocks").asInstanceOf[Int]
    algo.setNumItemBlocks(numItemBlocks)

    val implicitPrefs = if (params.get("implicitPrefs").asInstanceOf[String] == "true") true else false    
    algo.setImplicitPrefs(implicitPrefs)
    
    val alpha = params.get("alpha").asInstanceOf[Double]
    algo.setAlpha(alpha)
    
    val regParam = params.get("regParam").asInstanceOf[Double]
    algo.setRegParam(regParam)

    val nonnegative = if (params.get("nonnegative").asInstanceOf[String] == "true") true else false    
    algo.setNonnegative(nonnegative)

    algo.setUserCol(userCol)
    algo.setItemCol(itemCol)

    algo.setRatingCol(ratingCol)
    algo.fit(dataset)

  }

}