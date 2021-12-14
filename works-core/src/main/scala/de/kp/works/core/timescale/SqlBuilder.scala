package de.kp.works.core.timescale

/*
 * Copyright (c) 2019- 2021 Dr. Krusche & Partner PartG. All rights reserved.
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

import de.kp.works.core.Names

object SqlBuilder {

  val CLASSIFIER_FIELDS: String =
    s"""
       |uuid VARCHAR(255),
       |timestamp BIGINT,
       |model_ns VARCHAR(255)
       |model_id VARCHAR(255),
       |model_name VARCHAR(255),
       |model_pack VARCHAR(255),
       |model_stage VARCHAR(255),
       |model_vers VARCHAR(255),
       |model_params TEXT,
       |${Names.ACCURACY} NUMERIC,
       |${Names.F1} NUMERIC,
       |${Names.WEIGHTED_FMEASURE} NUMERIC,
       |${Names.WEIGHTED_PRECISION} NUMERIC,
       |${Names.WEIGHTED_PRECISION} NUMERIC,
       |${Names.WEIGHTED_RECALL} NUMERIC,
       |${Names.WEIGHTED_FALSE_POSITIVE} NUMERIC,
       |${Names.WEIGHTED_TRUE_POSITIVE} NUMERIC
       |PRIMARY KEY (uuid)
       |""".stripMargin.replaceAll("\\n", "").trim

  val CLUSTER_FIELDS: String =
    s"""
       |uuid VARCHAR(255),
       |timestamp BIGINT,
       |model_ns VARCHAR(255)
       |model_id VARCHAR(255),
       |model_name VARCHAR(255),
       |model_pack VARCHAR(255),
       |model_stage VARCHAR(255),
       |model_vers VARCHAR(255),
       |model_params TEXT,
       |${Names.SILHOUETTE_EUCLIDEAN} NUMERIC,
       |${Names.SILHOUETTE_COSINE} NUMERIC,
       |${Names.PERPLEXITY} NUMERIC,
       |${Names.LIKELIHOOD} NUMERIC
       |PRIMARY KEY (uuid)
       |""".stripMargin.replaceAll("\\n", "").trim

  val PLAIN_FIELDS: String =
    s"""
       |uuid VARCHAR(255),
       |timestamp BIGINT,
       |model_ns VARCHAR(255)
       |model_id VARCHAR(255),
       |model_name VARCHAR(255),
       |model_pack VARCHAR(255),
       |model_stage VARCHAR(255),
       |model_vers VARCHAR(255),
       |model_params TEXT,
       |model_metrics TEXT
       |PRIMARY KEY (uuid)
       |""".stripMargin.replaceAll("\\n", "").trim

  val REGRESSOR_FIELDS: String =
    s"""
       |uuid VARCHAR(255),
       |timestamp BIGINT,
       |model_ns VARCHAR(255)
       |model_id VARCHAR(255),
       |model_name VARCHAR(255),
       |model_pack VARCHAR(255),
       |model_stage VARCHAR(255),
       |model_vers VARCHAR(255),
       |model_params TEXT,
       |${Names.RSME} NUMERIC,
       |${Names.MSE} NUMERIC,
       |${Names.MAE} NUMERIC,
       |${Names.R2} NUMERIC
       |PRIMARY KEY (uuid)
       |""".stripMargin.replaceAll("\\n", "").trim

  def insertClassifierSql(algoName:String): String = {
      s"""
         |INSERT
         | INTO
         | $algoName
         | VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
         |""".stripMargin.replaceAll("\\n", "").trim
  }

  def insertClusterSql(algoName:String):String = {
    s"""
       |INSERT
       | INTO
       | $algoName
       | VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
       |""".stripMargin.replaceAll("\\n", "").trim
  }

  def insertPlainSql(algoName:String):String = {
    s"""
       |INSERT
       | INTO
       | $algoName
       | VALUES(?,?,?,?,?,?,?,?,?,?)
       |""".stripMargin.replaceAll("\\n", "").trim
  }

  def insertRegressorSql(algoName:String):String = {
    s"""
       |INSERT
       | INTO
       | $algoName
       | VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
       |""".stripMargin.replaceAll("\\n", "").trim
  }

}



