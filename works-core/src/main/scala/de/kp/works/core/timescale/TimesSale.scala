package de.kp.works.core.timescale
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

import de.kp.works.core.Names
import de.kp.works.core.configuration.JdbcConf
import de.kp.works.core.model.{ClassifierMetric, ClusterMetric, ModelProfile, RegressorMetric}
import io.cdap.cdap.api.dataset.table.Put

import java.nio.ByteBuffer
import java.sql.Connection
import java.util.{List => JList}
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

case class TimeTable(name:String, category:String, symbol:String)

object TimeScale {

  private var instance:Option[TimeScale] = None

  def getInstance(jdbcAccess:JdbcConf):TimeScale = {

    if (instance.isEmpty)
      instance = Some(new TimeScale(jdbcAccess))

    instance.get

  }

}
/**
 * [[TimeScale]] was initially designed to work with Hikari,
 * but we experience continuous connection leakages, even
 * after connections are evicted and released to the pool.
 *
 * As there currently is no need to work with a connection
 * pool, we switched to support situation specific conns.
 */
class TimeScale(jdbcAccess:JdbcConf) {

  /*
   * A helper method to retrieve a pooled database
   * connection to access Timescale DB
   */
  private var connection:Option[java.sql.Connection] = None

  /*
   * Helper method to create or update a Postgres
   * table
   */
  private def getConnection: Connection = try {

    val props = new java.util.Properties()
    val url = jdbcAccess.getUrl

    /* Set user & password */
    val user = jdbcAccess.getUser
    if (user != null) props.setProperty("user", user)

    val password = jdbcAccess.getPassword
    if (password != null) props.setProperty("password", password)

    if (connection.isEmpty)
      connection = Some(java.sql.DriverManager.getConnection(url, props))

    val conn = connection.get
    if (conn.isClosed)
      connection = Some(java.sql.DriverManager.getConnection(url, props))

    connection.get.setAutoCommit(false)
    connection.get

  } catch {
    case t:Throwable => t.printStackTrace();null
  }
  /**
   * This method retrieves the classifier metrics from the
   * algorithm specific table and transforms into a data
   * structure that is used to determine the best model
   */
  def getClassifierMetrics(algoName:String, modelName:String, modelStage:String):JList[ClassifierMetric] = {

    val metricsSql = SqlBuilder.classifierMetricsSql(algoName, modelName, modelStage)
    val metrics = readTable(metricsSql)

    metrics.map(row => {

      val metric = new ClassifierMetric()
      metric.setTs(row.head.asInstanceOf[Long])

      metric.setId(row(1).asInstanceOf[String])
      metric.setAccuracy(row(2).asInstanceOf[Double])

      metric.setF1(row(3).asInstanceOf[Double])
      metric.setWeightedFMeasure(row(4).asInstanceOf[Double])

      metric.setWeightedPrecision(row(5).asInstanceOf[Double])
      metric.setWeightedRecall(row(6).asInstanceOf[Double])

      metric.setWeightedFalsePositiveRate(row(7).asInstanceOf[Double])
      metric.setWeightedTruePositiveRate(row(8).asInstanceOf[Double])

      metric.setFsPath(row(9).asInstanceOf[String])
      metric

    }).toList

  }

  def insertClassifierRow(algoName:String, row:Put):Unit = {
    /*
     * Create algorithm specific table if it
     * does not exist already
     */
    val table = algoName
    val columns = SqlBuilder.CLASSIFIER_FIELDS

    createIfNotExistsTable(table, columns)
    val insertSql = SqlBuilder.insertClassifierSql(table)

    val conn = getConnection
    val stmt = conn.prepareStatement(insertSql)

    /* uuid **/
    val uuid = java.util.UUID.randomUUID.toString

    var pos = 1
    stmt.setString(pos, uuid)

    val values = row.getValues
      .map{case(k, v) => (new String(k), v)}

    /* timestamp */
    val timestamp = ByteBuffer.wrap(values(Names.TIMESTAMP)).getLong

    pos += 1
    stmt.setLong(pos, timestamp)

    /* model_ns */
    val model_ns = new String(values(Names.NAMESPACE))

    pos += 1
    stmt.setString(pos, model_ns)

    /* model_id */
    val model_id = new String(values(Names.ID))

    pos += 1
    stmt.setString(pos, model_id)

    /* model_name */
    val model_name = new String(values(Names.NAME))

    pos += 1
    stmt.setString(pos, model_name)

    /* model_pack */
    val model_pack = new String(values(Names.PACK))

    pos += 1
    stmt.setString(pos, model_pack)

    /* model_stage */
    val model_stage = new String(values(Names.STAGE))

    pos += 1
    stmt.setString(pos, model_stage)

    /* model_vers */
    val model_vers = new String(values(Names.VERSION))

    pos += 1
    stmt.setString(pos, model_vers)

    /* model_params */
    val model_params = new String(values(Names.PARAMS))

    pos += 1
    stmt.setString(pos, model_params)

    /* model metrics */
    val metricNames = Array[String](
      Names.ACCURACY,
      Names.F1,
      Names.WEIGHTED_FMEASURE,
      Names.WEIGHTED_PRECISION,
      Names.WEIGHTED_RECALL,
      Names.WEIGHTED_FALSE_POSITIVE,
      Names.WEIGHTED_TRUE_POSITIVE)

    metricNames.foreach(metricName => {
      val metricValue = ByteBuffer.wrap(values(metricName)).getDouble

      pos += 1
      stmt.setDouble(pos, metricValue)

    })

    /* fs_name */
    val fs_name = new String(values(Names.FS_NAME))

    pos += 1
    stmt.setString(pos, fs_name)

    /* fs_path */
    val fs_path = new String(values(Names.FS_PATH))

    pos += 1
    stmt.setString(pos, fs_path)

    stmt.addBatch()

    stmt.executeBatch()
    stmt.close()

    conn.commit()
    releaseConn(conn)

  }
  /**
   * This method retrieves the cluster metrics from the
   * algorithm specific table and transforms into a data
   * structure that is used to determine the best model
   */
  def getClusterMetrics(algoName:String, modelName:String, modelStage:String):JList[ClusterMetric] = {

    val metricsSql = SqlBuilder.clusterMetricsSql(algoName, modelName, modelStage)
    val metrics = readTable(metricsSql)

    metrics.map(row => {

      val metric = new ClusterMetric()
      metric.setTs(row.head.asInstanceOf[Long])

      metric.setId(row(1).asInstanceOf[String])
      metric.setSilhouetteEuclidean(row(2).asInstanceOf[Double])

      metric.setSilhouetteCosine(row(3).asInstanceOf[Double])
      metric.setPerplexity(row(4).asInstanceOf[Double])

      metric.setLikelihood(row(5).asInstanceOf[Double])
      metric.setFsPath(row(6).asInstanceOf[String])
      metric

    }).toList

  }

  def insertClusterRow(algoName:String, row:Put):Unit = {
    /*
     * Create algorithm specific table if it
     * does not exist already
     */
    val table = algoName
    val columns = SqlBuilder.CLUSTER_FIELDS

    createIfNotExistsTable(table, columns)
    val insertSql = SqlBuilder.insertClassifierSql(table)

    val conn = getConnection
    val stmt = conn.prepareStatement(insertSql)

    /* uuid **/
    val uuid = java.util.UUID.randomUUID.toString

    var pos = 1
    stmt.setString(pos, uuid)

    val values = row.getValues
      .map{case(k, v) => (new String(k), v)}

    /* timestamp */
    val timestamp = ByteBuffer.wrap(values(Names.TIMESTAMP)).getLong

    pos += 1
    stmt.setLong(pos, timestamp)

    /* model_ns */
    val model_ns = new String(values(Names.NAMESPACE))

    pos += 1
    stmt.setString(pos, model_ns)

    /* model_id */
    val model_id = new String(values(Names.ID))

    pos += 1
    stmt.setString(pos, model_id)

    /* model_name */
    val model_name = new String(values(Names.NAME))

    pos += 1
    stmt.setString(pos, model_name)

    /* model_pack */
    val model_pack = new String(values(Names.PACK))

    pos += 1
    stmt.setString(pos, model_pack)

    /* model_stage */
    val model_stage = new String(values(Names.STAGE))

    pos += 1
    stmt.setString(pos, model_stage)

    /* model_vers */
    val model_vers = new String(values(Names.VERSION))

    pos += 1
    stmt.setString(pos, model_vers)

    /* model_params */
    val model_params = new String(values(Names.PARAMS))

    pos += 1
    stmt.setString(pos, model_params)

    /* model metrics */
    val metricNames = Array[String](
      Names.SILHOUETTE_EUCLIDEAN,
      Names.SILHOUETTE_COSINE,
      Names.PERPLEXITY,
      Names.LIKELIHOOD)

    metricNames.foreach(metricName => {
      val metricValue = ByteBuffer.wrap(values(metricName)).getDouble

      pos += 1
      stmt.setDouble(pos, metricValue)

    })

    /* fs_name */
    val fs_name = new String(values(Names.FS_NAME))

    pos += 1
    stmt.setString(pos, fs_name)

    /* fs_path */
    val fs_path = new String(values(Names.FS_PATH))

    pos += 1
    stmt.setString(pos, fs_path)

    stmt.addBatch()

    stmt.executeBatch()
    stmt.close()

    conn.commit()
    releaseConn(conn)

  }

  def insertPlainRow(algoName:String, row:Put):Unit = {
    /*
     * Create algorithm specific table if it
     * does not exist already
     */
    val table = algoName
    val columns = SqlBuilder.PLAIN_FIELDS

    createIfNotExistsTable(table, columns)
    val insertSql = SqlBuilder.insertClassifierSql(table)

    val conn = getConnection
    val stmt = conn.prepareStatement(insertSql)

    /* uuid **/
    val uuid = java.util.UUID.randomUUID.toString

    var pos = 1
    stmt.setString(pos, uuid)

    val values = row.getValues
      .map{case(k, v) => (new String(k), v)}

    /* timestamp */
    val timestamp = ByteBuffer.wrap(values(Names.TIMESTAMP)).getLong

    pos += 1
    stmt.setLong(pos, timestamp)

    /* model_ns */
    val model_ns = new String(values(Names.NAMESPACE))

    pos += 1
    stmt.setString(pos, model_ns)

    /* model_id */
    val model_id = new String(values(Names.ID))

    pos += 1
    stmt.setString(pos, model_id)

    /* model_name */
    val model_name = new String(values(Names.NAME))

    pos += 1
    stmt.setString(pos, model_name)

    /* model_pack */
    val model_pack = new String(values(Names.PACK))

    pos += 1
    stmt.setString(pos, model_pack)

    /* model_stage */
    val model_stage = new String(values(Names.STAGE))

    pos += 1
    stmt.setString(pos, model_stage)

    /* model_vers */
    val model_vers = new String(values(Names.VERSION))

    pos += 1
    stmt.setString(pos, model_vers)

    /* model_params */
    val model_params = new String(values(Names.PARAMS))

    pos += 1
    stmt.setString(pos, model_params)

    /* model_metrics */
    val model_metrics = new String(values(Names.METRICS))

    pos += 1
    stmt.setString(pos, model_metrics)

    /* fs_name */
    val fs_name = new String(values(Names.FS_NAME))

    pos += 1
    stmt.setString(pos, fs_name)

    /* fs_path */
    val fs_path = new String(values(Names.FS_PATH))

    pos += 1
    stmt.setString(pos, fs_path)

    stmt.addBatch()

    stmt.executeBatch()
    stmt.close()

    conn.commit()
    releaseConn(conn)

  }
  /**
   * This method retrieves the regressor metrics from the
   * algorithm specific table and transforms into a data
   * structure that is used to determine the best model
   */
  def getRegressorMetrics(algoName:String, modelName:String, modelStage:String):JList[RegressorMetric] = {

    val metricsSql = SqlBuilder.regressorMetricsSql(algoName, modelName, modelStage)
    val metrics = readTable(metricsSql)

    metrics.map(row => {

      val metric = new RegressorMetric()
      metric.setTs(row.head.asInstanceOf[Long])

      metric.setId(row(1).asInstanceOf[String])
      metric.setRsme(row(2).asInstanceOf[Double])

      metric.setMse(row(3).asInstanceOf[Double])
      metric.setMae(row(4).asInstanceOf[Double])

      metric.setR2(row(5).asInstanceOf[Double])
      metric.setFsPath(row(6).asInstanceOf[String])

      metric

    }).toList

  }

  def insertRegressorRow(algoName:String, row:Put):Unit = {
    /*
     * Create algorithm specific table if it
     * does not exist already
     */
    val table = algoName
    val columns = SqlBuilder.REGRESSOR_FIELDS

    createIfNotExistsTable(table, columns)
    val insertSql = SqlBuilder.insertClassifierSql(table)

    val conn = getConnection
    val stmt = conn.prepareStatement(insertSql)

    /* uuid **/
    val uuid = java.util.UUID.randomUUID.toString

    var pos = 1
    stmt.setString(pos, uuid)

    val values = row.getValues
      .map{case(k, v) => (new String(k), v)}

    /* timestamp */
    val timestamp = ByteBuffer.wrap(values(Names.TIMESTAMP)).getLong

    pos += 1
    stmt.setLong(pos, timestamp)

    /* model_ns */
    val model_ns = new String(values(Names.NAMESPACE))

    pos += 1
    stmt.setString(pos, model_ns)

    /* model_id */
    val model_id = new String(values(Names.ID))

    pos += 1
    stmt.setString(pos, model_id)

    /* model_name */
    val model_name = new String(values(Names.NAME))

    pos += 1
    stmt.setString(pos, model_name)

    /* model_pack */
    val model_pack = new String(values(Names.PACK))

    pos += 1
    stmt.setString(pos, model_pack)

    /* model_stage */
    val model_stage = new String(values(Names.STAGE))

    pos += 1
    stmt.setString(pos, model_stage)

    /* model_vers */
    val model_vers = new String(values(Names.VERSION))

    pos += 1
    stmt.setString(pos, model_vers)

    /* model_params */
    val model_params = new String(values(Names.PARAMS))

    pos += 1
    stmt.setString(pos, model_params)

    /* model metrics */
    val metricNames = Array[String](
      Names.RSME,
      Names.MSE,
      Names.MAE,
      Names.R2)

    metricNames.foreach(metricName => {
      val metricValue = ByteBuffer.wrap(values(metricName)).getDouble

      pos += 1
      stmt.setDouble(pos, metricValue)

    })

    /* fs_name */
    val fs_name = new String(values(Names.FS_NAME))

    pos += 1
    stmt.setString(pos, fs_name)

    /* fs_path */
    val fs_path = new String(values(Names.FS_PATH))

    pos += 1
    stmt.setString(pos, fs_path)

    stmt.addBatch()

    stmt.executeBatch()
    stmt.close()

    conn.commit()
    releaseConn(conn)

  }
  /**
   * This method retrieves a sorted (ascending time order)
   * list of model ids and artifact paths for the provided
   * algo name, model name and stage.
   *
   * The last id and artifact path is returned.
   */
  def getLastProfile(algoName:String, modelName:String, modelStage:String):ModelProfile = {

    val modelPathSql = SqlBuilder.modelPathSql(algoName, modelName, modelStage)
    val result = readTable(modelPathSql)

    val profiles = result.map(row => {

      val profile = new ModelProfile()

      profile.setId(row(1).asInstanceOf[String])
      profile.setPath(row(2).asInstanceOf[String])

      profile

    })

    profiles.last

  }

  def getLastVersion(algoName:String, modelNS:String, modelName:String, modelStage:String):String = {

    val versionSql = SqlBuilder.modelVersionSql(algoName, modelNS, modelName, modelStage)
    val result = readTable(versionSql)

    if (result.isEmpty) {
      "M-1"
    }
    else {

      val latest = result
        .map(row =>
          row(1).asInstanceOf[String]
        )
        .last

      val tokens = latest.split("-")
      val numVersion = Integer.parseInt(tokens(1)) + 1

      Integer.toString(numVersion)

    }
  }

  def createIfNotExistsTable(name:String, columns:String):Unit = {

    val sql = s"CREATE TABLE IF NOT EXISTS $name ($columns)"

    val conn = getConnection
    val stmt = conn.createStatement

    stmt.executeUpdate(sql)
    if (stmt != null) stmt.close()
    /*
     * `autocommit` is set to false
     */
    conn.commit()
    releaseConn(conn)

  }

  def createTable(name:String, columns:String):Unit = {

    val conn = getConnection
    try {

      val sql = s"DROP TABLE IF EXISTS $name"
      val stmt = conn.createStatement

      stmt.executeUpdate(sql)
      stmt.close()
      /*
       * `autocommit` is set to false
       */
      conn.commit()

    } catch {
      case _:Throwable => /* Do nothing */
    }

    val sql = s"CREATE TABLE $name ($columns)"
    val stmt = conn.createStatement

    stmt.executeUpdate(sql)
    stmt.close()
    /*
     * `autocommit` is set to false
     */
    conn.commit()
    releaseConn(conn)

  }

  def dropTable(name:String):Unit = {

    try {

      val sql = s"DROP TABLE $name"

      val conn = getConnection
      val stmt = conn.createStatement

      stmt.executeUpdate(sql)
      stmt.close()
      /*
       * `autocommit` is set to false
       */
      conn.commit()
      releaseConn(conn)

    } catch {
      case _:Throwable => /* Do nothing */
    }

  }

  def countTable(name:String):Int = {

    val sql = s"SELECT COUNT(*) FROM $name"

    val conn = getConnection
    val stmt = conn.createStatement()

    var count = 0

    val results = stmt.executeQuery(sql)
    while (results.next) {
      count = results.getInt("count")
    }

    results.close()
    stmt.close()

    conn.commit()
    releaseConn(conn)

    count

  }

  /*
   * A helper method to retrieve the content of
   * a certain table by applying a SQL query
   */
  def readTable(readSql:String):Seq[Seq[Any]] = {

    val conn = getConnection
    val stmt = conn.createStatement()

    val results = stmt.executeQuery(readSql)
    val metadata = results.getMetaData

    val count = metadata.getColumnCount
    val schema = (1 to count).map(idx => {

      val label  = metadata.getColumnLabel(idx)
      val `type` = metadata.getColumnTypeName(idx)

      (idx, label, `type`)
    })

    val rows = ArrayBuffer.empty[Seq[Any]]
    while (results.next()) {

      val values = schema.map(entry => {

        val name = entry._2
        entry._3 match {
          case "int4" =>
            results.getInt(name)
          case "int8" =>
            results.getLong(name)
          case "float8" =>
            results.getDouble(name)
          case "name" =>
            results.getString(name)
          case "numeric" =>
            results.getDouble(name)
          case "text" =>
            results.getString(name)
          case "varchar" =>
            results.getString(name)

          case _ => throw new Exception(s"Data type `${entry._3}` not supported.")

        }
      })

      rows += values
    }

    results.close()
    stmt.close()

    conn.commit()
    releaseConn(conn)

    rows
  }

  private def releaseConn(conn:java.sql.Connection):Unit = {

    /* Check whether connection is closed */
    if (!conn.isClosed) conn.close()

  }

}
