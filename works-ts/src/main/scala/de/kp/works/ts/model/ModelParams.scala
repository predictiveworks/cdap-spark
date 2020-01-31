package de.kp.works.ts.model
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
import java.math.{BigDecimal => JBigDecimal}

import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/** ARIMA PARAMETERS **/

trait HasStandardizationParam extends Params {

  final val standardization: BooleanParam = new BooleanParam(this, "standardization", "")

  final def getStandardization: Boolean = $(standardization)

  final def setStandardization(value:Boolean): this.type = set(standardization, value)
  
}

trait HasFitInterceptParam extends Params {

  final val fitIntercept: BooleanParam = new BooleanParam(this, "fitIntercept", "")

  final def getFitIntercept: Boolean = $(fitIntercept)

  final def setFitIntercept(value:Boolean): this.type = set(fitIntercept, value)
  
}

trait HasMeanOutParam extends Params {

  final val meanOut: BooleanParam = new BooleanParam(this, "meanOut", "")

  final def getMeanOut: Boolean = $(meanOut)

  final def setMeanOut(value:Boolean): this.type = set(meanOut, value)
  
}

trait HasEarlyStopParam extends Params {

  final val earlyStop: BooleanParam = new BooleanParam(this, "earlyStop", "")

  final def getEarlyStop: Boolean = $(earlyStop)

  final def setEarlyStop(value:Boolean): this.type = set(earlyStop, value)
  
}

trait HasCriterionParam extends Params {

  final val criterion: Param[String] = new Param[String](this, "criterion", "", 
      ParamValidators.inArray(Array("aic", "bic", "aicc")))

  final def getCriterion: String = $(criterion)

  final def setCriterion(value:String): this.type = set(criterion, value)
  
}

trait HasPParam extends Params {
  /**
   * ARIMA p parameter: order of the autoregressive part.
   */
  final val p: IntParam = new IntParam(this, "p", "Order of the autoregressive part. Must be >= 0. "
      + "Must also be greater than q (order of the moving average part).", ParamValidators.gtEq(0))

  final def getP: Int = $(p)

  final def setP(value:Int): this.type = set(p, value)
  
}

trait HasQParam extends Params {  
  /**
   * ARIMA q parameter: order of the moving average part.
   */
  final val q: IntParam = new IntParam(this, "q", "Order of the moving average part. Must be >= 0. "
      + "Must be smaller that p (order of the autoregressive part).", ParamValidators.gtEq(0))

  final def getQ: Int = $(q)

  final def setQ(value:Int): this.type = set(q, value)
  
}

trait HasDParam extends Params {  
  /**
   * ARIMA d parameter: degree of first differencing involved.
   */
  final val d: IntParam = new IntParam(this, "d", "Degree of first differencing involved.", ParamValidators.gtEq(0))

  final def getD: Int = $(d)

  final def setD(value:Int): this.type = set(d, value)
  
}

trait HasPMaxParam extends Params {

  final val pmax: IntParam = new IntParam(this, "pmax", "The maximum order of the autoregressive part. Must be >= 0. "
      + "Must also be greater than qmax (maximum order of the moving average part).", ParamValidators.gtEq(0))

  final def getPMax: Int = $(pmax)

  final def setPMax(value:Int): this.type = set(pmax, value)
  
}

trait HasQMaxParam extends Params {  

  final val qmax: IntParam = new IntParam(this, "qmax", "Maximum order of the moving average part. Must be >= 0. "
      + "Must be smaller that pmax (order of the autoregressive part).", ParamValidators.gtEq(0))

  final def getQMax: Int = $(qmax)

  final def setQMax(value:Int): this.type = set(qmax, value)
  
}

trait HasDMaxParam extends Params {  

  final val dmax: IntParam = new IntParam(this, "dmax", "Maximum degree of first differencing involved.", ParamValidators.gtEq(0))

  final def getDMax: Int = $(dmax)

  final def setDMax(value:Int): this.type = set(dmax, value)
  
}

/** REGRESSION PARAMETERS **/

trait HasRegParam extends Params {
  /**
   * Param for regularization parameter (&gt;= 0).
   */
  final val regParam: DoubleParam = new DoubleParam(this, "regParam", "regularization parameter (>= 0)", ParamValidators.gtEq(0))

  final def getRegParam: Double = $(regParam)
  
  final def setRegParam(value:Double): this.type = set(regParam, value)
  
}

trait HasElasticNetParam extends Params {

  /**
   * Param for the ElasticNet mixing parameter, in range [0, 1]. For alpha = 0, 
   * the penalty is an L2 penalty. For alpha = 1, it is an L1 penalty.
   */
  final val elasticNetParam: DoubleParam = new DoubleParam(this, "elasticNetParam", "the ElasticNet mixing parameter, "
      + "in range [0, 1]. For alpha = 0, the penalty is an L2 penalty. For alpha = 1, it is an L1 penalty", ParamValidators.inRange(0, 1))

  final def getElasticNetParam: Double = $(elasticNetParam)

  final def setElasticNetParam(value:Double): this.type = set(elasticNetParam, value)

}

trait ModelParams extends Params {
  
  final val timeCol = new Param[String](this, "timeCol",
      "Name of the timestamp field", (value:String) => true)
   
  final val valueCol = new Param[String](this, "valueCol",
      "Name of the value field", (value:String) => true)
 
  /** @group setParam */
  def setTimeCol(value:String): this.type = set(timeCol, value)
 
  /** @group setParam */
  def setValueCol(value:String): this.type = set(valueCol, value)
  
  def validateSchema(schema:StructType):Unit = {
    
    /* TIME FIELD */
    
    val timeColName = $(timeCol)  
    
    if (schema.fieldNames.contains(timeColName) == false)
      throw new IllegalArgumentException(s"Time column $timeColName does not exist.")
    
    val timeColType = schema(timeColName).dataType
    if (!(timeColType == DateType || timeColType == LongType || timeColType == TimestampType)) {
      throw new IllegalArgumentException(s"Data type of time column $timeColName must be DateType, LongType or TimestampType.")
    }
    
    /* VALUE FIELD */
    
    val valueColName = $(valueCol)  
    
    if (schema.fieldNames.contains(valueColName) == false)
      throw new IllegalArgumentException(s"Value column $valueColName does not exist.")
    
    val valueColType = schema(valueColName).dataType
    valueColType match {
      case DoubleType | FloatType | IntegerType | LongType | ShortType =>
      case _ => throw new IllegalArgumentException(s"Data type of value column $valueColName must be a numeric type.")
    }
    
  }

  def getDouble(value: Any): Double = {
    value match {
      case s: Short         => s.toDouble
      case i: Int           => i.toDouble
      case l: Long          => l.toDouble
      case f: Float         => f.toDouble
      case d: Double        => d
      case s: String        => s.toDouble
      case jbd: JBigDecimal => jbd.doubleValue
      case bd: BigDecimal   => bd.doubleValue
      case _ => 0.0
    }
  }
  
}