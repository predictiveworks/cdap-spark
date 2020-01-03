package org.apache.spark.sql.types

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

/**
 * __KUP__
 * 
 * Subsequent object is extracted from v2.4.x and
 * adjusted to be usable with version v2.1.3 
 */

object DataTypeUtil {
    /*
	   * The source code below is extracted from Apache Spark v2.4
  		 * [DecimalType]
	   */
    val ByteDecimal = DecimalType(3, 0)
    val ShortDecimal = DecimalType(5, 0)
    val IntDecimal = DecimalType(10, 0)
    val LongDecimal = DecimalType(20, 0)
    val FloatDecimal = DecimalType(14, 7)
    val DoubleDecimal = DecimalType(30, 15)
    val BigIntDecimal = DecimalType(38, 0)

    def DecimalforType(dataType: DataType): DecimalType = dataType match {
      case ByteType => ByteDecimal
      case ShortType => ShortDecimal
      case IntegerType => IntDecimal
      case LongType => LongDecimal
      case FloatType => FloatDecimal
      case DoubleType => DoubleDecimal
  }
    
}