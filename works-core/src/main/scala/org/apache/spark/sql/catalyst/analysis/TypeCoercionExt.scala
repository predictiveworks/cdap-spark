package org.apache.spark.sql.catalyst.analysis

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

import javax.annotation.Nullable

import scala.annotation.tailrec
import scala.collection.mutable

import org.apache.spark.sql.types._

/**
 * __KUP__
 * 
 * This object contains the v2.4.x functionality that is required
 * to infer JSON schemas; as in previous version TypeCoercion exists,
 * we just restrict to the missing (main) method
 * 
 * Note, it is important to keep this class at least in the org.apache.sql
 * package as IntegralType is private here
 */
class TypeCoercionExt(resolver: Resolver) {
  
  val numericPrecedence =
    IndexedSeq(
      ByteType,
      ShortType,
      IntegerType,
      LongType,
      FloatType,
      DoubleType)

  /**
   * Case 1 type widening (see the classdoc comment above for TypeCoercion).
   *
   * Find the tightest common type of two types that might be used in a binary expression.
   * This handles all numeric types except fixed-precision decimals interacting with each other or
   * with primitive types, because in that case the precision and scale of the result depends on
   * the operation. Those rules are implemented in [[DecimalPrecision]].
   */
  val findTightestCommonType: (DataType, DataType) => Option[DataType] = {
    case (t1, t2) if t1 == t2 => Some(t1)
    case (NullType, t1) => Some(t1)
    case (t1, NullType) => Some(t1)

    case (t1: IntegralType, t2: DecimalType) if t2.isWiderThan(t1) =>
      Some(t2)
    case (t1: DecimalType, t2: IntegralType) if t1.isWiderThan(t2) =>
      Some(t1)

    // Promote numeric types to the highest of the two
    case (t1: NumericType, t2: NumericType)
        if !t1.isInstanceOf[DecimalType] && !t2.isInstanceOf[DecimalType] =>
      val index = numericPrecedence.lastIndexWhere(t => t == t1 || t == t2)
      Some(numericPrecedence(index))

    case (_: TimestampType, _: DateType) | (_: DateType, _: TimestampType) =>
      Some(TimestampType)

    case (t1, t2) => findTypeForComplex(t1, t2, findTightestCommonType)
  }
  
  /* Leveraged from org.apache.spark.sql.catalyst.expressions.Cast v2.4x */
  private def forceNullable(from: DataType, to: DataType) = (from, to) match {
    case (NullType, _) => true
    case (_, _) if from == to => false

    case (StringType, BinaryType) => false
    case (StringType, _) => true
    case (_, StringType) => false

    case (FloatType | DoubleType, TimestampType) => true
    case (TimestampType, DateType) => false
    case (_, DateType) => true
    case (DateType, TimestampType) => false
    case (DateType, _) => true
    case (_, CalendarIntervalType) => true

    case (_, _: DecimalType) => true  // overflow
    case (_: FractionalType, _: IntegralType) => true  // NaN, infinity
    case _ => false
  }
  
  private def findTypeForComplex(
      t1: DataType,
      t2: DataType,
      findTypeFunc: (DataType, DataType) => Option[DataType]): Option[DataType] = (t1, t2) match {
    case (ArrayType(et1, containsNull1), ArrayType(et2, containsNull2)) =>
      findTypeFunc(et1, et2).map { et =>
        ArrayType(et, containsNull1 || containsNull2 ||
          forceNullable(et1, et) || forceNullable(et2, et))
      }
    case (MapType(kt1, vt1, valueContainsNull1), MapType(kt2, vt2, valueContainsNull2)) =>
      findTypeFunc(kt1, kt2)
        .filter { kt => !forceNullable(kt1, kt) && !forceNullable(kt2, kt) }
        .flatMap { kt =>
          findTypeFunc(vt1, vt2).map { vt =>
            MapType(kt, vt, valueContainsNull1 || valueContainsNull2 ||
              forceNullable(vt1, vt) || forceNullable(vt2, vt))
          }
      }
    case (StructType(fields1), StructType(fields2)) if fields1.length == fields2.length =>
      /* 
       * In v2.4.x the resolver is available from the SQLConf object;
       * earlier versions require the resolver explicitly provided to
       * avoid SQLConf dependencies
       */
      fields1.zip(fields2).foldLeft(Option(new StructType())) {
        case (Some(struct), (field1, field2)) if resolver(field1.name, field2.name) =>
          findTypeFunc(field1.dataType, field2.dataType).map { dt =>
            struct.add(field1.name, dt, field1.nullable || field2.nullable ||
              forceNullable(field1.dataType, dt) || forceNullable(field2.dataType, dt))
          }
        case _ => None
      }
    case _ => None
  }
}