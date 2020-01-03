package org.apache.spark.sql.catalyst.expressions

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

import java.text.{ DecimalFormat, DecimalFormatSymbols, ParsePosition }
import java.util.Locale
/**
 * __KUP__
 * 
 * [ExprUtils] is derived from the respective v2.4.x object,
 * but is stripped down to a single method leveraged by the
 * [JsonInferSchema] class
 * 
 */
object ExprUtils {

  def getDecimalParser(locale: Locale): String => java.math.BigDecimal = {
    if (locale == Locale.US) { 
      /* Special handling the default locale for backward compatibility */
      (s: String) => new java.math.BigDecimal(s.replaceAll(",", ""))

    } else {
      val decimalFormat = new DecimalFormat("", new DecimalFormatSymbols(locale))
      decimalFormat.setParseBigDecimal(true)
      (s: String) => {
        val pos = new ParsePosition(0)
        val result = decimalFormat.parse(s, pos).asInstanceOf[java.math.BigDecimal]
        if (pos.getIndex() != s.length() || pos.getErrorIndex() != -1) {
          throw new IllegalArgumentException("Cannot parse any decimal");
        } else {
          result
        }
      }
    }
  }
}