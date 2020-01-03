package org.apache.spark.sql.catalyst.json

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

import com.fasterxml.jackson.core.{JsonFactory, JsonParser}

/**
 * __KUP__
 * 
 * The [CreateJacksonParser] object is extracted from Apache Spark v2.4.x
 * and stripped down to the 'string' method as this is the only one which
 * is needed here.
 */
object CreateJacksonParser extends Serializable {

  def string(jsonFactory: JsonFactory, record: String): JsonParser = {
    jsonFactory.createParser(record)
  }
}