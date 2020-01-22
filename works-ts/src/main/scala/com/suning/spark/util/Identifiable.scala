package com.suning.spark.util
/*
 * Copyright (c) 2016 Suning R&D. All rights reserved.
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
 */

import java.util.UUID

trait Identifiable {

  /**
    * An immutable unique ID for the object and its derivatives.
    */
  val uid: String

  override def toString: String = uid
}


object Identifiable {

  /**
    * Returns a random UID that concatenates the given prefix, "_", and 16 random hex chars.
    */
  def randomUID(prefix: String): String = {
    prefix + "_" + UUID.randomUUID().toString.takeRight(16)
  }
}
