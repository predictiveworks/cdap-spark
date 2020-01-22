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

import java.io.{FileInputStream, ObjectInputStream}
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.spark.SparkContext
  
trait Load[T] {

  def load(path: String): T = {
    val is = new ObjectInputStream(new FileInputStream(path))
    val t = is.readObject().asInstanceOf[T]
    is.close()
    t
  }

  def loadHDFS(sc: SparkContext, path: String): T = {
    val hadoopConf = sc.hadoopConfiguration
    val fileSystem = FileSystem.get(hadoopConf)
    val HDFSPath = new Path(path)
    val ois = new ObjectInputStream(new FSDataInputStream(fileSystem.open(HDFSPath)))
    val t = ois.readObject.asInstanceOf[T]
    ois.close()
    t
  }
}
