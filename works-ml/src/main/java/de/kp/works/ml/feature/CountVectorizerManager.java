package de.kp.works.ml.feature;
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

import java.io.IOException;
import java.util.Date;

import org.apache.spark.ml.feature.CountVectorizerModel;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import de.kp.works.core.ml.AbstractModelManager;
import de.kp.works.core.ml.SparkMLManager;

public class CountVectorizerManager extends AbstractModelManager {

	private String ALGORITHM_NAME = "CountVectorizer";

	public CountVectorizerModel read(FileSet fs, Table table, String modelName) throws IOException {
		
		String fsPath = getModelFsPath(table, ALGORITHM_NAME, modelName);
		if (fsPath == null) return null;
		/*
		 * Leverage Apache Spark mechanism to read the CountVectorizer model
		 * from a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		return CountVectorizerModel.load(modelPath);
		
	}

	public void save(FileSet modelFs, Table modelMeta, String modelName, String modelParams, String modelMetrics,
			CountVectorizerModel model) throws IOException {

		/***** MODEL COMPONENTS *****/

		/*
		 * Define the path of this model on CDAP's internal feature fileset
		 */
		Long ts = new Date().getTime();
		String fsPath = ALGORITHM_NAME + "/" + ts.toString() + "/" + modelName;
		/*
		 * Leverage Apache Spark mechanism to write the CountVectorizer model
		 * to a model specific file set
		 */
		String modelPath = modelFs.getBaseLocation().append(fsPath).toURI().getPath();
		model.save(modelPath);

		/***** MODEL METADATA *****/

		/*
		 * Append model metadata to the metadata table associated with the
		 * clustering fileset
		 */
		String fsName = SparkMLManager.FEATURE_FS;
		String modelVersion = getModelVersion(modelMeta, ALGORITHM_NAME, modelName);

		byte[] key = Bytes.toBytes(ts);
		modelMeta.put(new Put(key).add("timestamp", ts).add("name", modelName).add("version", modelVersion)
				.add("algorithm", ALGORITHM_NAME).add("params", modelParams).add("metrics", modelMetrics)
				.add("fsName", fsName).add("fsPath", fsPath));

	}

}