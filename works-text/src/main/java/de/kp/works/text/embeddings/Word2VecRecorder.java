package de.kp.works.text.embeddings;
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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.api.batch.SparkPluginContext;
import de.kp.works.core.ml.AbstractModelManager;
import de.kp.works.core.ml.SparkMLManager;
import de.kp.works.text.embeddings.Word2VecModel;

public class Word2VecRecorder extends AbstractModelManager {

	private String ALGORITHM_NAME = "Word2Vec";
	/**
	 * The Word2Vec model is used with other builders; as their initializtion
	 * phase is based on the basic plugin context, we need an extra read method
	 */
	public Word2VecModel read(SparkPluginContext context, String modelName) throws Exception {

		FileSet fs = SparkMLManager.getTextanalysisFS(context);
		Table table = SparkMLManager.getTextanalysisMeta(context);
		
		return read(fs, table, modelName);
	}
	
	public Word2VecModel read(SparkExecutionPluginContext context, String modelName) throws Exception {

		FileSet fs = SparkMLManager.getTextanalysisFS(context);
		Table table = SparkMLManager.getTextanalysisMeta(context);
		
		return read(fs, table, modelName);
	}
		
	private Word2VecModel read(FileSet fs, Table table, String modelName) throws IOException {
		
		String fsPath = getModelFsPath(table, ALGORITHM_NAME, modelName);
		if (fsPath == null) return null;
		/*
		 * Leverage Apache Spark mechanism to read the Word2Vec model
		 * from a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		return Word2VecModel.load(modelPath);
		
	}

	public void track(SparkExecutionPluginContext context, String modelName, String modelParams, String modelMetrics,
			Word2VecModel model) throws Exception {

		FileSet fs = SparkMLManager.getTextanalysisFS(context);
		Table table = SparkMLManager.getTextanalysisMeta(context);
		
		save(fs, table, modelName, modelParams, modelMetrics, model);
		
	}

	public void save(FileSet fs, Table table, String modelName, String modelParams, String modelMetrics,
			Word2VecModel model) throws IOException {

		/***** MODEL COMPONENTS *****/

		/*
		 * Define the path of this model on CDAP's internal textanalysis fileset;
		 * not, the timestamp within the path ensures that each model of the 
		 * same name but different version has its own path
		 */
		Long ts = new Date().getTime();
		String fsPath = ALGORITHM_NAME + "/" + ts.toString() + "/" + modelName;
		/*
		 * Leverage Apache Spark mechanism to write the Word2Vec model
		 * to a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		model.save(modelPath);

		/***** MODEL METADATA *****/

		/*
		 * Append model metadata to the metadata table associated with the
		 * textanalysis fileset
		 */
		String fsName = SparkMLManager.TEXTANALYSIS_FS;
		String modelVersion = getModelVersion(table, ALGORITHM_NAME, modelName);

		byte[] key = Bytes.toBytes(ts);
		table.put(new Put(key).add("timestamp", ts).add("name", modelName).add("version", modelVersion)
				.add("algorithm", ALGORITHM_NAME).add("params", modelParams).add("metrics", modelMetrics)
				.add("fsName", fsName).add("fsPath", fsPath));

	}

	public Object getParam(Table table, String modelName, String paramName) {
		return getModelParam(table, ALGORITHM_NAME, modelName, paramName);
	}

}

