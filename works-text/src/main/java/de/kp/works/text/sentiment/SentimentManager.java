package de.kp.works.text.sentiment;
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
import java.lang.reflect.Type;
import java.util.Date;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.johnsnowlabs.nlp.annotators.sda.vivekn.ViveknSentimentModel;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.ml.AbstractModelManager;
import de.kp.works.core.ml.SparkMLManager;

public class SentimentManager extends AbstractModelManager {

	private String ALGORITHM_NAME = "ViveknSentiment";

	protected Type metricsType = new TypeToken<Map<String, Object>>() {
	}.getType();

	public ViveknSentimentModel read(SparkExecutionPluginContext context, String modelName) throws Exception {

		FileSet fs = SparkMLManager.getTextanalysisFS(context);
		Table table = SparkMLManager.getTextanalysisMeta(context);
		
		return read(fs, table, modelName);
		
	}

	public ViveknSentimentModel read(FileSet fs, Table table, String modelName) throws IOException {
		
		String fsPath = getModelFsPath(table, ALGORITHM_NAME, modelName);
		if (fsPath == null) return null;
		/*
		 * Leverage Apache Spark mechanism to read the Sentiment Analysis model
		 * from a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		ViveknSentimentModel model = (ViveknSentimentModel) ViveknSentimentModel.load(modelPath);
		
		return model;
		
	}

	public void save(SparkExecutionPluginContext context, String modelName, String modelParams, String modelMetrics,
			ViveknSentimentModel model) throws Exception {

		FileSet fs = SparkMLManager.getTextanalysisFS(context);
		Table table = SparkMLManager.getTextanalysisMeta(context);
		
		save(fs, table, modelName, modelParams, modelMetrics, model);
		
	}

	private void save(FileSet modelFs, Table table, String modelName, String modelParams, String modelMetrics,
			ViveknSentimentModel model) throws IOException {

		/***** MODEL COMPONENTS *****/

		/*
		 * Define the path of this model on CDAP's internal text analysis fileset
		 */
		Long ts = new Date().getTime();
		String fsPath = ALGORITHM_NAME + "/" + ts.toString() + "/" + modelName;
		/*
		 * Leverage Apache Spark mechanism to write the Sentiment Analysis model
		 * to a model specific file set
		 */
		String modelPath = modelFs.getBaseLocation().append(fsPath).toURI().getPath();
		model.save(modelPath);

		/***** MODEL METADATA *****/

		setMetadata(ts, table, ALGORITHM_NAME, modelName, modelParams, modelMetrics, fsPath);

	}

	protected void setMetadata(long ts, Table table, String algorithmName, String modelName, String modelParams,
			String modelMetrics, String fsPath) {
		/*
		 * Unpack regression metrics to build time series of metric values
		 */
		Map<String, Object> metrics = new Gson().fromJson(modelMetrics, metricsType);

		Double rsme = (Double) metrics.get("rsme");
		Double mse = (Double) metrics.get("mse");
		Double mae = (Double) metrics.get("mae");
		Double r2 = (Double) metrics.get("r2");

		String fsName = SparkMLManager.TEXTANALYSIS_FS;
		String modelVersion = getModelVersion(table, algorithmName, modelName);

		byte[] key = Bytes.toBytes(ts);
		table.put(new Put(key).add("timestamp", ts).add("name", modelName).add("version", modelVersion)
				.add("algorithm", algorithmName).add("params", modelParams).add("rsme", rsme).add("mse", mse)
				.add("mae", mae).add("r2", r2).add("fsName", fsName).add("fsPath", fsPath));

	}
	
}
