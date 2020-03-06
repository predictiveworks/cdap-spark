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

import de.kp.works.core.Algorithms;
import de.kp.works.core.ml.AbstractRecorder;
import de.kp.works.core.ml.SparkMLManager;
import de.kp.works.core.model.ModelScanner;

public class SentimentRecorder extends AbstractRecorder {

	protected Type metricsType = new TypeToken<Map<String, Object>>() {
	}.getType();

	public ViveknSentimentModel read(SparkExecutionPluginContext context, String modelName, String modelStage, String modelOption)
			throws Exception {

		FileSet fs = SparkMLManager.getTextFS(context);
		Table table = SparkMLManager.getTextTable(context);

		String algorithmName = Algorithms.VIVEKN_SENTIMENT;
		
		String fsPath = null;
		switch (modelOption) {
		case "best" : {
			fsPath = getBestModelFsPath(table, algorithmName, modelName, modelStage);
			break;
		}
		case "latest" : {
			fsPath = getLatestModelFsPath(table, algorithmName, modelName, modelStage);
			break;
		}
		default:
			throw new Exception(String.format("Model option '%s' is not supported yet.", modelOption));
		}

		if (fsPath == null)
			return null;
		/*
		 * Leverage Apache Spark mechanism to read the Sentiment Analysis model from a
		 * model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		ViveknSentimentModel model = (ViveknSentimentModel) ViveknSentimentModel.load(modelPath);

		return model;

	}

	public void track(SparkExecutionPluginContext context, String modelName, String modelStage, String modelParams,
			String modelMetrics, ViveknSentimentModel model) throws Exception {

		String algorithmName = Algorithms.VIVEKN_SENTIMENT;

		/***** ARTIFACTS *****/

		Long ts = new Date().getTime();
		String fsPath = algorithmName + "/" + ts.toString() + "/" + modelName;

		FileSet fs = SparkMLManager.getTextFS(context);

		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		model.save(modelPath);

		/***** METADATA *****/

		String modelPack = "WorksText";
		Table table = SparkMLManager.getTextTable(context);

		setMetadata(ts, table, algorithmName, modelName, modelPack, modelStage, modelParams, modelMetrics, fsPath);

	}
	private String getBestModelFsPath(Table table, String algorithmName, String modelName, String modelStage) {
		
		ModelScanner scanner = new ModelScanner();
		String fsPath = scanner.bestText(table, algorithmName, modelName, modelStage);
		if (fsPath == null)
			fsPath = getLatestModelFsPath(table, algorithmName, modelName, modelStage);
		
		return fsPath;

	}	
	/*
	 * HINT: This metadata setting implicitly extends the text analysis schema;
	 * CDAP supports this feature, but this change must recognized by the model
	 * services API
	 */
	private void setMetadata(long ts, Table table, String algorithmName, String modelName, String modelPack,
			String modelStage, String modelParams, String modelMetrics, String fsPath) {

		String fsName = SparkMLManager.TEXTANALYSIS_FS;
		String modelVersion = getLatestModelVersion(table, algorithmName, modelName, modelStage);

		byte[] key = Bytes.toBytes(ts);
		Put row = buildRow(key, ts, modelName, modelVersion, fsName, fsPath, modelPack, modelStage, algorithmName,
				modelParams);
		/*
		 * Unpack regression metrics to build time series of metric values
		 */
		Map<String, Object> metrics = new Gson().fromJson(modelMetrics, metricsType);

		Double rsme = (Double) metrics.get("rsme");
		Double mse = (Double) metrics.get("mse");
		Double mae = (Double) metrics.get("mae");
		Double r2 = (Double) metrics.get("r2");

		table.put(row.add("rsme", rsme).add("mse", mse).add("mae", mae).add("r2", r2));

	}

}
