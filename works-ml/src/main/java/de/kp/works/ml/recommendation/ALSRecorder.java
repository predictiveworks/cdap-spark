package de.kp.works.ml.recommendation;
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

import org.apache.spark.ml.recommendation.ALSModel;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.Algorithms;
import de.kp.works.core.ml.AbstractRecorder;
import de.kp.works.core.ml.SparkMLManager;
import de.kp.works.core.model.ModelScanner;

public class ALSRecorder extends AbstractRecorder {

	private Type metricsType = new TypeToken<Map<String, Object>>() {
	}.getType();

	private String getBestModelFsPath(Table table, String algorithmName, String modelName, String modelStage) {
		
		ModelScanner scanner = new ModelScanner();
		return scanner.bestRecommender(table, algorithmName, modelName, modelStage);

	}

	public ALSModel read(SparkExecutionPluginContext context, String modelName, String modelStage, String modelOption) throws Exception {

		FileSet fs = SparkMLManager.getRecommendationFS(context);
		Table table = SparkMLManager.getRecommendationTable(context);

		String algorithmName = Algorithms.ALS;
		
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
		 * Leverage Apache Spark mechanism to read the Bisecting KMeans clustering model
		 * from a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		return ALSModel.load(modelPath);

	}

	public void track(SparkExecutionPluginContext context, String modelName, String modelStage, String modelParams,
			String modelMetrics, ALSModel model) throws Exception {

		String algorithmName = Algorithms.ALS;

		/***** ARTIFACTS *****/

		Long ts = new Date().getTime();
		String fsPath = algorithmName + "/" + ts.toString() + "/" + modelName;
		/*
		 * Leverage Apache Spark mechanism to write the ALS model to a model specific
		 * file set
		 */
		FileSet fs = SparkMLManager.getRecommendationFS(context);

		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		model.save(modelPath);

		/***** METADATA *****/

		String modelPack = "WorksML";
		Table table = SparkMLManager.getRecommendationTable(context);

		setMetadata(ts, table, algorithmName, modelName, modelPack, modelStage, modelParams, modelMetrics, fsPath);

	}

	private void setMetadata(long ts, Table table, String algorithmName, String modelName, String modelPack,
			String modelStage, String modelParams, String modelMetrics, String fsPath) {

		String fsName = SparkMLManager.RECOMMENDATION_FS;
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