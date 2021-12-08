package de.kp.works.core.recording.regression;

/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
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
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import de.kp.works.core.recording.AbstractRecorder;
import de.kp.works.core.recording.SparkMLManager;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.Names;
import de.kp.works.core.model.ModelProfile;
import de.kp.works.core.model.ModelScanner;

public class RegressorRecorder extends AbstractRecorder {

	protected Type metricsType = new TypeToken<Map<String, Object>>() {
	}.getType();

	public String getModelPath(SparkExecutionPluginContext context, String algorithmName, String modelName, String modelStage, String modelOption) throws Exception {

		FileSet fs = SparkMLManager.getRegressionFS(context);
		Table table = SparkMLManager.getRegressionTable(context);
		
		switch (modelOption) {
		case "best" : {
			profile = getBestModelProfile(table, algorithmName, modelName, modelStage);
			break;
		}
		case "latest" : {
			profile = getLatestModelProfile(table, algorithmName, modelName, modelStage);
			break;
		}
		default:
			throw new Exception(String.format("Model option '%s' is not supported yet.", modelOption));
		}
				
		if (profile.fsPath == null) return null;
		return fs.getBaseLocation().append(profile.fsPath).toURI().getPath();
		
	}

	protected ModelProfile getBestModelProfile(Table table, String algorithmName, String modelName, String modelStage) {
		
		ModelScanner scanner = new ModelScanner();

		ModelProfile profile = scanner.bestRegressor(table, algorithmName, modelName, modelStage);
		if (profile == null)
			profile = getLatestModelProfile(table, algorithmName, modelName, modelStage);
		
		return profile;

	}

	protected void setMetadata(long ts, Table table, String namespace, String algorithmName, String modelName, String modelPack,
			String modelStage, String modelParams, String modelMetrics, String fsPath) {

		String fsName = SparkMLManager.REGRESSION_FS;
		String modelVersion = getLatestModelVersion(table, algorithmName, namespace, modelName, modelStage);

		byte[] key = Bytes.toBytes(ts);
		Put row = buildRow(key, ts, namespace, modelName, modelVersion, fsName, fsPath, modelPack, modelStage, algorithmName,
				modelParams);

		/*
		 * Unpack regression metrics to build time series of metric values
		 */
		Map<String, Object> metrics = new Gson().fromJson(modelMetrics, metricsType);

		Double rsme = (Double) metrics.get(Names.RSME);
		Double mse = (Double) metrics.get(Names.MSE);
		Double mae = (Double) metrics.get(Names.MAE);
		Double r2 = (Double) metrics.get(Names.R2);

		table.put(row.add(Names.RSME, rsme).add(Names.MSE, mse).add(Names.MAE, mae).add(Names.R2, r2));

	}
}
