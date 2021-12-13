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

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import de.kp.works.core.Names;
import de.kp.works.core.recording.AbstractRecorder;
import de.kp.works.core.recording.SparkMLManager;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;

import java.lang.reflect.Type;
import java.util.Map;

public class RegressorRecorder extends AbstractRecorder {

	protected String algoName;
	protected Type metricsType = new TypeToken<Map<String, Object>>() {}.getType();

	public RegressorRecorder() {
		algoType = SparkMLManager.REGRESSOR;
	}

	public String getModelPath(SparkExecutionPluginContext context, String modelName, String modelStage, String modelOption) throws Exception {
		return getPath(context, algoName, modelName, modelStage, modelOption);
	}

	protected String buildModelPath(SparkExecutionPluginContext context, String fsPath) throws Exception {
		return buildPath(context, fsPath);
	}

	@Override
	protected void setMetadata(long ts, Table table, String modelNS, String modelName, String modelPack,
			String modelStage, String modelParams, String modelMetrics, String fsPath) throws Exception {

		Put row = buildRow(ts, table, modelNS, modelName, modelPack, modelStage, modelParams, fsPath);
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
