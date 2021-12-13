package de.kp.works.core.recording;
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * This software is the confidential and proprietary information of 
 * Dr. Krusche & Partner PartG ("Confidential Information"). 
 * 
 * You shall not disclose such Confidential Information and shall use 
 * it only in accordance with the terms of the license agreement you 
 * entered into with Dr. Krusche & Partner PartG.
 * 
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 * 
 */

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import de.kp.works.core.Algorithms;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkPluginContext;

import java.lang.reflect.Type;
import java.util.Map;

public class TextRecorder extends AbstractRecorder {

	protected String algoName;
	protected String algoType = SparkMLManager.TEXT;

	protected Type metricsType = new TypeToken<Map<String, Object>>() {}.getType();
	
	public String getModelPath(SparkPluginContext context, String algoName, String modelName, String modelStage, String modelOption)
			throws Exception {
		return getPath(context, algoType, algoName, modelName, modelStage, modelOption);
	}
	
	public String getModelPath(SparkExecutionPluginContext context, String algoName, String modelName, String modelStage, String modelOption)
			throws Exception {
		return getPath(context, algoType, algoName, modelName, modelStage, modelOption);
	}

	protected String buildModelPath(SparkExecutionPluginContext context, String fsPath) throws Exception {
		return buildPath(context, algoType, fsPath);
	}

	protected String buildModelPath(SparkPluginContext context, String fsPath) throws Exception {
		return buildPath(context, algoType, fsPath);
	}

	protected void setMetadata(SparkExecutionPluginContext context, long ts, String modelName, String modelPack, String modelStage,
							   String modelParams, String modelMetrics, String fsPath) throws Exception {

		Table table = SparkMLManager.getTextTable(context);
		String modelNS = context.getNamespace();

		setMetadata(ts, table, modelNS, algoName, modelName, modelPack, modelStage, modelParams, modelMetrics, fsPath);

	}

	protected void setMetadata(long ts, Table table, String namespace, String algorithmName, String modelName, String modelPack,
			String modelStage, String modelParams, String modelMetrics, String fsPath) {

		String fsName = SparkMLManager.TEXTANALYSIS_FS;
		String modelVersion = getLatestVersion(table, algorithmName, namespace, modelName, modelStage);

		byte[] key = Bytes.toBytes(ts);
		Put row = buildRow(key, ts, namespace, modelName, modelVersion, fsName, fsPath, modelPack, modelStage, algorithmName,
				modelParams);

		if (algorithmName.equals(Algorithms.VIVEKN_SENTIMENT)) {
			/*
			 * Unpack regression metrics to build time series of metric values
			 */
			Map<String, Object> metrics = new Gson().fromJson(modelMetrics, metricsType);

			Double rsme = (Double) metrics.get("rsme");
			Double mse = (Double) metrics.get("mse");
			Double mae = (Double) metrics.get("mae");
			Double r2 = (Double) metrics.get("r2");

			table.put(row.add("rsme", rsme).add("mse", mse).add("mae", mae).add("r2", r2));
			
		} else {
			table.put(row.add("metrics", modelMetrics));
		}
		
	}
}
