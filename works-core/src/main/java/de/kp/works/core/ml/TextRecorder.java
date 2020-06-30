package de.kp.works.core.ml;
/*
 * Copyright (c) 2019 Dr. Krusche & Partner PartG. All rights reserved.
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

import java.lang.reflect.Type;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkPluginContext;
import de.kp.works.core.Algorithms;
import de.kp.works.core.model.ModelProfile;
import de.kp.works.core.model.ModelScanner;

public class TextRecorder extends AbstractRecorder {

	protected Type metricsType = new TypeToken<Map<String, Object>>() {
	}.getType();
	
	public String getModelPath(SparkPluginContext context, String algorithmName, String modelName, String modelStage, String modelOption)
			throws Exception {

		FileSet fs = SparkMLManager.getTextFS(context);
		Table table = SparkMLManager.getTextTable(context);
		
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

		if (profile.fsPath == null)return null;
		return fs.getBaseLocation().append(profile.fsPath).toURI().getPath();

	}
	
	public String getModelPath(SparkExecutionPluginContext context, String algorithmName, String modelName, String modelStage, String modelOption)
			throws Exception {

		FileSet fs = SparkMLManager.getTextFS(context);
		Table table = SparkMLManager.getTextTable(context);
		
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

		if (profile.fsPath == null)return null;
		return fs.getBaseLocation().append(profile.fsPath).toURI().getPath();

	}

	protected ModelProfile getBestModelProfile(Table table, String algorithmName, String modelName, String modelStage) {
		
		ModelScanner scanner = new ModelScanner();

		ModelProfile profile = scanner.bestText(table, algorithmName, modelName, modelStage);
		if (profile == null)
			profile = getLatestModelProfile(table, algorithmName, modelName, modelStage);
		
		return profile;

	}

	protected void setMetadata(long ts, Table table, String namespace, String algorithmName, String modelName, String modelPack,
			String modelStage, String modelParams, String modelMetrics, String fsPath) {

		String fsName = SparkMLManager.TEXTANALYSIS_FS;
		String modelVersion = getLatestModelVersion(table, algorithmName, modelName, modelStage);

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
