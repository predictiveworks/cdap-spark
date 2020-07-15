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

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.model.ModelProfile;
import de.kp.works.core.model.ModelScanner;

public class FeatureRecorder extends AbstractRecorder {

	protected String getModelPath(SparkExecutionPluginContext context, String algorithmName, String modelName, String modelStage, String modelOption)
			throws Exception {

		FileSet fs = SparkMLManager.getFeatureFS(context);
		Table table = SparkMLManager.getFeatureTable(context);
		
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

		ModelProfile profile = scanner.bestFeature(table, algorithmName, modelName, modelStage);
		if (profile == null)
			profile = getLatestModelProfile(table, algorithmName, modelName, modelStage);
		
		return profile;

	}

	protected void setMetadata(long ts, Table table, String namespace, String algorithmName, String modelName, String modelPack,
			String modelStage, String modelParams, String modelMetrics, String fsPath) {

		String fsName = SparkMLManager.FEATURE_FS;
		String modelVersion = getLatestModelVersion(table, algorithmName, namespace, modelName, modelStage);

		byte[] key = Bytes.toBytes(ts);
		Put row = buildRow(key, ts, namespace, modelName, modelVersion, fsName, fsPath, modelPack, modelStage, algorithmName,
				modelParams);

		table.put(row.add("metrics", modelMetrics));

	}
}
