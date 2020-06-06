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

import de.kp.works.core.Names;
import de.kp.works.core.model.ModelProfile;
import de.kp.works.core.model.ModelScanner;

public class ClusterRecorder extends AbstractRecorder {

	protected Type metricsType = new TypeToken<Map<String, Object>>() {
	}.getType();
	
	public String getModelPath(SparkExecutionPluginContext context, String algorithmName, String modelName, String modelStage, String modelOption) throws Exception {

		FileSet fs = SparkMLManager.getClusteringFS(context);
		Table table = SparkMLManager.getClusteringTable(context);
		
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

		ModelProfile profile = scanner.bestCluster(table, algorithmName, modelName, modelStage);
		if (profile == null)
			profile = getLatestModelProfile(table, algorithmName, modelName, modelStage);
		
		return profile;

	}

	protected void setMetadata(long ts, Table table, String algorithmName, String modelName, String modelPack,
			String modelStage, String modelParams, String modelMetrics, String fsPath) {

		String fsName = SparkMLManager.CLUSTERING_FS;
		String modelVersion = getLatestModelVersion(table, algorithmName, modelName, modelStage);

		byte[] key = Bytes.toBytes(ts);
		Put row = buildRow(key, ts, modelName, modelVersion, fsName, fsPath, modelPack, modelStage, algorithmName,
				modelParams);

		/*
		 * Unpack recommendation metrics to build time series of metric values
		 */
		Map<String, Object> metrics = new Gson().fromJson(modelMetrics, metricsType);

		Double silhouette_euclidean = (Double) metrics.get(Names.SILHOUETTE_EUCLDIAN);
		Double silhouette_cosine = (Double) metrics.get(Names.SILHOUETTE_COSINE);
		Double perplexity = (Double) metrics.get(Names.PERPLEXITY);
		Double likelihood = (Double) metrics.get(Names.LIKELIHOOD);

		table.put(row.add(Names.SILHOUETTE_EUCLDIAN, silhouette_euclidean).add(Names.SILHOUETTE_COSINE, silhouette_cosine)
				.add(Names.PERPLEXITY, perplexity).add(Names.LIKELIHOOD, likelihood));

	}

}
