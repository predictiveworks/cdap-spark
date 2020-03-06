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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import de.kp.works.core.Names;
import de.kp.works.core.model.ModelScanner;

public class ClusterRecorder extends AbstractRecorder {

	protected Type metricsType = new TypeToken<Map<String, Object>>() {
	}.getType();

	protected String getBestModelFsPath(Table table, String algorithmName, String modelName, String modelStage) {
		
		ModelScanner scanner = new ModelScanner();
		String fsPath = scanner.bestCluster(table, algorithmName, modelName, modelStage);
		if (fsPath == null)
			fsPath = getLatestModelFsPath(table, algorithmName, modelName, modelStage);
		
		return fsPath;

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
