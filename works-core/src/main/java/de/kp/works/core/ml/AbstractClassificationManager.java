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

public class AbstractClassificationManager extends AbstractModelManager {

	protected Type metricsType = new TypeToken<Map<String, Object>>() {
	}.getType();

	protected void setMetadata(long ts, Table table, String algorithmName, String modelName, String modelParams,
			String modelMetrics, String fsPath) {
		/*
		 * Unpack classification metrics to build time series of metric values
		 */
		Map<String, Object> metrics = new Gson().fromJson(modelMetrics, metricsType);

		Double accuracy                  = (Double) metrics.get("accuracy");
		Double f1                        = (Double) metrics.get("f1");
		Double hammingLoss               = (Double) metrics.get("hammingLoss");
		Double weightedFMeasure          = (Double) metrics.get("weightedFMeasure");
		Double weightedPrecision         = (Double) metrics.get("weightedPrecision");
		Double weightedRecall            = (Double) metrics.get("weightedRecall");
		Double weightedFalsePositiveRate = (Double) metrics.get("weightedFalsePositiveRate");
		Double weightedTruePositiveRate  = (Double) metrics.get("weightedTruePositiveRate");

		String fsName = SparkMLManager.CLASSIFICATION_FS;
		String modelVersion = getModelVersion(table, algorithmName, modelName);

		byte[] key = Bytes.toBytes(ts);
		table.put(new Put(key).add("timestamp", ts).add("name", modelName).add("version", modelVersion)
				.add("algorithm", algorithmName).add("params", modelParams).add("accuracy", accuracy).add("f1", f1)
				.add("hammingLoss", hammingLoss).add("weightedFMeasure", weightedFMeasure)
				.add("weightedPrecision", weightedPrecision).add("weightedRecall", weightedRecall)
				.add("weightedFalsePositiveRate", weightedFalsePositiveRate)
				.add("weightedTruePositiveRate", weightedTruePositiveRate).add("fsName", fsName).add("fsPath", fsPath));

	}

}
