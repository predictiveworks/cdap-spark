package de.kp.works.core.recording.classification;
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
import de.kp.works.core.Names;
import de.kp.works.core.recording.AbstractRecorder;
import de.kp.works.core.recording.SparkMLManager;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;

import java.lang.reflect.Type;
import java.util.Map;

public class ClassifierRecorder extends AbstractRecorder {

	protected String algoName;
	protected Type metricsType = new TypeToken<Map<String, Object>>() {}.getType();

	public ClassifierRecorder() {
		algoType = SparkMLManager.CLASSIFIER;
	}

	/**
	 * This method retrieves the (internal) CDAP path of a stored
	 * machine learning model; either the path of the latest
	 *
	 */
	protected String getModelPath(SparkExecutionPluginContext context, String modelName, String modelStage, String modelOption) throws Exception {
		return getPath(context, algoName, modelName, modelStage, modelOption);
	}

	protected String buildModelPath(SparkExecutionPluginContext context, String fsPath) throws Exception {
		return buildPath(context, fsPath);
	}

	@Override
	protected void setMetadata(long ts, Table table, String namespace, String modelName, String modelPack,
			String modelStage, String modelParams, String modelMetrics, String fsPath) {

		String fsName = SparkMLManager.CLASSIFICATION_FS;
		String modelVersion = getLatestVersion(table, algoName, namespace, modelName, modelStage);

		byte[] key = Bytes.toBytes(ts);
		Put row = buildRow(key, ts, namespace, modelName, modelVersion, fsName, fsPath, modelPack, modelStage, algoName,
				modelParams);

		/*
		 * Unpack classification metrics to build time series of metric values
		 */
		Map<String, Object> metrics = new Gson().fromJson(modelMetrics, metricsType);

		Double accuracy = (Double) metrics.get(Names.ACCURACY);
		Double f1 = (Double) metrics.get(Names.F1);
		Double weightedFMeasure = (Double) metrics.get(Names.WEIGHTED_FMEASURE);
		Double weightedPrecision = (Double) metrics.get(Names.WEIGHTED_PRECISION);
		Double weightedRecall = (Double) metrics.get(Names.WEIGHTED_RECALL);
		Double weightedFalsePositiveRate = (Double) metrics.get(Names.WEIGHTED_FALSE_POSITIVE);
		Double weightedTruePositiveRate = (Double) metrics.get(Names.WEIGHTED_TRUE_POSITIVE);

		table.put(row.add(Names.ACCURACY, accuracy).add(Names.F1, f1).add(Names.WEIGHTED_FMEASURE, weightedFMeasure)
				.add(Names.WEIGHTED_PRECISION, weightedPrecision).add(Names.WEIGHTED_RECALL, weightedRecall)
				.add(Names.WEIGHTED_FALSE_POSITIVE, weightedFalsePositiveRate)
				.add(Names.WEIGHTED_TRUE_POSITIVE, weightedTruePositiveRate));

	}

}
