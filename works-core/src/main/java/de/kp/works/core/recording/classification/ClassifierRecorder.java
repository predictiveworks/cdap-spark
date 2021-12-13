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
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;

import java.lang.reflect.Type;
import java.util.Map;

public class ClassifierRecorder extends AbstractRecorder {

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
	protected void setMetadata(long ts, Table table, String modelNS, String modelName, String modelPack,
			String modelStage, String modelParams, String modelMetrics, String fsPath) throws Exception {

		Put row = buildRow(ts, table, modelNS, modelName, modelPack, modelStage, modelParams, fsPath);

		String[] metricNames = new String[] {
			Names.ACCURACY,
			Names.F1,
			Names.WEIGHTED_FMEASURE,
			Names.WEIGHTED_PRECISION,
			Names.WEIGHTED_RECALL,
			Names.WEIGHTED_FALSE_POSITIVE,
			Names.WEIGHTED_TRUE_POSITIVE
		};

		Map<String, Object> metrics = new Gson().fromJson(modelMetrics, metricsType);
		for (String metricName: metricNames) {
			Double metricValue = (Double) metrics.get(metricName);
			row.add(metricName, metricValue);
		}

		table.put(row);

	}

}
