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
import de.kp.works.core.Names;
import de.kp.works.core.configuration.ConfigReader;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkPluginContext;

import java.lang.reflect.Type;
import java.util.Map;

public class TextRecorder extends AbstractRecorder {

	protected String algoName;
	protected Type metricsType = new TypeToken<Map<String, Object>>() {}.getType();

	public TextRecorder(ConfigReader configReader) {
		super(configReader);
		algoType = SparkMLManager.TEXT;
	}

	public String getModelPath(SparkPluginContext context, String modelName, String modelStage, String modelOption)
			throws Exception {
		return getPath(context, algoName, modelName, modelStage, modelOption);
	}
	
	public String getModelPath(SparkExecutionPluginContext context, String modelName, String modelStage, String modelOption)
			throws Exception {
		return getPath(context, algoName, modelName, modelStage, modelOption);
	}

	protected String buildModelPath(SparkExecutionPluginContext context, String fsPath) throws Exception {
		return buildPath(context, fsPath);
	}

	protected String buildModelPath(SparkPluginContext context, String fsPath) throws Exception {
		return buildPath(context, fsPath);
	}

	@Override
	protected void setMetadata(long ts, Table table, String modelNS, String modelName, String modelPack,
			String modelStage, String modelParams, String modelMetrics, String fsPath) throws Exception {

		Put row = buildRow(ts, table, modelNS, modelName, modelPack, modelStage, modelParams, fsPath);
		if (algoName.equals(Algorithms.VIVEKN_SENTIMENT)) {

			String[] metricNames = new String[] {
					Names.RSME,
					Names.MSE,
					Names.MAE,
					Names.R2
			};

			Map<String, Object> metrics = new Gson().fromJson(modelMetrics, metricsType);
			for (String metricName: metricNames) {
				Double metricValue = (Double) metrics.get(metricName);
				row.add(metricName, metricValue);
			}

			table.put(row);

		} else {
			table.put(row.add("metrics", modelMetrics));
		}
		
	}
}
