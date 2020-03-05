package de.kp.works.ts.util;
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

import java.util.Date;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.Algorithms;
import de.kp.works.core.ml.AbstractRecorder;
import de.kp.works.core.ml.SparkMLManager;
import de.kp.works.core.model.ModelScanner;
import de.kp.works.ts.AutoCorrelationModel;

public class ACFRecorder extends AbstractRecorder {

	/** AUTOCORRELATION FUNCTION **/

	public AutoCorrelationModel read(SparkExecutionPluginContext context, String modelName, String modelStage, String modelOption) throws Exception {
		
		FileSet fs = SparkMLManager.getTimeFS(context);
		Table table = SparkMLManager.getTimesTable(context);
		
		String algorithmName = Algorithms.ACF;
		
		String fsPath = null;
		switch (modelOption) {
		case "best" : {
			fsPath = getBestModelFsPath(table, algorithmName, modelName, modelStage);
			break;
		}
		case "latest" : {
			fsPath = getLatestModelFsPath(table, algorithmName, modelName, modelStage);
			break;
		}
		default:
			throw new Exception(String.format("Model option '%s' is not supported yet.", modelOption));
		}

		if (fsPath == null) return null;
		/*
		 * Leverage Apache Spark mechanism to read the AutoCorrelation model
		 * from a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		return AutoCorrelationModel.load(modelPath);
		
	}

	public void track(SparkExecutionPluginContext context, String modelName, String modelStage, String modelParams, String modelMetrics,
			AutoCorrelationModel model) throws Exception {
		
		String algorithmName = Algorithms.ACF;

		/***** ARTIFACTS *****/

		Long ts = new Date().getTime();
		String fsPath = algorithmName + "/" + ts.toString() + "/" + modelName;

		FileSet fs = SparkMLManager.getTimeFS(context);

		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		model.save(modelPath);

		/***** METADATA *****/

		String modelPack = "WorksTS";
		Table table = SparkMLManager.getTimesTable(context);

		setMetadata(ts, table, algorithmName, modelName, modelPack, modelStage, modelParams, modelMetrics, fsPath);

	}

	private String getBestModelFsPath(Table table, String algorithmName, String modelName, String modelStage) {
		
		ModelScanner scanner = new ModelScanner();
		return scanner.bestTime(table, algorithmName, modelName, modelStage);

	}

	private void setMetadata(long ts, Table table, String algorithmName, String modelName, String modelPack,
			String modelStage, String modelParams, String modelMetrics, String fsPath) {

		String fsName = SparkMLManager.TIMESERIES_FS;
		String modelVersion = getLatestModelVersion(table, algorithmName, modelName, modelStage);

		byte[] key = Bytes.toBytes(ts);
		Put row = buildRow(key, ts, modelName, modelVersion, fsName, fsPath, modelPack, modelStage, algorithmName,
				modelParams);

		table.put(row.add("metrics", modelMetrics));

	}	
}
