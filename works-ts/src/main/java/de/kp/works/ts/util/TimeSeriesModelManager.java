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

import java.io.IOException;
import java.util.Date;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import de.kp.works.core.ml.AbstractRecorder;
import de.kp.works.core.ml.SparkMLManager;
import de.kp.works.ts.AutoCorrelationModel;

public class TimeSeriesModelManager extends AbstractRecorder {

	/** AUTOCORRELATION FUNCTION **/

	public AutoCorrelationModel readACF(FileSet fs, Table table, String modelName) throws IOException {
		
		String algorithmName = "ACF";
		
		String fsPath = getModelFsPath(table, algorithmName, modelName);
		if (fsPath == null) return null;
		/*
		 * Leverage Apache Spark mechanism to read the AutoCorrelation model
		 * from a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		return AutoCorrelationModel.load(modelPath);
		
	}

	public void saveACF(FileSet fs, Table table, String modelName, String modelParams, String modelMetrics,
			AutoCorrelationModel model) throws IOException {
		
		String algorithmName = "ACF";

		/***** MODEL COMPONENTS *****/

		/*
		 * Define the path of this model on CDAP's internal timeseries fileset
		 */
		Long ts = new Date().getTime();
		String fsPath = algorithmName + "/" + ts.toString() + "/" + modelName;
		/*
		 * Leverage Apache Spark mechanism to write the AutoCorrelation model
		 * to a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		model.save(modelPath);

		/***** MODEL METADATA *****/

		/*
		 * Append model metadata to the metadata table associated with the
		 * timeseries fileset
		 */
		String fsName = SparkMLManager.TIMESERIES_FS;
		String modelVersion = getModelVersion(table, algorithmName, modelName);

		byte[] key = Bytes.toBytes(ts);
		table.put(new Put(key).add("timestamp", ts).add("name", modelName).add("version", modelVersion)
				.add("algorithm", algorithmName).add("params", modelParams).add("metrics", modelMetrics)
				.add("fsName", fsName).add("fsPath", fsPath));

	}
	
}
