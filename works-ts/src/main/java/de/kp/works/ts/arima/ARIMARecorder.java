package de.kp.works.ts.arima;
/*
 * Copyright (c) 2019 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 * 
 */

import java.util.Date;

import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.ml.TimeRecorder;
import de.kp.works.core.Algorithms;
import de.kp.works.core.ml.SparkMLManager;
import de.kp.works.ts.model.ARIMAModel;
import de.kp.works.ts.model.AutoARIMAModel;

public class ARIMARecorder extends TimeRecorder {

	/** READ **/
	
	public ARIMAModel readARIMA(SparkExecutionPluginContext context, String modelName, String modelStage) throws Exception {

		FileSet fs = SparkMLManager.getTimeFS(context);
		Table table = SparkMLManager.getTimesTable(context);
		
		String algorithmName = Algorithms.ARIMA;
		
		String fsPath = getModelFsPath(table, algorithmName, modelName, modelStage);
		if (fsPath == null) return null;
		/*
		 * Leverage Apache Spark mechanism to read the ARIMA model
		 * from a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		return ARIMAModel.load(modelPath);
		
	}
	
	public AutoARIMAModel readAutoARIMA(SparkExecutionPluginContext context, String modelName, String modelStage) throws Exception {

		FileSet fs = SparkMLManager.getTimeFS(context);
		Table table = SparkMLManager.getTimesTable(context);
		
		String algorithmName = Algorithms.AUTO_ARIMA;
		
		String fsPath = getModelFsPath(table, algorithmName, modelName, modelStage);
		if (fsPath == null) return null;
		/*
		 * Leverage Apache Spark mechanism to read the AutoARIMA model
		 * from a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		return AutoARIMAModel.load(modelPath);
		
	}

	/** WRITE **/
	
	public void trackARIMA(SparkExecutionPluginContext context, String modelName, String modelStage, String modelParams, String modelMetrics,
			ARIMAModel model) throws Exception {
		
		String algorithmName = Algorithms.ARIMA;

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
	
	public void trackAutoARIMA(SparkExecutionPluginContext context, String modelName, String modelStage, String modelParams, String modelMetrics,
			AutoARIMAModel model) throws Exception {
		
		String algorithmName = Algorithms.AUTO_ARIMA;

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

}
