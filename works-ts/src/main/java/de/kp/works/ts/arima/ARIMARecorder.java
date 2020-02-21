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

import java.io.IOException;
import java.util.Date;

import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.ml.TimeRecorder;
import de.kp.works.core.ml.SparkMLManager;
import de.kp.works.ts.model.ARIMAModel;
import de.kp.works.ts.model.AutoARIMAModel;

public class ARIMARecorder extends TimeRecorder {

	/** READ **/
	
	public ARIMAModel readARIMA(SparkExecutionPluginContext context, String modelName) throws Exception {

		FileSet fs = SparkMLManager.getTimeFS(context);
		Table table = SparkMLManager.getTimesTable(context);
		
		return readARIMA(fs, table, modelName);
		
	}
	
	private ARIMAModel readARIMA(FileSet fs, Table table, String modelName) throws IOException {
		
		String algorithmName = "ARIMA";
		
		String fsPath = getModelFsPath(table, algorithmName, modelName);
		if (fsPath == null) return null;
		/*
		 * Leverage Apache Spark mechanism to read the ARIMA model
		 * from a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		return ARIMAModel.load(modelPath);
		
	}
	
	public AutoARIMAModel readAutoARIMA(SparkExecutionPluginContext context, String modelName) throws Exception {

		FileSet fs = SparkMLManager.getTimeFS(context);
		Table table = SparkMLManager.getTimesTable(context);
		
		return readAutoARIMA(fs, table, modelName);
		
	}

	private AutoARIMAModel readAutoARIMA(FileSet fs, Table table, String modelName) throws IOException {
		
		String algorithmName = "AutoARIMA";
		
		String fsPath = getModelFsPath(table, algorithmName, modelName);
		if (fsPath == null) return null;
		/*
		 * Leverage Apache Spark mechanism to read the AutoARIMA model
		 * from a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		return AutoARIMAModel.load(modelPath);
		
	}

	/** WRITE **/
	
	public void trackARIMA(SparkExecutionPluginContext context, String modelName, String modelParams, String modelMetrics,
			ARIMAModel model) throws Exception {

		FileSet fs = SparkMLManager.getTimeFS(context);
		Table table = SparkMLManager.getTimesTable(context);
		
		saveARIMA(fs, table, modelName, modelParams, modelMetrics, model);
		
	}
	
	private void saveARIMA(FileSet fs, Table table, String modelName, String modelParams, String modelMetrics,
			ARIMAModel model) throws IOException {
		
		String algorithmName = "ARIMA";

		/***** MODEL COMPONENTS *****/

		/*
		 * Define the path of this model on CDAP's internal timeseries fileset;
		 * not, the timestamp within the path ensures that each model of the 
		 * same name but different version has its own path
		 */
		Long ts = new Date().getTime();
		String fsPath = algorithmName + "/" + ts.toString() + "/" + modelName;
		/*
		 * Leverage Apache Spark mechanism to write the ARIMA model
		 * to a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		model.save(modelPath);

		/***** MODEL METADATA *****/

		setMetadata(ts, table, algorithmName, modelName, modelParams, modelMetrics, fsPath);

	}
	
	public void trackAutoARIMA(SparkExecutionPluginContext context, String modelName, String modelParams, String modelMetrics,
			AutoARIMAModel model) throws Exception {

		FileSet fs = SparkMLManager.getTimeFS(context);
		Table table = SparkMLManager.getTimesTable(context);
		
		saveAutoARIMA(fs, table, modelName, modelParams, modelMetrics, model);
		
	}
	
	private void saveAutoARIMA(FileSet fs, Table table, String modelName, String modelParams, String modelMetrics,
			AutoARIMAModel model) throws IOException {
		
		String algorithmName = "AutoARIMA";

		/***** MODEL COMPONENTS *****/

		/*
		 * Define the path of this model on CDAP's internal timeseries fileset;
		 * not, the timestamp within the path ensures that each model of the 
		 * same name but different version has its own path
		 */
		Long ts = new Date().getTime();
		String fsPath = algorithmName + "/" + ts.toString() + "/" + modelName;
		/*
		 * Leverage Apache Spark mechanism to write the AutoARIMA model
		 * to a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		model.save(modelPath);

		/***** MODEL METADATA *****/

		setMetadata(ts, table, algorithmName, modelName, modelParams, modelMetrics, fsPath);

	}

}
