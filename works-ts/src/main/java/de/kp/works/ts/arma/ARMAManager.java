package de.kp.works.ts.arma;
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
import de.kp.works.core.ml.AbstractTimeSeriesManager;
import de.kp.works.core.ml.SparkMLManager;
import de.kp.works.ts.model.ARMAModel;
import de.kp.works.ts.model.AutoARMAModel;

public class ARMAManager extends AbstractTimeSeriesManager {

	/** READ **/
	
	public ARMAModel readARMA(SparkExecutionPluginContext context, String modelName) throws Exception {

		FileSet fs = SparkMLManager.getTimeseriesFS(context);
		Table table = SparkMLManager.getTimeseriesMeta(context);
		
		return readARMA(fs, table, modelName);
		
	}
	
	private ARMAModel readARMA(FileSet fs, Table table, String modelName) throws IOException {
		
		String algorithmName = "ARMA";
		
		String fsPath = getModelFsPath(table, algorithmName, modelName);
		if (fsPath == null) return null;
		/*
		 * Leverage Apache Spark mechanism to read the ARMA model
		 * from a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		return ARMAModel.load(modelPath);
		
	}
	
	public AutoARMAModel readAutoARMA(SparkExecutionPluginContext context, String modelName) throws Exception {

		FileSet fs = SparkMLManager.getTimeseriesFS(context);
		Table table = SparkMLManager.getTimeseriesMeta(context);
		
		return readAutoARMA(fs, table, modelName);
		
	}

	private AutoARMAModel readAutoARMA(FileSet fs, Table table, String modelName) throws IOException {
		
		String algorithmName = "AutoARMA";
		
		String fsPath = getModelFsPath(table, algorithmName, modelName);
		if (fsPath == null) return null;
		/*
		 * Leverage Apache Spark mechanism to read the AutoARMA model
		 * from a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		return AutoARMAModel.load(modelPath);
		
	}

	/** WRITE **/
	
	public void saveARMA(SparkExecutionPluginContext context, String modelName, String modelParams, String modelMetrics,
			ARMAModel model) throws Exception {

		FileSet fs = SparkMLManager.getTimeseriesFS(context);
		Table table = SparkMLManager.getTimeseriesMeta(context);
		
		saveARMA(fs, table, modelName, modelParams, modelMetrics, model);
		
	}
	
	private void saveARMA(FileSet fs, Table table, String modelName, String modelParams, String modelMetrics,
			ARMAModel model) throws IOException {
		
		String algorithmName = "ARMA";

		/***** MODEL COMPONENTS *****/

		/*
		 * Define the path of this model on CDAP's internal timeseries fileset;
		 * not, the timestamp within the path ensures that each model of the 
		 * same name but different version has its own path
		 */
		Long ts = new Date().getTime();
		String fsPath = algorithmName + "/" + ts.toString() + "/" + modelName;
		/*
		 * Leverage Apache Spark mechanism to write the ARMA model
		 * to a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		model.save(modelPath);

		/***** MODEL METADATA *****/

		setMetadata(ts, table, algorithmName, modelName, modelParams, modelMetrics, fsPath);

	}
	
	public void saveAutoARMA(SparkExecutionPluginContext context, String modelName, String modelParams, String modelMetrics,
			AutoARMAModel model) throws Exception {

		FileSet fs = SparkMLManager.getTimeseriesFS(context);
		Table table = SparkMLManager.getTimeseriesMeta(context);
		
		saveAutoARMA(fs, table, modelName, modelParams, modelMetrics, model);
		
	}
	
	private void saveAutoARMA(FileSet fs, Table table, String modelName, String modelParams, String modelMetrics,
			AutoARMAModel model) throws IOException {
		
		String algorithmName = "AutoARMA";

		/***** MODEL COMPONENTS *****/

		/*
		 * Define the path of this model on CDAP's internal timeseries fileset;
		 * not, the timestamp within the path ensures that each model of the 
		 * same name but different version has its own path
		 */
		Long ts = new Date().getTime();
		String fsPath = algorithmName + "/" + ts.toString() + "/" + modelName;
		/*
		 * Leverage Apache Spark mechanism to write the AutoARMA model
		 * to a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		model.save(modelPath);

		/***** MODEL METADATA *****/

		setMetadata(ts, table, algorithmName, modelName, modelParams, modelMetrics, fsPath);

	}

}
