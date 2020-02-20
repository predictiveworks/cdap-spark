package de.kp.works.ts.ar;
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
import de.kp.works.ts.model.ARYuleWalkerModel;
import de.kp.works.ts.model.AutoARModel;
import de.kp.works.ts.model.AutoRegressionModel;
import de.kp.works.ts.model.DiffAutoRegressionModel;

public class ARManager extends AbstractTimeSeriesManager {

	/** READ **/
	
	public AutoRegressionModel readAR(SparkExecutionPluginContext context, String modelName) throws Exception {

		FileSet fs = SparkMLManager.getTimeseriesFS(context);
		Table table = SparkMLManager.getTimeseriesMeta(context);
		
		return readAR(fs, table, modelName);
		
	}
		
	private AutoRegressionModel readAR(FileSet fs, Table table, String modelName) throws IOException {
		
		String algorithmName = "AR";
		
		String fsPath = getModelFsPath(table, algorithmName, modelName);
		if (fsPath == null) return null;
		/*
		 * Leverage Apache Spark mechanism to read the AutoRegression model
		 * from a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		return AutoRegressionModel.load(modelPath);
		
	}
	
	public AutoARModel readAutoAR(SparkExecutionPluginContext context, String modelName) throws Exception {

		FileSet fs = SparkMLManager.getTimeseriesFS(context);
		Table table = SparkMLManager.getTimeseriesMeta(context);
		
		return readAutoAR(fs, table, modelName);
		
	}

	private AutoARModel readAutoAR(FileSet fs, Table table, String modelName) throws IOException {
		
		String algorithmName = "AutoAR";
		
		String fsPath = getModelFsPath(table, algorithmName, modelName);
		if (fsPath == null) return null;
		/*
		 * Leverage Apache Spark mechanism to read the AutoAR model
		 * from a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		return AutoARModel.load(modelPath);
		
	}
	
	public DiffAutoRegressionModel readDiffAR(SparkExecutionPluginContext context, String modelName) throws Exception {

		FileSet fs = SparkMLManager.getTimeseriesFS(context);
		Table table = SparkMLManager.getTimeseriesMeta(context);
		
		return readDiffAR(fs, table, modelName);
		
	}

	private DiffAutoRegressionModel readDiffAR(FileSet fs, Table table, String modelName) throws IOException {
		
		String algorithmName = "DiffAR";
		
		String fsPath = getModelFsPath(table, algorithmName, modelName);
		if (fsPath == null) return null;
		/*
		 * Leverage Apache Spark mechanism to read the DiffAutoRegression model
		 * from a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		return DiffAutoRegressionModel.load(modelPath);
		
	}
	
	public ARYuleWalkerModel readYuleWalker(SparkExecutionPluginContext context, String modelName) throws Exception {

		FileSet fs = SparkMLManager.getTimeseriesFS(context);
		Table table = SparkMLManager.getTimeseriesMeta(context);
		
		return readYuleWalker(fs, table, modelName);
		
	}

	private ARYuleWalkerModel readYuleWalker(FileSet fs, Table table, String modelName) throws IOException {
		
		String algorithmName = "YuleWalker";
		
		String fsPath = getModelFsPath(table, algorithmName, modelName);
		if (fsPath == null) return null;
		/*
		 * Leverage Apache Spark mechanism to read the ARYuleWalker model
		 * from a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		return ARYuleWalkerModel.load(modelPath);
		
	}

	/** WRITE **/
	
	public void saveAR(SparkExecutionPluginContext context, String modelName, String modelParams, String modelMetrics,
			AutoRegressionModel model) throws Exception {

		FileSet fs = SparkMLManager.getTimeseriesFS(context);
		Table table = SparkMLManager.getTimeseriesMeta(context);
		
		saveAR(fs, table, modelName, modelParams, modelMetrics, model);
		
	}

		
	private void saveAR(FileSet fs, Table table, String modelName, String modelParams, String modelMetrics,
			AutoRegressionModel model) throws IOException {
		
		String algorithmName = "AR";

		/***** MODEL COMPONENTS *****/

		/*
		 * Define the path of this model on CDAP's internal timeseries fileset;
		 * not, the timestamp within the path ensures that each model of the 
		 * same name but different version has its own path
		 */
		Long ts = new Date().getTime();
		String fsPath = algorithmName + "/" + ts.toString() + "/" + modelName;
		/*
		 * Leverage Apache Spark mechanism to write the AutoRegression model
		 * to a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		model.save(modelPath);

		/***** MODEL METADATA *****/

		setMetadata(ts, table, algorithmName, modelName, modelParams, modelMetrics, fsPath);

	}
	
	public void saveAutoAR(SparkExecutionPluginContext context, String modelName, String modelParams, String modelMetrics,
			AutoARModel model) throws Exception {

		FileSet fs = SparkMLManager.getTimeseriesFS(context);
		Table table = SparkMLManager.getTimeseriesMeta(context);
		
		saveAutoAR(fs, table, modelName, modelParams, modelMetrics, model);
		
	}
	
	private void saveAutoAR(FileSet fs, Table table, String modelName, String modelParams, String modelMetrics,
			AutoARModel model) throws IOException {
		
		String algorithmName = "AutoAR";

		/***** MODEL COMPONENTS *****/

		/*
		 * Define the path of this model on CDAP's internal timeseries fileset;
		 * not, the timestamp within the path ensures that each model of the 
		 * same name but different version has its own path
		 */
		Long ts = new Date().getTime();
		String fsPath = algorithmName + "/" + ts.toString() + "/" + modelName;
		/*
		 * Leverage Apache Spark mechanism to write the AutoAR model
		 * to a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		model.save(modelPath);

		/***** MODEL METADATA *****/

		setMetadata(ts, table, algorithmName, modelName, modelParams, modelMetrics, fsPath);

	}
	
	public void saveDiffAR(SparkExecutionPluginContext context, String modelName, String modelParams, String modelMetrics,
			DiffAutoRegressionModel model) throws Exception {

		FileSet fs = SparkMLManager.getTimeseriesFS(context);
		Table table = SparkMLManager.getTimeseriesMeta(context);
		
		saveDiffAR(fs, table, modelName, modelParams, modelMetrics, model);
		
	}
	
	private void saveDiffAR(FileSet fs, Table table, String modelName, String modelParams, String modelMetrics,
			DiffAutoRegressionModel model) throws IOException {
		
		String algorithmName = "DiffAR";

		/***** MODEL COMPONENTS *****/

		/*
		 * Define the path of this model on CDAP's internal timeseries fileset;
		 * not, the timestamp within the path ensures that each model of the 
		 * same name but different version has its own path
		 */
		Long ts = new Date().getTime();
		String fsPath = algorithmName + "/" + ts.toString() + "/" + modelName;
		/*
		 * Leverage Apache Spark mechanism to write the DiffAutoRegression model
		 * to a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		model.save(modelPath);

		/***** MODEL METADATA *****/

		setMetadata(ts, table, algorithmName, modelName, modelParams, modelMetrics, fsPath);

	}
	
	public void saveYuleWalker(SparkExecutionPluginContext context, String modelName, String modelParams, String modelMetrics,
			ARYuleWalkerModel model) throws Exception {

		FileSet fs = SparkMLManager.getTimeseriesFS(context);
		Table table = SparkMLManager.getTimeseriesMeta(context);
		
		saveYuleWalker(fs, table, modelName, modelParams, modelMetrics, model);
		
	}
	
	private void saveYuleWalker(FileSet fs, Table table, String modelName, String modelParams, String modelMetrics,
			ARYuleWalkerModel model) throws IOException {
		
		String algorithmName = "YuleWalker";

		/***** MODEL COMPONENTS *****/

		/*
		 * Define the path of this model on CDAP's internal timeseries fileset;
		 * not, the timestamp within the path ensures that each model of the 
		 * same name but different version has its own path
		 */
		Long ts = new Date().getTime();
		String fsPath = algorithmName + "/" + ts.toString() + "/" + modelName;
		/*
		 * Leverage Apache Spark mechanism to write the ARYuleWalker model
		 * to a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		model.save(modelPath);

		/***** MODEL METADATA *****/

		setMetadata(ts, table, algorithmName, modelName, modelParams, modelMetrics, fsPath);

	}

}
