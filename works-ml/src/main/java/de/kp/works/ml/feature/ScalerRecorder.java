package de.kp.works.ml.feature;
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

import org.apache.spark.ml.feature.MaxAbsScalerModel;
import org.apache.spark.ml.feature.MinMaxScalerModel;
import org.apache.spark.ml.feature.StandardScalerModel;

import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.Algorithms;
import de.kp.works.core.ml.FeatureRecorder;
import de.kp.works.core.ml.SparkMLManager;
/*
 * This class provides management support in common for
 * those scaler models:
 * 
 * - MinMax Scaler
 * - MaxAbs Scaler
 * - Standard Scaler
 * 
 */
public class ScalerRecorder extends FeatureRecorder {

	/** MIN MAX **/

	public MinMaxScalerModel readMinMaxScaler(SparkExecutionPluginContext context, String modelName, String modelStage, String modelOption) throws Exception {

		FileSet fs = SparkMLManager.getFeatureFS(context);
		Table table = SparkMLManager.getFeatureTable(context);
		
		String algorithmName = Algorithms.MIN_MAX_SCALER;
		
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
		 * Leverage Apache Spark mechanism to read the MinMax Scaler model
		 * from a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		return MinMaxScalerModel.load(modelPath);
		
	}

	public void trackMinMaxScaler(SparkExecutionPluginContext context, String modelName, String modelStage, String modelParams, String modelMetrics,
			MinMaxScalerModel model) throws Exception {
		
		String algorithmName = Algorithms.MIN_MAX_SCALER;

		/***** ARTIFACTS *****/

		Long ts = new Date().getTime();
		String fsPath = algorithmName + "/" + ts.toString() + "/" + modelName;

		FileSet fs = SparkMLManager.getFeatureFS(context);

		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		model.save(modelPath);

		/***** METADATA *****/

		String modelPack = "WorksML";
		Table table = SparkMLManager.getFeatureTable(context);

		setMetadata(ts, table, algorithmName, modelName, modelPack, modelStage, modelParams, modelMetrics, fsPath);
		
	}

	/** MAX ABS **/

	public MaxAbsScalerModel readMaxAbsScaler(SparkExecutionPluginContext context, String modelName, String modelStage, String modelOption) throws Exception {

		FileSet fs = SparkMLManager.getFeatureFS(context);
		Table table = SparkMLManager.getFeatureTable(context);
		
		String algorithmName = Algorithms.MAX_ABS_SCALER;
		
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
		 * Leverage Apache Spark mechanism to read the MinMax Scaler model
		 * from a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		return MaxAbsScalerModel.load(modelPath);
		
	}
	
	public void trackMaxAbsScaler(SparkExecutionPluginContext context, String modelName, String modelStage, String modelParams, String modelMetrics,
			MaxAbsScalerModel model) throws Exception {
		
		String algorithmName = Algorithms.MAX_ABS_SCALER;

		/***** ARTIFACTS *****/

		Long ts = new Date().getTime();
		String fsPath = algorithmName + "/" + ts.toString() + "/" + modelName;

		FileSet fs = SparkMLManager.getFeatureFS(context);

		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		model.save(modelPath);

		/***** METADATA *****/

		String modelPack = "WorksML";
		Table table = SparkMLManager.getFeatureTable(context);

		setMetadata(ts, table, algorithmName, modelName, modelPack, modelStage, modelParams, modelMetrics, fsPath);
		
	}

	/** STANDARD **/

	public StandardScalerModel readStandardScaler(SparkExecutionPluginContext context, String modelName, String modelStage, String modelOption) throws Exception {

		FileSet fs = SparkMLManager.getFeatureFS(context);
		Table table = SparkMLManager.getFeatureTable(context);

		String algorithmName = Algorithms.STANDARD_SCALER;
		
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
		 * Leverage Apache Spark mechanism to read the Standard Scaler model
		 * from a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		return StandardScalerModel.load(modelPath);
		
	}
	
	public void trackStandardScaler(SparkExecutionPluginContext context, String modelName, String modelStage, String modelParams, String modelMetrics,
			StandardScalerModel model) throws Exception {

		String algorithmName = Algorithms.STANDARD_SCALER;

		/***** ARTIFACTS *****/

		Long ts = new Date().getTime();
		String fsPath = algorithmName + "/" + ts.toString() + "/" + modelName;

		FileSet fs = SparkMLManager.getFeatureFS(context);

		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		model.save(modelPath);

		/***** METADATA *****/

		String modelPack = "WorksML";
		Table table = SparkMLManager.getFeatureTable(context);

		setMetadata(ts, table, algorithmName, modelName, modelPack, modelStage, modelParams, modelMetrics, fsPath);
		
	}

}
