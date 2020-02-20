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

import java.io.IOException;
import java.util.Date;

import org.apache.spark.ml.feature.MaxAbsScalerModel;
import org.apache.spark.ml.feature.MinMaxScalerModel;
import org.apache.spark.ml.feature.StandardScalerModel;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.ml.AbstractModelManager;
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
public class ScalerRecorder extends AbstractModelManager {

	/** MIN MAX **/

	public MinMaxScalerModel readMinMaxScaler(SparkExecutionPluginContext context, String modelName) throws Exception {

		FileSet fs = SparkMLManager.getFeatureFS(context);
		Table table = SparkMLManager.getFeatureMeta(context);
		
		return readMinMaxScaler(fs, table, modelName);
		
	}

	private MinMaxScalerModel readMinMaxScaler(FileSet fs, Table table, String modelName) throws IOException {
		
		String fsPath = getModelFsPath(table, "MinMaxScaler", modelName);
		if (fsPath == null) return null;
		/*
		 * Leverage Apache Spark mechanism to read the MinMax Scaler model
		 * from a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		return MinMaxScalerModel.load(modelPath);
		
	}

	public void trackMinMaxScaler(SparkExecutionPluginContext context, String modelName, String modelParams, String modelMetrics,
			MinMaxScalerModel model) throws Exception {

		FileSet fs = SparkMLManager.getFeatureFS(context);
		Table table = SparkMLManager.getFeatureMeta(context);
		
		saveMinMaxScaler(fs, table, modelName, modelParams, modelMetrics, model);
		
	}

	private void saveMinMaxScaler(FileSet modelFs, Table modelMeta, String modelName, String modelParams, String modelMetrics,
			MinMaxScalerModel model) throws IOException {

		String algorithmName = "MinMaxScaler";
		/***** MODEL COMPONENTS *****/

		/*
		 * Define the path of this model on CDAP's internal feature fileset
		 */
		Long ts = new Date().getTime();
		String fsPath = algorithmName + "/" + ts.toString() + "/" + modelName;
		/*
		 * Leverage Apache Spark mechanism to write the MinMax Scaler model
		 * to a model specific file set
		 */
		String modelPath = modelFs.getBaseLocation().append(fsPath).toURI().getPath();
		model.save(modelPath);

		/***** MODEL METADATA *****/

		/*
		 * Append model metadata to the metadata table associated with the
		 * clustering fileset
		 */
		String fsName = SparkMLManager.FEATURE_FS;
		String modelVersion = getModelVersion(modelMeta, algorithmName, modelName);

		byte[] key = Bytes.toBytes(ts);
		modelMeta.put(new Put(key).add("timestamp", ts).add("name", modelName).add("version", modelVersion)
				.add("algorithm", algorithmName).add("params", modelParams).add("metrics", modelMetrics)
				.add("fsName", fsName).add("fsPath", fsPath));

	}

	/** MAX ABS **/

	public MaxAbsScalerModel readMaxAbsScaler(SparkExecutionPluginContext context, String modelName) throws Exception {

		FileSet fs = SparkMLManager.getFeatureFS(context);
		Table table = SparkMLManager.getFeatureMeta(context);
		
		return readMaxAbsScaler(fs, table, modelName);
		
	}
	
	private MaxAbsScalerModel readMaxAbsScaler(FileSet fs, Table table, String modelName) throws IOException {
		
		String fsPath = getModelFsPath(table, "MaxAbsScaler", modelName);
		if (fsPath == null) return null;
		/*
		 * Leverage Apache Spark mechanism to read the MinMax Scaler model
		 * from a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		return MaxAbsScalerModel.load(modelPath);
		
	}
	
	public void trackMaxAbsScaler(SparkExecutionPluginContext context, String modelName, String modelParams, String modelMetrics,
			MaxAbsScalerModel model) throws Exception {

		FileSet fs = SparkMLManager.getFeatureFS(context);
		Table table = SparkMLManager.getFeatureMeta(context);
		
		saveMaxAbsScaler(fs, table, modelName, modelParams, modelMetrics, model);
		
	}

	private void saveMaxAbsScaler(FileSet modelFs, Table modelMeta, String modelName, String modelParams, String modelMetrics,
			MaxAbsScalerModel model) throws IOException {

		String algorithmName = "MaxAbsScaler";
		
		/***** MODEL COMPONENTS *****/

		/*
		 * Define the path of this model on CDAP's internal feature fileset
		 */
		Long ts = new Date().getTime();
		String fsPath = algorithmName + "/" + ts.toString() + "/" + modelName;
		/*
		 * Leverage Apache Spark mechanism to write the MaxAbs Scaler model
		 * to a model specific file set
		 */
		String modelPath = modelFs.getBaseLocation().append(fsPath).toURI().getPath();
		model.save(modelPath);

		/***** MODEL METADATA *****/

		/*
		 * Append model metadata to the metadata table associated with the
		 * clustering fileset
		 */
		String fsName = SparkMLManager.FEATURE_FS;
		String modelVersion = getModelVersion(modelMeta, algorithmName, modelName);

		byte[] key = Bytes.toBytes(ts);
		modelMeta.put(new Put(key).add("timestamp", ts).add("name", modelName).add("version", modelVersion)
				.add("algorithm", algorithmName).add("params", modelParams).add("metrics", modelMetrics)
				.add("fsName", fsName).add("fsPath", fsPath));

	}

	/** STANDARD **/

	public StandardScalerModel readStandardScaler(SparkExecutionPluginContext context, String modelName) throws Exception {

		FileSet fs = SparkMLManager.getFeatureFS(context);
		Table table = SparkMLManager.getFeatureMeta(context);
		
		return readStandardScaler(fs, table, modelName);
		
	}
	
	private StandardScalerModel readStandardScaler(FileSet fs, Table table, String modelName) throws IOException {
		
		String fsPath = getModelFsPath(table, "StandardScaler", modelName);
		if (fsPath == null) return null;
		/*
		 * Leverage Apache Spark mechanism to read the Standard Scaler model
		 * from a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		return StandardScalerModel.load(modelPath);
		
	}
	
	public void trackStandardScaler(SparkExecutionPluginContext context, String modelName, String modelParams, String modelMetrics,
			StandardScalerModel model) throws Exception {

		FileSet fs = SparkMLManager.getFeatureFS(context);
		Table table = SparkMLManager.getFeatureMeta(context);
		
		saveStandardScaler(fs, table, modelName, modelParams, modelMetrics, model);
		
	}

	private void saveStandardScaler(FileSet modelFs, Table modelMeta, String modelName, String modelParams, String modelMetrics,
			StandardScalerModel model) throws IOException {

		String algorithmName = "StandardScaler";
		
		/***** MODEL COMPONENTS *****/

		/*
		 * Define the path of this model on CDAP's internal feature fileset
		 */
		Long ts = new Date().getTime();
		String fsPath = algorithmName + "/" + ts.toString() + "/" + modelName;
		/*
		 * Leverage Apache Spark mechanism to write the MaxAbs Scaler model
		 * to a model specific file set
		 */
		String modelPath = modelFs.getBaseLocation().append(fsPath).toURI().getPath();
		model.save(modelPath);

		/***** MODEL METADATA *****/

		/*
		 * Append model metadata to the metadata table associated with the
		 * clustering fileset
		 */
		String fsName = SparkMLManager.FEATURE_FS;
		String modelVersion = getModelVersion(modelMeta, algorithmName, modelName);

		byte[] key = Bytes.toBytes(ts);
		modelMeta.put(new Put(key).add("timestamp", ts).add("name", modelName).add("version", modelVersion)
				.add("algorithm", algorithmName).add("params", modelParams).add("metrics", modelMetrics)
				.add("fsName", fsName).add("fsPath", fsPath));

	}

}
