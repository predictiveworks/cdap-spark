package de.kp.works.core.recording.feature;
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
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

import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.Algorithms;
import de.kp.works.core.recording.feature.FeatureRecorder;
import de.kp.works.core.recording.SparkMLManager;
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
		
		String algorithmName = Algorithms.MIN_MAX_SCALER;

		String modelPath = getModelPath(context, algorithmName, modelName, modelStage, modelOption);
		if (modelPath == null) return null;
		/*
		 * Leverage Apache Spark mechanism to read the MinMax Scaler model
		 * from a model specific file set
		 */
		return MinMaxScalerModel.load(modelPath);
		
	}

	public void trackMinMaxScaler(SparkExecutionPluginContext context, String modelName, String modelStage, String modelParams, String modelMetrics,
			MinMaxScalerModel model) throws Exception {
		
		String algorithmName = Algorithms.MIN_MAX_SCALER;

		/* ARTIFACTS */

		long ts = new Date().getTime();
		String fsPath = algorithmName + "/" + ts + "/" + modelName;

		FileSet fs = SparkMLManager.getFeatureFS(context);

		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		model.save(modelPath);

		/* METADATA */

		String modelPack = "WorksML";

		Table table = SparkMLManager.getFeatureTable(context);
		String namespace = context.getNamespace();

		setMetadata(ts, table, namespace, algorithmName, modelName, modelPack, modelStage, modelParams, modelMetrics, fsPath);
		
	}

	/** MAX ABS **/

	public MaxAbsScalerModel readMaxAbsScaler(SparkExecutionPluginContext context, String modelName, String modelStage, String modelOption) throws Exception {
		
		String algorithmName = Algorithms.MAX_ABS_SCALER;

		String modelPath = getModelPath(context, algorithmName, modelName, modelStage, modelOption);
		if (modelPath == null) return null;
		/*
		 * Leverage Apache Spark mechanism to read the MinMax Scaler model
		 * from a model specific file set
		 */
		return MaxAbsScalerModel.load(modelPath);
		
	}
	
	public void trackMaxAbsScaler(SparkExecutionPluginContext context, String modelName, String modelStage, String modelParams, String modelMetrics,
			MaxAbsScalerModel model) throws Exception {
		
		String algorithmName = Algorithms.MAX_ABS_SCALER;

		/* ARTIFACTS */

		long ts = new Date().getTime();
		String fsPath = algorithmName + "/" + ts + "/" + modelName;

		FileSet fs = SparkMLManager.getFeatureFS(context);

		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		model.save(modelPath);

		/* METADATA */

		String modelPack = "WorksML";

		Table table = SparkMLManager.getFeatureTable(context);
		String namespace = context.getNamespace();

		setMetadata(ts, table, namespace, algorithmName, modelName, modelPack, modelStage, modelParams, modelMetrics, fsPath);
		
	}

	/** STANDARD **/

	public StandardScalerModel readStandardScaler(SparkExecutionPluginContext context, String modelName, String modelStage, String modelOption) throws Exception {

		String algorithmName = Algorithms.STANDARD_SCALER;

		String modelPath = getModelPath(context, algorithmName, modelName, modelStage, modelOption);
		if (modelPath == null) return null;
		/*
		 * Leverage Apache Spark mechanism to read the Standard Scaler model
		 * from a model specific file set
		 */
		return StandardScalerModel.load(modelPath);
		
	}
	
	public void trackStandardScaler(SparkExecutionPluginContext context, String modelName, String modelStage, String modelParams, String modelMetrics,
			StandardScalerModel model) throws Exception {

		String algorithmName = Algorithms.STANDARD_SCALER;

		/* ARTIFACTS */

		long ts = new Date().getTime();
		String fsPath = algorithmName + "/" + ts + "/" + modelName;

		FileSet fs = SparkMLManager.getFeatureFS(context);

		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		model.save(modelPath);

		/* METADATA */

		String modelPack = "WorksML";

		Table table = SparkMLManager.getFeatureTable(context);
		String namespace = context.getNamespace();

		setMetadata(ts, table, namespace, algorithmName, modelName, modelPack, modelStage, modelParams, modelMetrics, fsPath);
		
	}

}
