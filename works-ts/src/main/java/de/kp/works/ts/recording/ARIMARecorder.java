package de.kp.works.ts.recording;
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

import de.kp.works.core.Algorithms;
import de.kp.works.core.configuration.ConfigReader;
import de.kp.works.core.model.ModelSpec;
import de.kp.works.core.recording.TimeRecorder;
import de.kp.works.ts.model.ARIMAModel;
import de.kp.works.ts.model.AutoARIMAModel;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;

import java.util.Date;

public class ARIMARecorder extends TimeRecorder {

	public ARIMARecorder(ConfigReader configReader) {
		super(configReader);
	}

	/** READ **/
	
	public ARIMAModel readARIMA(SparkExecutionPluginContext context, String modelName, String modelStage, String modelOption) throws Exception {

		algoName = Algorithms.ARIMA;

		String modelPath = getModelPath(context, algoName, modelName, modelStage, modelOption);
		if (modelPath == null) return null;
		/*
		 * Leverage Apache Spark mechanism to read the ARIMA model
		 * from a model specific file set
		 */
		return ARIMAModel.load(modelPath);
		
	}
	
	public AutoARIMAModel readAutoARIMA(SparkExecutionPluginContext context, String modelName, String modelStage, String modelOption) throws Exception {

		algoName = Algorithms.AUTO_ARIMA;

		String modelPath = getModelPath(context, algoName, modelName, modelStage, modelOption);
		if (modelPath == null) return null;
		/*
		 * Leverage Apache Spark mechanism to read the AutoARIMA model
		 * from a model specific file set
		 */
		return AutoARIMAModel.load(modelPath);
		
	}

	/** WRITE **/
	
	public void trackARIMA(SparkExecutionPluginContext context, String modelName, String modelStage, String modelParams, String modelMetrics,
			ARIMAModel model) throws Exception {

		algoName = Algorithms.ARIMA;

		/* ARTIFACTS */

		long ts = new Date().getTime();
		String fsPath = algoName + "/" + ts + "/" + modelName;

		String modelPath = buildModelPath(context, fsPath);
		model.save(modelPath);

		/* METADATA */

		String modelPack = "WorksTS";

		ModelSpec modelSpec = new ModelSpec();
		modelSpec.setTs(ts);

		modelSpec.setAlgoName(algoName);
		modelSpec.setModelName(modelName);

		modelSpec.setModelPack(modelPack);
		modelSpec.setModelStage(modelStage);

		modelSpec.setModelParams(modelParams);
		modelSpec.setModelMetrics(modelMetrics);

		modelSpec.setFsPath(fsPath);
		setMetadata(context, modelSpec);
		
	}
	
	public void trackAutoARIMA(SparkExecutionPluginContext context, String modelName, String modelStage, String modelParams, String modelMetrics,
			AutoARIMAModel model) throws Exception {

		algoName = Algorithms.AUTO_ARIMA;

		/* ARTIFACTS */

		long ts = new Date().getTime();
		String fsPath = algoName + "/" + ts + "/" + modelName;

		String modelPath = buildModelPath(context, fsPath);
		model.save(modelPath);

		/* METADATA */

		String modelPack = "WorksTS";

		ModelSpec modelSpec = new ModelSpec();
		modelSpec.setTs(ts);

		modelSpec.setAlgoName(algoName);
		modelSpec.setModelName(modelName);

		modelSpec.setModelPack(modelPack);
		modelSpec.setModelStage(modelStage);

		modelSpec.setModelParams(modelParams);
		modelSpec.setModelMetrics(modelMetrics);

		modelSpec.setFsPath(fsPath);
		setMetadata(context, modelSpec);
		
	}

}
