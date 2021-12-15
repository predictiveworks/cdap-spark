package de.kp.works.core.recording.regression;

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
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import org.apache.spark.ml.regression.LinearRegressionModel;

import java.util.Date;

public class LinearRecorder extends RegressorRecorder {

	public LinearRecorder(ConfigReader configReader) {
		super(configReader);
		algoName = Algorithms.LINEAR_REGRESSION;
	}

	public LinearRegressionModel read(SparkExecutionPluginContext context, String modelName, String modelStage, String modelOption) throws Exception {

		String modelPath = getModelPath(context, modelName, modelStage, modelOption);
		if (modelPath == null) return null;
		/*
		 * Leverage Apache Spark mechanism to read the LinearRegression model
		 * from a model specific file set
		 */
		return LinearRegressionModel.load(modelPath);
		
	}

	public void track(SparkExecutionPluginContext context, String modelName, String modelStage, String modelParams, String modelMetrics,
			LinearRegressionModel model) throws Exception {

		/* ARTIFACTS */

		long ts = new Date().getTime();
		String fsPath = algoName + "/" + ts + "/" + modelName;

		String modelPath = buildModelPath(context, fsPath);
		model.save(modelPath);

		/* METADATA */

		String modelPack = "WorksML";

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

