package de.kp.works.core.recording.classification;

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
import org.apache.spark.ml.classification.RandomForestClassificationModel;

import java.util.Date;

public class RFCRecorder extends ClassifierRecorder {

	public RFCRecorder(ConfigReader configReader) {
		super(configReader);
		algoName = Algorithms.RANDOM_FOREST_TREE;
	}

	public RandomForestClassificationModel read(SparkExecutionPluginContext context, String modelName, String modelStage, String modelOption) throws Exception {

		String modelPath = getModelPath(context, modelName, modelStage, modelOption);
		if (modelPath == null) return null;

		return RandomForestClassificationModel.load(modelPath);

	}

	public void track(SparkExecutionPluginContext context, String modelName, String modelStage, String modelParams, String modelMetrics,
			RandomForestClassificationModel model) throws Exception {

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
