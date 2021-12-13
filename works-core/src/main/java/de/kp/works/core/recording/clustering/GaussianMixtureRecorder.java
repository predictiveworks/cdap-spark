package de.kp.works.core.recording.clustering;
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
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import org.apache.spark.ml.clustering.GaussianMixtureModel;

import java.util.Date;

public class GaussianMixtureRecorder extends ClusterRecorder {

	public GaussianMixtureRecorder() {
		super();
		algoName = Algorithms.GAUSSIAN_MIXTURE;
	}

	public GaussianMixtureModel read(SparkExecutionPluginContext context, String modelName, String modelStage, String modelOption) throws Exception {

		String modelPath = getModelPath(context, algoName, modelName, modelStage, modelOption);
		if (modelPath == null) return null;
		/*
		 * Leverage Apache Spark mechanism to read the GaussianMixture clustering model
		 * from a model specific file set
		 */
		return GaussianMixtureModel.load(modelPath);

	}

	public void track(SparkExecutionPluginContext context, String modelName, String modelStage, String modelParams, String modelMetrics,
			GaussianMixtureModel model) throws Exception {

		/* ARTIFACTS */

		long ts = new Date().getTime();
		String fsPath = algoName + "/" + ts + "/" + modelName;

		String modelPath = buildModelPath(context, fsPath);
		model.save(modelPath);

		/* METADATA */

		String modelPack = "WorksML";
		setMetadata(context, ts, modelName, modelPack, modelStage, modelParams, modelMetrics, fsPath);

	}

}