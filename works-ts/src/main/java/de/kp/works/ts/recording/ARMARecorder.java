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
import de.kp.works.core.recording.TimeRecorder;
import de.kp.works.ts.model.ARMAModel;
import de.kp.works.ts.model.AutoARMAModel;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;

import java.util.Date;

public class ARMARecorder extends TimeRecorder {

	public ARMARecorder() {
		super();
	}

	/** READ **/
	
	public ARMAModel readARMA(SparkExecutionPluginContext context, String modelName, String modelStage, String modelOption) throws Exception {

		algoName = Algorithms.ARMA;

		String modelPath = getModelPath(context, algoName, modelName, modelStage, modelOption);
		if (modelPath == null) return null;
		/*
		 * Leverage Apache Spark mechanism to read the ARMA model
		 * from a model specific file set
		 */
		return ARMAModel.load(modelPath);
		
	}
	
	public AutoARMAModel readAutoARMA(SparkExecutionPluginContext context, String modelName, String modelStage, String modelOption) throws Exception {

		algoName = Algorithms.AUTO_ARMA;

		String modelPath = getModelPath(context, algoName, modelName, modelStage, modelOption);
		if (modelPath == null) return null;
		/*
		 * Leverage Apache Spark mechanism to read the AutoARMA model
		 * from a model specific file set
		 */
		return AutoARMAModel.load(modelPath);
		
	}

	/** WRITE **/
	
	public void trackARMA(SparkExecutionPluginContext context, String modelName, String modelStage, String modelParams, String modelMetrics,
			ARMAModel model) throws Exception {

		algoName = Algorithms.ARMA;

		/* ARTIFACTS */

		long ts = new Date().getTime();
		String fsPath = algoName + "/" + ts + "/" + modelName;

		String modelPath = buildModelPath(context, fsPath);
		model.save(modelPath);

		/* METADATA */

		String modelPack = "WorksTS";
		setMetadata(context, ts, modelName, modelPack, modelStage, modelParams, modelMetrics, fsPath);
		
	}
	
	public void trackAutoARMA(SparkExecutionPluginContext context, String modelName, String modelStage, String modelParams, String modelMetrics,
			AutoARMAModel model) throws Exception {

		algoName = Algorithms.AUTO_ARMA;

		/* ARTIFACTS */

		long ts = new Date().getTime();
		String fsPath = algoName + "/" + ts + "/" + modelName;

		String modelPath = buildModelPath(context, fsPath);
		model.save(modelPath);

		/* METADATA */

		String modelPack = "WorksTS";
		setMetadata(context, ts, modelName, modelPack, modelStage, modelParams, modelMetrics, fsPath);
		
	}

}
