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
import de.kp.works.core.recording.TimeRecorder;
import de.kp.works.ts.model.ARYuleWalkerModel;
import de.kp.works.ts.model.AutoARModel;
import de.kp.works.ts.model.AutoRegressionModel;
import de.kp.works.ts.model.DiffAutoRegressionModel;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;

import java.util.Date;

public class ARRecorder extends TimeRecorder {

	public ARRecorder(ConfigReader configReader) {
		super(configReader);
	}

	/** READ **/
	
	public AutoRegressionModel readAR(SparkExecutionPluginContext context, String modelName, String modelStage, String modelOption) throws Exception {

		algoName = Algorithms.AR;

		String modelPath = getModelPath(context, algoName, modelName, modelStage, modelOption);
		if (modelPath == null) return null;
		/*
		 * Leverage Apache Spark mechanism to read the AutoRegression model
		 * from a model specific file set
		 */
		return AutoRegressionModel.load(modelPath);
		
	}
	
	public AutoARModel readAutoAR(SparkExecutionPluginContext context, String modelName, String modelStage, String modelOption) throws Exception {

		algoName = Algorithms.AUTO_AR;

		String modelPath = getModelPath(context, algoName, modelName, modelStage, modelOption);
		if (modelPath == null) return null;
		/*
		 * Leverage Apache Spark mechanism to read the AutoAR model
		 * from a model specific file set
		 */
		return AutoARModel.load(modelPath);
		
	}
	
	public DiffAutoRegressionModel readDiffAR(SparkExecutionPluginContext context, String modelName, String modelStage, String modelOption) throws Exception {

		algoName = Algorithms.DIFF_AR;

		String modelPath = getModelPath(context, algoName, modelName, modelStage, modelOption);
		if (modelPath == null) return null;
		/*
		 * Leverage Apache Spark mechanism to read the DiffAutoRegression model
		 * from a model specific file set
		 */
		return DiffAutoRegressionModel.load(modelPath);
		
	}
	
	public ARYuleWalkerModel readYuleWalker(SparkExecutionPluginContext context, String modelName, String modelStage, String modelOption) throws Exception {

		algoName = Algorithms.YULE_WALKER;

		String modelPath = getModelPath(context, algoName, modelName, modelStage, modelOption);
		if (modelPath == null) return null;
		/*
		 * Leverage Apache Spark mechanism to read the ARYuleWalker model
		 * from a model specific file set
		 */
		return ARYuleWalkerModel.load(modelPath);
		
	}

	/** WRITE **/
	
	public void trackAR(SparkExecutionPluginContext context, String modelName, String modelStage, String modelParams, String modelMetrics,
			AutoRegressionModel model) throws Exception {

		algoName = Algorithms.AR;

		/* ARTIFACTS */

		long ts = new Date().getTime();
		String fsPath = algoName + "/" + ts + "/" + modelName;

		String modelPath = buildModelPath(context, fsPath);
		model.save(modelPath);

		/* METADATA */

		String modelPack = "WorksTS";
		setMetadata(context, ts, modelName, modelPack, modelStage, modelParams, modelMetrics, fsPath);
		
	}
	
	public void trackAutoAR(SparkExecutionPluginContext context, String modelName, String modelStage, String modelParams, String modelMetrics,
			AutoARModel model) throws Exception {

		algoName = Algorithms.AUTO_AR;

		/* ARTIFACTS */

		long ts = new Date().getTime();
		String fsPath = algoName + "/" + ts + "/" + modelName;

		String modelPath = buildModelPath(context, fsPath);
		model.save(modelPath);

		/* METADATA */

		String modelPack = "WorksTS";
		setMetadata(context, ts, modelName, modelPack, modelStage, modelParams, modelMetrics, fsPath);
		
	}
	
	public void trackDiffAR(SparkExecutionPluginContext context, String modelName, String modelStage, String modelParams, String modelMetrics,
			DiffAutoRegressionModel model) throws Exception {

		algoName = Algorithms.DIFF_AR;

		/* ARTIFACTS */

		long ts = new Date().getTime();
		String fsPath = algoName + "/" + ts + "/" + modelName;

		String modelPath = buildModelPath(context, fsPath);
		model.save(modelPath);

		/* METADATA */

		String modelPack = "WorksTS";
		setMetadata(context, ts, modelName, modelPack, modelStage, modelParams, modelMetrics, fsPath);
		
	}
	
	public void trackYuleWalker(SparkExecutionPluginContext context, String modelName, String modelStage, String modelParams, String modelMetrics,
			ARYuleWalkerModel model) throws Exception {

		algoName = Algorithms.YULE_WALKER;

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
