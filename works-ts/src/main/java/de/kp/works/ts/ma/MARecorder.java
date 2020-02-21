package de.kp.works.ts.ma;
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

import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.ml.TimeRecorder;
import de.kp.works.core.Algorithms;
import de.kp.works.core.ml.SparkMLManager;
import de.kp.works.ts.model.AutoMAModel;
import de.kp.works.ts.model.MovingAverageModel;

public class MARecorder extends TimeRecorder {

	/** READ **/
	
	public MovingAverageModel readMA(SparkExecutionPluginContext context, String modelName, String modelStage) throws Exception {

		FileSet fs = SparkMLManager.getTimeFS(context);
		Table table = SparkMLManager.getTimesTable(context);
		
		String algorithmName = Algorithms.MA;
		
		String fsPath = getModelFsPath(table, algorithmName, modelName, modelStage);
		if (fsPath == null) return null;
		/*
		 * Leverage Apache Spark mechanism to read the MovingAverage model
		 * from a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		return MovingAverageModel.load(modelPath);
		
	}
	
	public AutoMAModel readAutoMA(SparkExecutionPluginContext context, String modelName, String modelStage) throws Exception {

		FileSet fs = SparkMLManager.getTimeFS(context);
		Table table = SparkMLManager.getTimesTable(context);
		
		String algorithmName = Algorithms.AUTO_MA;
		
		String fsPath = getModelFsPath(table, algorithmName, modelName, modelStage);
		if (fsPath == null) return null;
		/*
		 * Leverage Apache Spark mechanism to read the AutoMA model
		 * from a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		return AutoMAModel.load(modelPath);
		
	}

	/** WRITE **/
	
	public void trackMA(SparkExecutionPluginContext context, String modelName, String modelStage, String modelParams, String modelMetrics,
			MovingAverageModel model) throws Exception {
		
		String algorithmName = Algorithms.MA;

		/***** ARTIFACTS *****/

		Long ts = new Date().getTime();
		String fsPath = algorithmName + "/" + ts.toString() + "/" + modelName;

		FileSet fs = SparkMLManager.getTimeFS(context);

		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		model.save(modelPath);

		/***** METADATA *****/

		String modelPack = "WorksTS";
		Table table = SparkMLManager.getTimesTable(context);

		setMetadata(ts, table, algorithmName, modelName, modelPack, modelStage, modelParams, modelMetrics, fsPath);
		
	}
	
	public void trackAutoMA(SparkExecutionPluginContext context, String modelName, String modelStage, String modelParams, String modelMetrics,
			AutoMAModel model) throws Exception {
		
		String algorithmName = Algorithms.AUTO_MA;

		/***** ARTIFACTS *****/

		Long ts = new Date().getTime();
		String fsPath = algorithmName + "/" + ts.toString() + "/" + modelName;

		FileSet fs = SparkMLManager.getTimeFS(context);

		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		model.save(modelPath);

		/***** METADATA *****/

		String modelPack = "WorksTS";
		Table table = SparkMLManager.getTimesTable(context);

		setMetadata(ts, table, algorithmName, modelName, modelPack, modelStage, modelParams, modelMetrics, fsPath);
		
	}

}
