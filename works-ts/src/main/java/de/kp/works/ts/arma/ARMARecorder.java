package de.kp.works.ts.arma;
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
import de.kp.works.ts.model.ARMAModel;
import de.kp.works.ts.model.AutoARMAModel;

public class ARMARecorder extends TimeRecorder {

	/** READ **/
	
	public ARMAModel readARMA(SparkExecutionPluginContext context, String modelName, String modelStage) throws Exception {

		FileSet fs = SparkMLManager.getTimeFS(context);
		Table table = SparkMLManager.getTimesTable(context);
		
		String algorithmName = Algorithms.ARMA;
		
		String fsPath = getModelFsPath(table, algorithmName, modelName, modelStage);
		if (fsPath == null) return null;
		/*
		 * Leverage Apache Spark mechanism to read the ARMA model
		 * from a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		return ARMAModel.load(modelPath);
		
	}
	
	public AutoARMAModel readAutoARMA(SparkExecutionPluginContext context, String modelName, String modelStage) throws Exception {

		FileSet fs = SparkMLManager.getTimeFS(context);
		Table table = SparkMLManager.getTimesTable(context);
		
		String algorithmName = Algorithms.AUTO_ARMA;
		
		String fsPath = getModelFsPath(table, algorithmName, modelName, modelStage);
		if (fsPath == null) return null;
		/*
		 * Leverage Apache Spark mechanism to read the AutoARMA model
		 * from a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		return AutoARMAModel.load(modelPath);
		
	}

	/** WRITE **/
	
	public void trackARMA(SparkExecutionPluginContext context, String modelName, String modelStage, String modelParams, String modelMetrics,
			ARMAModel model) throws Exception {
		
		String algorithmName = Algorithms.ARMA;

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
	
	public void trackAutoARMA(SparkExecutionPluginContext context, String modelName, String modelStage, String modelParams, String modelMetrics,
			AutoARMAModel model) throws Exception {
		
		String algorithmName = Algorithms.AUTO_ARMA;

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
