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

import java.io.IOException;
import java.util.Date;

import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.table.Table;
import de.kp.works.core.ml.AbstractTimeSeriesManager;
import de.kp.works.ts.model.AutoMAModel;
import de.kp.works.ts.model.MovingAverageModel;

public class MAManager extends AbstractTimeSeriesManager {

	/** READ **/
	
	public MovingAverageModel readMA(FileSet fs, Table table, String modelName) throws IOException {
		
		String algorithmName = "MA";
		
		String fsPath = getModelFsPath(table, algorithmName, modelName);
		if (fsPath == null) return null;
		/*
		 * Leverage Apache Spark mechanism to read the MovingAverage model
		 * from a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		return MovingAverageModel.load(modelPath);
		
	}

	public AutoMAModel readAutoMA(FileSet fs, Table table, String modelName) throws IOException {
		
		String algorithmName = "AutoMA";
		
		String fsPath = getModelFsPath(table, algorithmName, modelName);
		if (fsPath == null) return null;
		/*
		 * Leverage Apache Spark mechanism to read the AutoMA model
		 * from a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		return AutoMAModel.load(modelPath);
		
	}

	/** WRITE **/
	
	public void saveMA(FileSet fs, Table table, String modelName, String modelParams, String modelMetrics,
			MovingAverageModel model) throws IOException {
		
		String algorithmName = "MA";

		/***** MODEL COMPONENTS *****/

		/*
		 * Define the path of this model on CDAP's internal timeseries fileset;
		 * not, the timestamp within the path ensures that each model of the 
		 * same name but different version has its own path
		 */
		Long ts = new Date().getTime();
		String fsPath = algorithmName + "/" + ts.toString() + "/" + modelName;
		/*
		 * Leverage Apache Spark mechanism to write the MovingAverage model
		 * to a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		model.save(modelPath);

		/***** MODEL METADATA *****/

		setMetadata(ts, table, algorithmName, modelName, modelParams, modelMetrics, fsPath);

	}
	
	public void saveAutoMA(FileSet fs, Table table, String modelName, String modelParams, String modelMetrics,
			AutoMAModel model) throws IOException {
		
		String algorithmName = "AutoMA";

		/***** MODEL COMPONENTS *****/

		/*
		 * Define the path of this model on CDAP's internal timeseries fileset;
		 * not, the timestamp within the path ensures that each model of the 
		 * same name but different version has its own path
		 */
		Long ts = new Date().getTime();
		String fsPath = algorithmName + "/" + ts.toString() + "/" + modelName;
		/*
		 * Leverage Apache Spark mechanism to write the AutoMA model
		 * to a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		model.save(modelPath);

		/***** MODEL METADATA *****/

		setMetadata(ts, table, algorithmName, modelName, modelParams, modelMetrics, fsPath);

	}

}
