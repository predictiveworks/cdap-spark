package de.kp.works.ml.regression;

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

import org.apache.spark.ml.regression.AFTSurvivalRegressionModel;

import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.ml.AbstractRegressionManager;
import de.kp.works.core.ml.SparkMLManager;

public class SurvivalRegressorManager extends AbstractRegressionManager {

	private String ALGORITHM_NAME = "AFTSurvivalRegressor";

	public AFTSurvivalRegressionModel read(SparkExecutionPluginContext context, String modelName) throws Exception {

		FileSet fs = SparkMLManager.getRegressionFS(context);
		Table table = SparkMLManager.getRegressionMeta(context);
		
		return read(fs, table, modelName);
		
	}
	
	private AFTSurvivalRegressionModel read(FileSet fs, Table table, String modelName) throws IOException {
		
		String fsPath = getModelFsPath(table, ALGORITHM_NAME, modelName);
		if (fsPath == null) return null;
		/*
		 * Leverage Apache Spark mechanism to read the AFTSurvivalRegression model
		 * from a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		return AFTSurvivalRegressionModel.load(modelPath);
		
	}

	public void save(FileSet fs, Table table, String modelName, String modelParams, String modelMetrics,
			AFTSurvivalRegressionModel model) throws IOException {

		/***** MODEL COMPONENTS *****/

		/*
		 * Define the path of this model on CDAP's internal regression fileset
		 */
		Long ts = new Date().getTime();
		String fsPath = ALGORITHM_NAME + "/" + ts.toString() + "/" + modelName;
		/*
		 * Leverage Apache Spark mechanism to write the AFTSurvivalRegression model
		 * to a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		model.save(modelPath);

		/***** MODEL METADATA *****/

		setMetadata(ts, table, ALGORITHM_NAME, modelName, modelParams, modelMetrics, fsPath);

	}

}
