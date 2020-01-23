package de.kp.works.ml.classification;

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

import org.apache.spark.ml.classification.RandomForestClassificationModel;

import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.table.Table;
import de.kp.works.core.ml.AbstractClassificationManager;

public class RFClassifierManager extends AbstractClassificationManager {

	private String ALGORITHM_NAME = "RandomForestClassifier";

	public RandomForestClassificationModel read(FileSet fs, Table table, String modelName) throws IOException {

		String fsPath = getModelFsPath(table, ALGORITHM_NAME, modelName);
		if (fsPath == null)
			return null;
		/*
		 * Leverage Apache Spark mechanism to read the RandomForest model from a model
		 * specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		return RandomForestClassificationModel.load(modelPath);

	}

	public void save(FileSet fs, Table table, String modelName, String modelParams, String modelMetrics,
			RandomForestClassificationModel model) throws IOException {

		/*
		 * Define the path of this model on CDAP's internal classification fileset
		 */
		Long ts = new Date().getTime();
		String fsPath = ALGORITHM_NAME + "/" + ts.toString() + "/" + modelName;
		/*
		 * Leverage Apache Spark mechanism to write the RandomForest model to a model
		 * specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		model.save(modelPath);

		/***** MODEL METADATA *****/

		setMetadata(ts, table, ALGORITHM_NAME, modelName, modelParams, modelMetrics, fsPath);

	}

}
