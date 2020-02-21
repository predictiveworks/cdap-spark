package de.kp.works.ml.clustering;
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

import org.apache.spark.ml.clustering.GaussianMixtureModel;

import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.Algorithms;
import de.kp.works.core.ml.ClusterRecorder;
import de.kp.works.core.ml.SparkMLManager;

public class GaussianMixtureRecorder extends ClusterRecorder {

	public GaussianMixtureModel read(SparkExecutionPluginContext context, String modelName) throws Exception {

		FileSet fs = SparkMLManager.getClusteringFS(context);
		Table table = SparkMLManager.getClusteringTable(context);

		String algorithmName = Algorithms.GAUSSIAN_MIXTURE;

		String fsPath = getModelFsPath(table, algorithmName, modelName);
		if (fsPath == null)
			return null;
		/*
		 * Leverage Apache Spark mechanism to read the GaussianMixture clustering model
		 * from a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		return GaussianMixtureModel.load(modelPath);

	}

	public void track(SparkExecutionPluginContext context, String modelName, String modelStage, String modelParams, String modelMetrics,
			GaussianMixtureModel model) throws Exception {

		String algorithmName = Algorithms.GAUSSIAN_MIXTURE;

		/***** ARTIFACTS *****/

		Long ts = new Date().getTime();
		String fsPath = algorithmName + "/" + ts.toString() + "/" + modelName;
		/*
		 * Leverage Apache Spark mechanism to write the LogisticRegression model to a
		 * model specific file set
		 */
		FileSet fs = SparkMLManager.getClusteringFS(context);
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();

		model.save(modelPath);

		/***** METADATA *****/

		String modelPack = "WorksML";
		Table table = SparkMLManager.getClusteringTable(context);

		setMetadata(ts, table, algorithmName, modelName, modelPack, modelStage, modelParams, modelMetrics, fsPath);

	}

}