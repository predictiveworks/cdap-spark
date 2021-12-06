package de.kp.works.core.cluster;

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
import org.apache.spark.ml.clustering.*;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;

import de.kp.works.core.Algorithms;
import de.kp.works.core.ml.clustering.ClusterRecorder;
import de.kp.works.core.ml.SparkMLManager;

/**
 * LDA based clustering is used in works-ml and also in works-text project
 */
public class LDARecorder extends ClusterRecorder {

	public LDAModel read(SparkExecutionPluginContext context, String modelName, String modelStage, String modelOption) throws Exception {

		String algorithmName = Algorithms.LATENT_DIRICHLET_ALLOCATION;

		String modelPath = getModelPath(context, algorithmName, modelName, modelStage, modelOption);
		if (modelPath == null) return null;
		/*
		 * Leverage Apache Spark mechanism to read the LDA clustering model from a model
		 * specific file set
		 */
		return DistributedLDAModel.load(modelPath);

	}

	public void track(SparkExecutionPluginContext context, String modelName, String modelPack, String modelStage,
			String modelParams, String modelMetrics, LDAModel model) throws Exception {

		String algorithmName = Algorithms.LATENT_DIRICHLET_ALLOCATION;

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

		Table table = SparkMLManager.getClusteringTable(context);
		String namespace = context.getNamespace();

		setMetadata(ts, table, namespace, algorithmName, modelName, modelPack, modelStage, modelParams, modelMetrics, fsPath);

	}

}
