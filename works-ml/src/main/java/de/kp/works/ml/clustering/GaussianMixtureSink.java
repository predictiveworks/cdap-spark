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

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.ml.clustering.GaussianMixtureModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.ClusterConfig;
import de.kp.works.ml.regression.Evaluator;
import de.kp.works.core.BaseClusterSink;

@Plugin(type = "sparksink")
@Name("GaussianMixtureSink")
@Description("A building stage for an Apache Spark based Gaussian Mixture clustering model.")
public class GaussianMixtureSink extends BaseClusterSink {

	private static final long serialVersionUID = -8171201794590284739L;

	public GaussianMixtureSink(GaussianMixtureConfig config) {
		this.config = config;
		this.className = BisectingKMeansSink.class.getName();
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
		super.configurePipeline(pipelineConfigurer);

		/* Validate configuration */
		((GaussianMixtureConfig)config).validate();

		/*
		 * Validate whether the input schema exists, contains the specified field for
		 * the feature vector and defines the feature vector as an ARRAY[DOUBLE]
		 */
		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
		inputSchema = stageConfigurer.getInputSchema();
		if (inputSchema != null)
			validateSchema(inputSchema, config);

	}

	@Override
	public void compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

		GaussianMixtureConfig clusterConfig = (GaussianMixtureConfig)config;
		/*
		 * STEP #1: Extract parameters and train Gaussian Mixture model
		 */
		String featuresCol = clusterConfig.featuresCol;

		Map<String, Object> params = clusterConfig.getParamsAsMap();
		String paramsJson = clusterConfig.getParamsAsJSON();
		/*
		 * The vectorCol specifies the internal column that has to be built from the
		 * featuresCol and that is used for training purposes
		 */
		String vectorCol = "_vector";
		/*
		 * Prepare provided dataset by vectorizing the feature column which is specified
		 * as Array[Numeric]
		 */
		GaussianMixtureTrainer trainer = new GaussianMixtureTrainer();
		Dataset<Row> vectorset = trainer.vectorize(source, featuresCol, vectorCol);

		GaussianMixtureModel model = trainer.train(vectorset, vectorCol, params);
		/*
		 * STEP #2: Compute silhouette coefficient as metric for this Gaussian Mixture
		 * parameter setting: to this end, the predictions are computed based on the 
		 * trained model and the vectorized data set
		 */
		String predictionCol = "_cluster";
		model.setPredictionCol(predictionCol);

		Dataset<Row> predictions = model.transform(vectorset);
		/*
		 * The Clustering evaluator computes the silhouette coefficent of the computed
		 * predictions as a means to evaluate the quality of the chosen parameters
		 */
	    String metricsJson = Evaluator.evaluate(predictions, vectorCol, predictionCol);
		/*
		 * STEP #3: Store trained Gaussian Mixture model including its associated 
		 * parameters and metrics
		 */
		String modelName = clusterConfig.modelName;
		new GaussianMixtureManager().save(modelFs, modelMeta, modelName, paramsJson, metricsJson, model);

	}

	public static class GaussianMixtureConfig extends ClusterConfig {

		private static final long serialVersionUID = 3332346148556050711L;
		
		@Description("The number of independent Gaussian distributions in the dataset. Must be > 1. Default is 2.")
		@Macro
		public Integer k;
		
	    @Description("The (maximum) number of iterations the algorithm has to execute. Default value: 100")
	    @Macro
	    private Integer maxIter;

		@Description("The positive convergence tolerance of iterations. Smaller values will lead to higher accuracy with the cost "
				+ "of more iterations. Default is 0.01.")
		@Macro
		public Double tol;		
		
		public GaussianMixtureConfig() {
			k = 2;
			maxIter = 100;
			tol = 0.01;
		}

		public Map<String, Object> getParamsAsMap() {

			Map<String, Object> params = new HashMap<String, Object>();
			params.put("k", k);
			params.put("maxIter", maxIter);
			params.put("tol", tol);

			return params;

		}

		public void validate() {
			super.validate();

			if (k <= 1) {
				throw new IllegalArgumentException(String.format("[%s] The number of leaf clusters must be greater than 1.",
						this.getClass().getName()));
			}
			if (maxIter <= 0) {
				throw new IllegalArgumentException(String
						.format("[%s] The number of iterations must be greater than 0.", this.getClass().getName()));
			}
		}
	}
}
