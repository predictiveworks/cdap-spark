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
import javax.annotation.Nullable;

import org.apache.spark.ml.clustering.KMeansModel;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.gson.Gson;
import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.BaseClusterConfig;
import de.kp.works.core.BaseClusterSink;

@Plugin(type = "sparksink")
@Name("KMeansSink")
@Description("A building stage for an Apache Spark based KMeans clustering model.")
public class KMeansSink extends BaseClusterSink {

	private static final long serialVersionUID = 8351695775316345380L;

	public KMeansSink(KMeansConfig config) {
		this.config = config;
		this.className = KMeansSink.class.getName();
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
		super.configurePipeline(pipelineConfigurer);

		/* Validate configuration */
		((KMeansConfig)config).validate();

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

		/*
		 * STEP #1: Extract parameters and train KMeans model
		 */
		String featuresCol = config.featuresCol;
		Map<String, Object> params = config.getParamsAsMap();
		/*
		 * The vectorCol specifies the internal column that has to be built from the
		 * featuresCol and that is used for training purposes
		 */
		String vectorCol = "_vector";
		/*
		 * Prepare provided dataset by vectorizing the feature column which is specified
		 * as Array[Double]
		 */
		KMeansTrainer trainer = new KMeansTrainer();
		Dataset<Row> vectorset = trainer.vectorize(source, featuresCol, vectorCol);

		KMeansModel model = trainer.train(vectorset, vectorCol, params);
		/*
		 * STEP #2: Compute silhouette coefficient as metric for this KMeans parameter
		 * setting: to this end, the predictions are computed based on the trained model
		 * and the vectorized data set
		 */
		String predictionCol = "_cluster";
		model.setPredictionCol(predictionCol);

		Dataset<Row> predictions = model.transform(vectorset);
		/*
		 * The KMeans evaluator computes the silhouette coefficent of the computed
		 * predictions as a means to evaluate the quality of the chosen parameters
		 */
		KMeansEvaluator evaluator = new KMeansEvaluator();

		evaluator.setPredictionCol(predictionCol);
		evaluator.setVectorCol(vectorCol);

		evaluator.setMetricName("silhouette");
		evaluator.setDistanceMeasure("squaredEuclidean");

		Double coefficent = evaluator.evaluate(predictions);
		/*
		 * The silhouette coefficent is specified as JSON metrics for this KMeans model
		 * and stored by the KMeans manager
		 */
		Map<String, Object> metrics = new HashMap<>();

		metrics.put("name", "silhouette");
		metrics.put("measure", "squaredEuclidean");
		metrics.put("coefficient", coefficent);
		/*
		 * STEP #3: Store trained KMeans model including its associated parameters and
		 * metrics
		 */
		String paramsJson = config.getParamsAsJSON();
		String metricsJson = new Gson().toJson(metrics);

		String modelName = config.modelName;
		new KMeansManager().save(modelFs, modelMeta, modelName, paramsJson, metricsJson, model);

	}

	public static class KMeansConfig extends BaseClusterConfig {

		private static final long serialVersionUID = -1071711175500255534L;

		@Description("The number of cluster that have to be created.")
		@Macro
		private Integer k;

		@Description("The (maximum) number of iterations the algorithm has to execute. Default value: 20")
		@Nullable
		@Macro
		private Integer maxIter;

		@Description("The convergence tolerance of the algorithm. Default value: 1e-4")
		@Nullable
		@Macro
		private Double tolerance;

		@Description("The number of steps for the initialization mode of the parallel KMeans algorithm. Default value: 2")
		@Nullable
		@Macro
		private Integer initSteps;

		@Description("The initialization mode of the algorithm. This can be either 'random' to choose random "
				+ "random points as initial cluster center, 'parallel' to use the parallel variant of KMeans++. Default value: 'parallel'")
		@Nullable
		@Macro
		private String initMode;

		public KMeansConfig() {

			referenceName = "KMeansSink";

			maxIter = 20;
			tolerance = 1e-4;

			initSteps = 2;
			initMode = "parallel";

		}

		public Map<String, Object> getParamsAsMap() {

			Map<String, Object> params = new HashMap<String, Object>();
			params.put("k", k);
			params.put("maxIter", maxIter);

			params.put("tolerance", tolerance);

			params.put("initSteps", initSteps);
			params.put("initMode", initMode);

			return params;

		}

		public void validate() {
			super.validate();

			if (k <= 1) {
				throw new IllegalArgumentException(String.format("[%s] The number of clusters must be greater than 1.",
						this.getClass().getName()));
			}
			if (maxIter <= 0) {
				throw new IllegalArgumentException(String
						.format("[%s] The number of iterations must be greater than 0.", this.getClass().getName()));
			}
			if (initSteps <= 0) {
				throw new IllegalArgumentException(String
						.format("[%s] The number of initial steps must be greater than 0.", this.getClass().getName()));
			}
			if (!(initMode.equals("random") || initMode.equals("parallel"))) {
				throw new IllegalArgumentException(
						String.format("[%s] The initialization mode must be either 'parallel' or 'random'.",
								this.getClass().getName()));
			}

		}
	}
}
