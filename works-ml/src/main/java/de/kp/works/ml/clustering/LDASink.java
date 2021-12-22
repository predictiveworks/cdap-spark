package de.kp.works.ml.clustering;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.spark.ml.clustering.LDAModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.gson.Gson;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkSink;

import de.kp.works.core.cluster.ClusterConfig;
import de.kp.works.core.cluster.ClusterSink;
import de.kp.works.core.recording.clustering.LDARecorder;

@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name("LDASink")
@Description("A building stage for an Apache Spark ML Latent Dirichlet Allocation (LDA) clustering model. "
		+ "This stage expects a dataset with at least one 'features' field as an array of numeric values to "
		+ "train the model.")
public class LDASink extends ClusterSink {

	private static final long serialVersionUID = 7607102103139502481L;

	private final LDAConfig config;

	public LDASink(LDAConfig config) {
		this.config = config;
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
		super.configurePipeline(pipelineConfigurer);

		/* Validate configuration */
		config.validate();

		/*
		 * Validate whether the input schema exists, contains the specified field for
		 * the feature vector and defines the feature vector as an ARRAY[DOUBLE]
		 */
		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
		inputSchema = stageConfigurer.getInputSchema();
		if (inputSchema != null)
			validateSchema(inputSchema);

	}

	@Override
	public void compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

		/*
		 * STEP #1: Extract parameters and train LDA model
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
		 * as Array[Numeric]
		 */
		LDATrainer trainer = new LDATrainer();
		Dataset<Row> vectorset = trainer.vectorize(source, featuresCol, vectorCol);
		/*
		 * Split the vectorset into a train & test dataset for later clustering
		 * evaluation
		 */
		Dataset<Row>[] splitted = vectorset.randomSplit(config.getSplits());

		Dataset<Row> trainset = splitted[0];
		Dataset<Row> testset = splitted[1];

		LDAModel model = trainer.train(trainset, vectorCol, params);
		/*
		 * The evaluation of the LDA model is performed leveraging the 
		 * perplexity measure; this is a measure to determine how well
		 * the features (vectors) of the testset are represented by the
		 * word distribution of the topics.
		 */
		Double perplexity = model.logPerplexity(testset);
		Double likelihood = model.logLikelihood(testset);
		/*
		 * The perplexity & likelihood coefficient is specified as intrinsic
		 * JSON metrics for this LDA model and stored by the LDAManager
		 */
		Map<String, Object> metrics = new HashMap<>();

		metrics.put("perplexity", perplexity);
		metrics.put("likelihood", likelihood);

		/* Add unused metrics to be schema compliant */
		metrics.put("silhouette_euclidean", 0.0);
		metrics.put("silhouette_cosine", 0.0);
		
		/*
		 * STEP #3: Store trained LDA model including its associated
		 * parameters and metrics
		 */
		String modelParams = config.getParamsAsJSON();
		String modelMetrics = new Gson().toJson(metrics);

		String modelName = config.modelName;
		String modelStage = config.modelStage;
		
		String modelPack = "WorksML";
		new LDARecorder(configReader)
				.track(context, modelName, modelPack, modelStage, modelParams, modelMetrics, model);

	}

	@Override
	public void validateSchema(Schema inputSchema) {
		config.validateSchema(inputSchema);
	}
	
	public static class LDAConfig extends ClusterConfig {
		  
		private static final long serialVersionUID = 7925435496096417998L;

		@Description("The split of the dataset into train & test data, e.g. 80:20. Default is 90:10.")
		@Macro
		public String dataSplit;
		
	    @Description("The number of topics that have to be created. Default is 10.")
	    @Macro
	    public Integer k;
		
	    @Description("The (maximum) number of iterations the algorithm has to execute. Default value is 20.")
	    @Macro
	    public Integer maxIter;
	    
	    LDAConfig() {

	    		referenceName = "LDASink";
	    		
	    		dataSplit = "90:10";		    	
    			modelStage = "experiment";
	    		
	    		k = 10;
	    		maxIter = 20;
	    		
	    }

		public Map<String, Object> getParamsAsMap() {

			Map<String, Object> params = new HashMap<>();

			params.put("k", k);
			params.put("maxIter", maxIter);

			return params;

		}
		
		public double[] getSplits() {
			
			String[] tokens = dataSplit.split(":");
			
			Double x = Double.parseDouble(tokens[0]) / 100D;
			Double y = Double.parseDouble(tokens[1]) / 100D;
			
			List<Double> splits = new ArrayList<>();
			splits.add(x);
			splits.add(y);

			Double[] array = splits.toArray(new Double[0]);
			return Stream.of(array).mapToDouble(Double::doubleValue).toArray();

		}
	    
		public void validate() {
			super.validate();

			if (k <= 1) {
				throw new IllegalArgumentException(String.format("[%s] The number of topics must be greater than 1.",
						this.getClass().getName()));
			}
			if (maxIter <= 0) {
				throw new IllegalArgumentException(String
						.format("[%s] The number of iterations must be greater than 0.", this.getClass().getName()));
			}
			
		}
		
	}
}
