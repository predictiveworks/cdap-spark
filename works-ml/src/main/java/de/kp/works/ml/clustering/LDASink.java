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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.spark.ml.clustering.LDAModel;
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
@Name("LDASink")
@Description("A building stage for an Apache Spark based Latent Dirichlet Allocation clustering model.")
public class LDASink extends BaseClusterSink {

	private static final long serialVersionUID = 7607102103139502481L;

	private LDAConfig config;

	public LDASink(LDAConfig config) {
		this.config = config;
		this.className = LDASink.class.getName();
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
		super.configurePipeline(pipelineConfigurer);

		/* Validate configuration */
		((LDAConfig)config).validate();

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
		 * The perplexity & likelihood coefficent is specified as intrinsic 
		 * JSON metrics for this LDA model and stored by the LDAManager
		 */
		Map<String, Object> metrics = new HashMap<>();

		metrics.put("perplexity", perplexity);
		metrics.put("likelihood", likelihood);
		/*
		 * STEP #3: Store trained LDA model including its associated
		 * parameters and metrics
		 */
		String paramsJson = config.getParamsAsJSON();
		String metricsJson = new Gson().toJson(metrics);

		String modelName = config.modelName;
		new LDAManager().save(modelFs, modelMeta, modelName, paramsJson, metricsJson, model);

	}
	
	public static class LDAConfig extends BaseClusterConfig {
		  
		private static final long serialVersionUID = 7925435496096417998L;

		@Description("The split of the dataset into train & test data, e.g. 80:20. Default is 90:10")
		@Macro
		public String dataSplit;
		
	    @Description("The number of topics that have to be created. Default is 10.")
	    @Macro
	    private Integer k;
		
	    @Description("The (maximum) number of iterations the algorithm has to execute. Default value: 20")
	    @Macro
	    private Integer maxIter;
	    
	    LDAConfig() {

	    		referenceName = "LDASink";
	    		dataSplit = "90:10";
	    		
	    		k = 10;
	    		maxIter = 20;
	    		
	    }

		public Map<String, Object> getParamsAsMap() {

			Map<String, Object> params = new HashMap<String, Object>();

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

			Double[] array = splits.toArray(new Double[splits.size()]);
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
