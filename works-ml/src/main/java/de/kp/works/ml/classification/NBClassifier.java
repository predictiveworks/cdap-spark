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

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
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
import de.kp.works.core.BaseClassifierConfig;
import de.kp.works.core.BaseClassifierSink;

@Plugin(type = "sparksink")
@Name("NBClassifer")
@Description("A building stage for an Apache Spark based Naive Bayes classifier model.")
public class NBClassifier extends BaseClassifierSink {

	private static final long serialVersionUID = -3067097831994994477L;
	
	public NBClassifier(NBClassifierConfig config) {
		this.config = config;
		this.className = NBClassifier.class.getName();
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
		super.configurePipeline(pipelineConfigurer);

		/* Validate configuration */
		((NBClassifierConfig)config).validate();
		
		/* Validate schema */
		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
		inputSchema = stageConfigurer.getInputSchema();
		if (inputSchema != null)
			validateSchema(inputSchema, config);

	}
	
	@Override
	public void compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {
		
		NBClassifierConfig classifierConfig = (NBClassifierConfig)config;
		/*
		 * STEP #1: Extract parameters and train classifier model
		 */
		String featuresCol = classifierConfig.featuresCol;
		String labelCol = classifierConfig.labelCol;

		Map<String, Object> params = classifierConfig.getParamsAsMap();
		/*
		 * The vectorCol specifies the internal column that has
		 * to be built from the featuresCol and that is used for
		 * training purposes
		 */
		String vectorCol = "_vector";
		/*
		 * Prepare provided dataset by vectorizing the feature
		 * column which is specified as Array[Numeric]
		 */
		NBTrainer trainer = new NBTrainer();
		Dataset<Row> vectorset = trainer.vectorize(source, featuresCol, vectorCol);
		/*
		 * Split the vectorset into a train & test dataset for
		 * later classification evaluation
		 */
	    Dataset<Row>[] splitted = vectorset.randomSplit(classifierConfig.getSplits());
		
	    Dataset<Row> trainset = splitted[0];
	    Dataset<Row> testset = splitted[1];

	    NaiveBayesModel model = trainer.train(trainset, vectorCol, labelCol, params);
		/*
		 * STEP #2: Compute accuracy of the trained classification
		 * model
		 */
	    String predictionCol = "_prediction";
	    model.setPredictionCol(predictionCol);

	    Dataset<Row> predictions = model.transform(testset);
	    /*
	     * This NaiveBayes plugin leverages the multiclass evaluator
	     * independent of whether the algorithm is used for binary 
	     * classification or not.
	     */
	    MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator();
	    evaluator.setLabelCol(labelCol);
	    evaluator.setPredictionCol(predictionCol);
	    
	    String metricName = "accuracy";
	    evaluator.setMetricName(metricName);
	    
	    double accuracy = evaluator.evaluate(predictions);
		/*
		 * The accuracy coefficent is specified as JSON
		 * metrics for this classification model and stored
		 * by the NBClassifierManager
		 */
		Map<String,Object> metrics = new HashMap<>();
		
		metrics.put("name", metricName);
		metrics.put("coefficient", accuracy);
		/*
		 * STEP #3: Store trained classification model 
		 * including its associated parameters and metrics
		 */
		String paramsJson = classifierConfig.getParamsAsJSON();
		String metricsJson = new Gson().toJson(metrics);
		
		String modelName = classifierConfig.modelName;
		new NBClassifierManager().save(modelFs, modelMeta, modelName, paramsJson, metricsJson, model);

	}
	
	public static class NBClassifierConfig extends BaseClassifierConfig {

		private static final long serialVersionUID = 7463362537971476965L;

		@Description("The model type of the Naive Bayes classifier. Supported values are 'bernoulli' and 'multinomial'. " 
				+ "Choosing the Bernoulli version of Naive Bayes requires the feature values to be binary (0 or 1). Default is 'multinomial'.")
		@Macro
		public String modelType;

		@Description("The smoothing parameter of the Naive Bayes classifier. Default is 1.0.")
		@Macro
		public Double smoothing;
		
		public NBClassifierConfig() {

			dataSplit = "70:30";
			
			modelType = "multinomial";
			smoothing = 1.0;
			
		}

		@Override
		public Map<String, Object> getParamsAsMap() {
			
			Map<String, Object> params = new HashMap<>();

			params.put("modelType", modelType);
			params.put("smoothing", smoothing);

			return params;
		
		}
		
		public void validate() {
			super.validate();
			
			/** PARAMETERS **/
			if (smoothing < 0D)
				throw new IllegalArgumentException(
						String.format("[%s] The smoothing must be nonnegative.", this.getClass().getName()));
						
		}
		
	}
}
