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

import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.common.base.Strings;
import com.google.gson.Gson;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.BaseClassifierConfig;
import de.kp.works.core.BaseClassifierSink;

@Plugin(type = "sparksink")
@Name("MLPClassifer")
@Description("A building stage for an Apache Spark based Multilayer Perceptron classifier model.")
public class MLPClassifier extends BaseClassifierSink {

	private static final long serialVersionUID = -7445401286046769822L;
	
	private MLPClassifierConfig config;
	
	public MLPClassifier(MLPClassifierConfig config) {
		this.config = config;
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
		super.configurePipeline(pipelineConfigurer);

		/* Validate configuration */
		config.validate();
		
		/* Validate schema */
		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
		Schema inputSchema = stageConfigurer.getInputSchema();

		validateSchema(inputSchema, config, MLPClassifier.class.getName());

	}
	
	@Override
	public void compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {
		/*
		 * STEP #1: Extract parameters and train classifier model
		 */
		String featuresCol = config.featuresCol;
		String labelCol = config.labelCol;

		Map<String, Object> params = config.getParamsAsMap();
		/*
		 * The vectorCol specifies the internal column that has
		 * to be built from the featuresCol and that is used for
		 * training purposes
		 */
		String vectorCol = "_vector";
		/*
		 * Prepare provided dataset by vectorizing the feature
		 * column which is specified as Array[Double]
		 */
		MLPTrainer trainer = new MLPTrainer();
		Dataset<Row> vectorset = trainer.vectorize(source, featuresCol, vectorCol);
		/*
		 * Split the vectorset into a train & test dataset for
		 * later classification evaluation
		 */
	    Dataset<Row>[] splitted = vectorset.randomSplit(config.getSplits());
		
	    Dataset<Row> trainset = splitted[0];
	    Dataset<Row> testset = splitted[1];

	    MultilayerPerceptronClassificationModel model = trainer.train(trainset, vectorCol, labelCol, params);
		/*
		 * STEP #2: Compute accuracy of the trained classification
		 * model
		 */
	    String predictionCol = "_prediction";
	    model.setPredictionCol(predictionCol);

	    Dataset<Row> predictions = model.transform(testset);
	    /*
	     * This Multilayer perceptron plugin leverages the multiclass evaluator
	     * independent of whether the algorithm is used for binary classification
	     * or not.
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
		 * by the MLPClassifierManager
		 */
		Map<String,Object> metrics = new HashMap<>();
		
		metrics.put("name", metricName);
		metrics.put("coefficient", accuracy);
		/*
		 * STEP #3: Store trained classification model 
		 * including its associated parameters and metrics
		 */
		String paramsJson = config.getParamsAsJSON();
		String metricsJson = new Gson().toJson(metrics);
		
		String modelName = config.modelName;
		new MLPClassifierManager().save(modelMeta, modelFs, modelName, paramsJson, metricsJson, model);

	}

	public static class MLPClassifierConfig extends BaseClassifierConfig {

		private static final long serialVersionUID = 7824534239299625292L;

		public MLPClassifierConfig() {
			/*
			 * The default split of the dataset into train & test data
			 * is set to 70:30
			 */
			dataSplit = "70:30";			
		}
	    
		@Override
		public Map<String, Object> getParamsAsMap() {
			
			Map<String, Object> params = new HashMap<>();

			return params;
		
		}

		public void validate() {

			/** MODEL & COLUMNS **/
			if (!Strings.isNullOrEmpty(modelName)) {
				throw new IllegalArgumentException("[MLPClassifierConfig] The model name must not be empty.");
			}
			if (!Strings.isNullOrEmpty(featuresCol)) {
				throw new IllegalArgumentException("[MLPClassifierConfig] The name of the field that contains the feature vector must not be empty.");
			}
			if (!Strings.isNullOrEmpty(labelCol)) {
				throw new IllegalArgumentException("[MLPClassifierConfig] The name of the field that contains the label value must not be empty.");
			}
			
			/** PARAMETERS **/
						
		}
		
	}
}
