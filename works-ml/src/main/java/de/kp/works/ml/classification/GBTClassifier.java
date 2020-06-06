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

import java.util.Map;

import org.apache.spark.ml.classification.GBTClassificationModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkSink;

import de.kp.works.core.classifier.ClassifierSink;
import de.kp.works.ml.config.GBTConfig;

@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name("GBTClassifer")
@Description("A building stage for an Apache Spark ML Gradient-Boosted Tree classifier model. This stage expects " 
				+ "a dataset with at least two fields to train the model: One as an array of numeric values, and, "  
				+ "another that describes the class or label value as numeric value.")
public class GBTClassifier extends ClassifierSink {

	private static final long serialVersionUID = 2970318408059323693L;

	private GBTConfig config;
	public GBTClassifier(GBTConfig config) {
		this.config = config;
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
		super.configurePipeline(pipelineConfigurer);

		/* Validate configuration */
		config.validate();

		/* Validate schema */
		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
		inputSchema = stageConfigurer.getInputSchema();
		if (inputSchema != null)
			validateSchema(inputSchema);

	}

	@Override
	public void compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {
		
		/*
		 * STEP #1: Extract parameters and train classifier model
		 */
		String featuresCol = config.featuresCol;
		String labelCol = config.labelCol;

		Map<String, Object> params = config.getParamsAsMap();
		String modelParams = config.getParamsAsJSON();
		/*
		 * The vectorCol specifies the internal column that has to be built from the
		 * featuresCol and that is used for training purposes
		 */
		String vectorCol = "_vector";
		/*
		 * Prepare provided dataset by vectorizing the feature column which is specified
		 * as Array[Numeric]
		 */
		GBTTrainer trainer = new GBTTrainer();
		Dataset<Row> vectorset = trainer.vectorize(source, featuresCol, vectorCol);
		/*
		 * Split the vectorset into a train & test dataset for later classification
		 * evaluation
		 */
		Dataset<Row>[] splitted = vectorset.randomSplit(config.getSplits());

		Dataset<Row> trainset = splitted[0];
		Dataset<Row> testset = splitted[1];

		GBTClassificationModel model = trainer.train(trainset, vectorCol, labelCol, params);
		/*
		 * STEP #2: Evaluate classification model and compute
		 * approved list of metrics
		 */
		String predictionCol = "_prediction";
		model.setPredictionCol(predictionCol);

		Dataset<Row> predictions = model.transform(testset);
	    String modelMetrics = Evaluator.evaluate(predictions, labelCol, predictionCol);
		/*
		 * STEP #3: Store trained classification model including
		 * its associated parameters and metrics
		 */		
		String modelName = config.modelName;
		String modelStage = config.modelStage;
		
		new GBCRecorder().track(context, modelName, modelStage, modelParams, modelMetrics, model);

	}

	@Override
	public void validateSchema(Schema inputSchema) {
		config.validateSchema(inputSchema);
	}

}
