package de.kp.works.text.sentiment;
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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.common.base.Strings;
import com.johnsnowlabs.nlp.annotators.sda.vivekn.ViveknSentimentModel;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.TextSink;

@Plugin(type = "sparksink")
@Name("SentimentSink")
@Description("A building stage for a Spark-NLP based Sentiment Analysis model.")
public class SentimentSink extends TextSink {
	
	private static final long serialVersionUID = -242069506700606299L;

	private SentimentSinkConfig config;
	
	public SentimentSink(SentimentSinkConfig config) {
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
		 * Split the source into a train & test dataset for later model
		 * evaluation; A sentiment analysis model is evaluated with a
		 * regression evaluator
		 */
		Dataset<Row>[] splitted = source.randomSplit(config.getSplits());

		Dataset<Row> trainset = splitted[0];
		Dataset<Row> testset = splitted[1];
		
		SATrainer trainer = new SATrainer();
		ViveknSentimentModel model = trainer.train(trainset, config.textCol, config.sentimentCol);

	    SAPredictor predictor = new SAPredictor(model);
	    Dataset<Row> predictions = predictor.predict(testset, config.textCol, "predicted");
	    
	    String metricsJson = SAEvaluator.evaluate(predictions, config.sentimentCol, "predicted");    
		String paramsJson = config.getParamsAsJSON();

		String modelName = config.modelName;
		new SentimentManager().save(modelFs, modelMeta, modelName, paramsJson, metricsJson, model);
	    
	}
	
	@Override
	public void validateSchema(Schema inputSchema) {

		/** TEXT COLUMN **/

		Schema.Field textCol = inputSchema.getField(config.textCol);
		if (textCol == null) {
			throw new IllegalArgumentException(String.format(
					"[%s] The input schema must contain the field that defines the text document.", this.getClass().getName()));
		}
		
		isString(config.textCol);

		/** SENTIMENT COLUMN **/

		Schema.Field sentimentCol = inputSchema.getField(config.sentimentCol);
		if (sentimentCol == null) {
			throw new IllegalArgumentException(String.format(
					"[%s] The input schema must contain the field that defines the sentiment.", className));
		}
		
		isString(config.sentimentCol);

	}

	public static class SentimentSinkConfig extends BaseSentimentConfig {

		private static final long serialVersionUID = 7018920867242125687L;

		@Description("The name of the field in the input schema that contains the sentiment value.")
		@Macro
		public String sentimentCol;

		@Description("The split of the dataset into train & test data, e.g. 80:20. Default is 70:30")
		@Macro
		public String dataSplit;

		public SentimentSinkConfig() {
			dataSplit = "70:30";
		}

		@Override
		public Map<String, Object> getParamsAsMap() {

			Map<String, Object> params = new HashMap<>();

			params.put("dataSplit", dataSplit);
			return params;

		}
		
		public void validate() {
			super.validate();
			
			if (Strings.isNullOrEmpty(sentimentCol)) {
				throw new IllegalArgumentException(String.format(
						"[%s] The name of the field that contains the sentiment value must not be empty.",
						this.getClass().getName()));
			}
			if (Strings.isNullOrEmpty(dataSplit)) {
				throw new IllegalArgumentException(
						String.format("[%s] The data split must not be empty.",
								this.getClass().getName()));
			}
			
		}
		
		public double[] getSplits() {
			return getDataSplits(dataSplit);
		}		
		
	}
}
