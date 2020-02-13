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
import co.cask.cdap.etl.api.batch.SparkSink;

import de.kp.works.core.SchemaUtil;
import de.kp.works.core.text.TextSink;

@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name("SentimentSink")
@Description("A building stage for a Sentiment Analysis model based on the sentiment algorithm "
		+ 	"introduced by Vivek Narayanan. The training corpus comprises a labeled set of sentiment "
		+ 	"tokens.")
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
		 * The corpus defines, line by line, a set of sentiment tokens and
		 * assigned sentiment label; first step is to split tokens and label.
		 * 
		 * The result is dataset with two additional columns _tokens and _label
		 */
		SATrainer trainer = new SATrainer();
		Dataset<Row> document = trainer.prepare(source, config.lineCol, config.sentimentDelimiter);
		/*
		 * Split the document into a train & test dataset for later model
		 * evaluation; A sentiment analysis model is evaluated with a
		 * regression evaluator
		 */
		Dataset<Row>[] splitted = document.randomSplit(config.getSplits());

		Dataset<Row> trainset = splitted[0];
		Dataset<Row> testset = splitted[1];
		
		ViveknSentimentModel model = trainer.train(trainset, "_text", "_label");

	    SAPredictor predictor = new SAPredictor(model);
	    Dataset<Row> predictions = predictor.predict(testset, "_text", "predicted");
	    
	    String metricsJson = SAEvaluator.evaluate(predictions, "_label", "predicted");    
		String paramsJson = config.getParamsAsJSON();

		String modelName = config.modelName;
		new SentimentManager().save(modelFs, modelMeta, modelName, paramsJson, metricsJson, model);
	    
	}
	
	@Override
	public void validateSchema(Schema inputSchema) {

		/** TEXT COLUMN **/

		Schema.Field textCol = inputSchema.getField(config.lineCol);
		if (textCol == null) {
			throw new IllegalArgumentException(String.format(
					"[%s] The input schema must contain the field that defines the labeled sentiment tokens.", this.getClass().getName()));
		}
		
		SchemaUtil.isString(inputSchema, config.lineCol);

	}

	public static class SentimentSinkConfig extends BaseSentimentConfig {

		private static final long serialVersionUID = 7018920867242125687L;

		@Description("The name of the field in the input schema that contains the labeled sentiment tokens.")
		@Macro
		public String lineCol;

		@Description("The delimiter to separate labels and associated tokens in the corpus. Default is '->'.")
		@Macro
		public String sentimentDelimiter;

		@Description("The split of the dataset into train & test data, e.g. 80:20. Default is 70:30")
		@Macro
		public String dataSplit;

		public SentimentSinkConfig() {
			dataSplit = "70:30";
			sentimentDelimiter = "->";
		}

		@Override
		public Map<String, Object> getParamsAsMap() {

			Map<String, Object> params = new HashMap<>();

			params.put("dataSplit", dataSplit);
			params.put("sentimentDelimiter", sentimentDelimiter);
			
			return params;

		}
		
		public void validate() {
			super.validate();
			
			if (Strings.isNullOrEmpty(lineCol)) {
				throw new IllegalArgumentException(String.format(
						"[%s] The name of the field that contains the labeled sentiment tokens must not be empty.",
						this.getClass().getName()));
			}

			if (Strings.isNullOrEmpty(sentimentDelimiter)) {
				throw new IllegalArgumentException(
						String.format("[%s] The sentiment delimiter must not be empty.",
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
