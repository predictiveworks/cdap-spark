package de.kp.works.text.topic;
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

import de.kp.works.text.recording.TopicRecorder;
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

import de.kp.works.core.SchemaUtil;
import de.kp.works.core.text.TextSink;

@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name("TopicBuilder")
@Description("A building stage for a Latent Dirichlet Allocation (LDA) model. In contrast to "
		+ "the LDABuilder plugin, this stage leverages an implicit document vectorization based "
		+ "on the term counts of the provided corpus. The trained model can be used to either "
		+ "determine the topic distribution per document or term-distribution per topic.")
public class TopicBuilder extends TextSink {

	private static final long serialVersionUID = 6709578668752668893L;

	private final TopicSinkConfig config;
	
	public TopicBuilder(TopicSinkConfig config) {
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
		 * Split the source into a train & test dataset for later clustering
		 * evaluation
		 */
		Dataset<Row>[] splitted = source.randomSplit(config.getSplits());

		Dataset<Row> trainset = splitted[0];
		Dataset<Row> testset = splitted[1];

		LDATopic approach = new LDATopic();
		approach.setTextCol(config.textCol);
		
		approach.setVocabSize(config.vocabSize);
		approach.setK(config.k);
		approach.setMaxIter(config.maxIter);
		
		LDATopicModel model = approach.fit(trainset);
		/*
		 * The evaluation of the LDATopic model is performed leveraging 
		 * the perplexity measure; this is a measure to determine how well
		 * the features (vectors) of the testset are represented by the
		 * word distribution of the topics.
		 */
		Double perplexity = model.logPerplexity(testset);
		Double likelihood = model.logLikelihood(testset);
		/*
		 * The perplexity & likelihood coefficient is specified as intrinsic
		 * JSON metrics for this LDATopic model and stored by the TopicManager
		 */
		Map<String, Object> metrics = new HashMap<>();

		metrics.put("perplexity", perplexity);
		metrics.put("likelihood", likelihood);
		/*
		 * STEP #3: Store trained LDATopic model including its associated
		 * parameters and metrics
		 */
		String modelMetrics = new Gson().toJson(metrics);
		String modelParams = config.getParamsAsJSON();

		String modelName = config.modelName;
		String modelStage = config.modelStage;
		
		new TopicRecorder(configReader)
				.track(context, modelName, modelStage, modelParams, modelMetrics, model);
				
	}
	
	@Override
	public void validateSchema(Schema inputSchema) {

		/* TEXT COLUMN */

		Schema.Field textCol = inputSchema.getField(config.textCol);
		if (textCol == null) {
			throw new IllegalArgumentException(
					String.format("[%s] The input schema must contain the field that defines the text document.",
							this.getClass().getName()));
		}

		SchemaUtil.isString(inputSchema, config.textCol);

	}

	public static class TopicSinkConfig extends BaseTopicConfig {

		private static final long serialVersionUID = -5856596535004094760L;

		@Description("The split of the dataset into train & test data, e.g. 80:20. Default is 90:10.")
		@Macro
		public String dataSplit;

		@Description("The number of topics that have to be created. Default is 10.")
		@Macro
		public Integer k;

		@Description("The (maximum) number of iterations the algorithm has to execute. Default value: 20.")
		@Macro
		public Integer maxIter;

		@Description("The size of the vocabulary to build vector representations. Default is 10000.")
		@Macro
		public Integer vocabSize;
		
		public TopicSinkConfig() {

			dataSplit = "90:10";
			modelStage = "experiment";
			
			vocabSize = 10000;
			
			k = 10;
			maxIter = 20;
			
			
		}

		public Map<String, Object> getParamsAsMap() {

			Map<String, Object> params = new HashMap<>();

			params.put("k", k);
			params.put("maxIter", maxIter);

			params.put("dataSplit", dataSplit);
			params.put("vocabSize", vocabSize);
			
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
				throw new IllegalArgumentException(
						String.format("[%s] The number of topics must be greater than 1.", this.getClass().getName()));
			}
			if (maxIter <= 0) {
				throw new IllegalArgumentException(String
						.format("[%s] The number of iterations must be greater than 0.", this.getClass().getName()));
			}

		}
		
	}
}
