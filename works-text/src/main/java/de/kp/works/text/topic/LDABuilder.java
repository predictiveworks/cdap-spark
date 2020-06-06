package de.kp.works.text.topic;
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

import com.google.common.base.Strings;
import com.google.gson.Gson;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkPluginContext;
import io.cdap.cdap.etl.api.batch.SparkSink;

import de.kp.works.core.SchemaUtil;
import de.kp.works.core.cluster.LDARecorder;
import de.kp.works.core.ml.SparkMLManager;
import de.kp.works.core.text.TextSink;
import de.kp.works.text.embeddings.Word2VecRecorder;
import de.kp.works.text.embeddings.Word2VecModel;

@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name("LDABuilder")
@Description("A building stage for a Latent Dirichlet Allocation (LDA) model. An LDA model can be used for "
		+ "text clustering or labeling. This model training stage requires a pre-trained Word Embedding model.")
public class LDABuilder extends TextSink {

	private static final long serialVersionUID = 4678742201151266996L;

	private LDATextSinkConfig config;
	private Word2VecModel word2vec;
	
	public LDABuilder(LDATextSinkConfig config) {
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
	public void prepareRun(SparkPluginContext context) throws Exception {
		/*
		 * Clustering model components and metadata are persisted in a CDAP FileSet
		 * as well as a Table; at this stage, we have to make sure that these internal
		 * metadata structures are present
		 */
		SparkMLManager.createClusteringIfNotExists(context);
		/*
		 * Retrieve text analysis specified Word2Vec embedding model for 
		 * later use in compute, Word2Vec model do not have any metrics, 
		 * i.e. there is no model option: always the latest model is used
		 */
		word2vec = new Word2VecRecorder().read(context, config.embeddingName, config.embeddingStage, LATEST_MODEL);
		if (word2vec == null)
			throw new IllegalArgumentException(
					String.format("[%s] A Word2Vec embedding model with name '%s' does not exist.",
							this.getClass().getName(), config.embeddingName));

		
	}

	@Override
	public void compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

		Map<String, Object> params = config.getParamsAsMap();
		String modelParams = config.getParamsAsJSON();
		
		LDATrainer trainer = new LDATrainer(word2vec);
		Dataset<Row> vectorset = trainer.vectorize(source, config.textCol, params);
		/*
		 * Split the vectorset into a train & test dataset for later clustering
		 * evaluation
		 */
		Dataset<Row>[] splitted = vectorset.randomSplit(config.getSplits());

		Dataset<Row> trainset = splitted[0];
		Dataset<Row> testset = splitted[1];
		
		LDAModel model = trainer.train(trainset, params);
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

		/* Add unused metrics to be schema compliant */
		metrics.put("silhouette_euclidean", 0.0);
		metrics.put("silhouette_cosine", 0.0);
		
		/*
		 * STEP #3: Store trained LDA model including its associated
		 * parameters and metrics
		 */
		String modelMetrics = new Gson().toJson(metrics);

		String modelName = config.modelName;
		String modelStage = config.modelStage;
		
		String modelPack = "WorksText";
		new LDARecorder().track(context, modelName, modelPack, modelStage, modelParams, modelMetrics, model);
	    
	}

	@Override
	public void validateSchema(Schema inputSchema) {

		/** TEXT COLUMN **/

		Schema.Field textCol = inputSchema.getField(config.textCol);
		if (textCol == null) {
			throw new IllegalArgumentException(
					String.format("[%s] The input schema must contain the field that contains the text document.",
							this.getClass().getName()));
		}

		SchemaUtil.isString(inputSchema, config.textCol);

	}

	public static class LDATextSinkConfig extends LDATextConfig {

		private static final long serialVersionUID = -2548563805267897668L;

		@Description("The split of the dataset into train & test data, e.g. 80:20. Default is 90:10")
		@Macro
		public String dataSplit;

		@Description("The number of topics that have to be created. Default is 10.")
		@Macro
		private Integer k;

		@Description("The (maximum) number of iterations the algorithm has to execute. Default value: 20")
		@Macro
		private Integer maxIter;

		public LDATextSinkConfig() {

			dataSplit = "90:10";
			modelStage = "experiment";
			
			embeddingStage = "experiment";
			poolingStrategy = "average";

			k = 10;
			maxIter = 20;

		}

		public Map<String, Object> getParamsAsMap() {

			Map<String, Object> params = new HashMap<String, Object>();

			params.put("k", k);
			params.put("maxIter", maxIter);

			params.put("dataSplit", dataSplit);
			params.put("strategy", getStrategy());

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
				throw new IllegalArgumentException(
						String.format("[%s] The number of topics must be greater than 1.", this.getClass().getName()));
			}
			if (maxIter <= 0) {
				throw new IllegalArgumentException(String
						.format("[%s] The number of iterations must be greater than 0.", this.getClass().getName()));
			}
			
			if (Strings.isNullOrEmpty(poolingStrategy)) {
				throw new IllegalArgumentException(String.format(
						"[%s] The name of the field that contains the pooling strategy must not be empty.",
						this.getClass().getName()));
			}

		}

	}
}
