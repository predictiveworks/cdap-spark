package de.kp.works.text.ner;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.gson.Gson;
import com.johnsnowlabs.nlp.annotators.ner.crf.NerCrfModel;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.api.batch.SparkPluginContext;
import de.kp.works.core.SchemaUtil;
import de.kp.works.core.TextSink;
import de.kp.works.core.ml.SparkMLManager;
import de.kp.works.text.embeddings.Word2VecManager;
import de.kp.works.text.embeddings.Word2VecModel;

@Plugin(type = "sparksink")
@Name("NERSink")
@Description("A building stage for an Apache Spark-NLP based NER (CRF) model.")
public class NERSink extends TextSink {

	private static final long serialVersionUID = 4968897885133224506L;
	
	private NERSinkConfig config;
	private Word2VecModel word2vec;

	public NERSink(NERSinkConfig config) {
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
		 * Text analysis model components and metadata are persisted in a CDAP FileSet
		 * as well as a Table; at this stage, we have to make sure that these internal
		 * metadata structures are present
		 */
		SparkMLManager.createTextanalysisIfNotExists(context);
		/*
		 * Retrieve text analysis specified dataset for later use incompute
		 */
		modelFs = SparkMLManager.getTextanalysisFS(context);
		modelMeta = SparkMLManager.getTextanalysisMeta(context);

		word2vec = new Word2VecManager().read(modelFs, modelMeta, config.embeddingName);
		if (word2vec == null)
			throw new IllegalArgumentException(
					String.format("[%s] A Word2Vec embedding model with name '%s' does not exist.",
							this.getClass().getName(), config.embeddingName));

	}

	@Override
	public void compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

		Map<String, Object> params = config.getParamsAsMap();
		String paramsJson = config.getParamsAsJSON();
		
		NERTrainer trainer = new NERTrainer(word2vec);
		NerCrfModel model = trainer.train(source, config.textCol, params);

		Map<String,Object> metrics = new HashMap<>();
		String metricsJson = new Gson().toJson(metrics);

		String modelName = config.modelName;
		new NERManager().save(modelFs, modelMeta, modelName, paramsJson, metricsJson, model);
	    
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
	
	public static class NERSinkConfig extends BaseNERConfig {

		private static final long serialVersionUID = 2523264167211336615L;

		@Description("Minimum number of epochs to train. Default is 10.")
		@Macro
		public Integer minEpochs;
		
		@Description("Maximum number of epochs to train. Default is 1000.")
		@Macro
		public Integer maxEpochs;
		
		public NERSinkConfig() {
			minEpochs = 10;
			maxEpochs = 1000;
		}

		@Override
		public Map<String, Object> getParamsAsMap() {
			
			Map<String, Object> params = new HashMap<>();

			params.put("minEpochs", minEpochs);
			params.put("maxEpochs", maxEpochs);

			return params;
		
		}
		
		public void validate() {
			super.validate();

			if (minEpochs < 0)
				throw new IllegalArgumentException(String.format(
						"[%s] The minimum number of epochs to train must be at least 0.", this.getClass().getName()));

			if (maxEpochs < 0)
				throw new IllegalArgumentException(String.format(
						"[%s] The maximum number of epochs to train must be at least 0.", this.getClass().getName()));

			if (minEpochs > maxEpochs)
				throw new IllegalArgumentException(
						String.format("[%s] The maximum number of epochs must be greater or equal than the minimum number.", this.getClass().getName()));

		}
		
	}
}
