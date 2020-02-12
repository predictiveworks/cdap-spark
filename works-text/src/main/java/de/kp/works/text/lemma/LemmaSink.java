package de.kp.works.text.lemma;
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
import com.google.gson.Gson;

import com.johnsnowlabs.nlp.annotators.LemmatizerModel;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.SchemaUtil;
import de.kp.works.core.text.TextSink;

@Plugin(type = "sparksink")
@Name("LemmaSink")
@Description("A building stage for a Lemmatization model. The training corpus assigns each lemma "
		+ "to a set of term variations that all map onto this lemma.")
public class LemmaSink extends TextSink {

	private static final long serialVersionUID = 3819386514287004996L;

	private LemmaSinkConfig config;
	
	public LemmaSink(LemmaSinkConfig config) {
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

		Map<String, Object> params = config.getParamsAsMap();
		String paramsJson = config.getParamsAsJSON();
		
		LemmaTrainer trainer = new LemmaTrainer();
		LemmatizerModel model = trainer.train(source, config.lineCol, params);

		Map<String,Object> metrics = new HashMap<>();
		String metricsJson = new Gson().toJson(metrics);

		String modelName = config.modelName;
		new LemmaManager().save(modelFs, modelMeta, modelName, paramsJson, metricsJson, model);
	    
	}

	@Override
	public void validateSchema(Schema inputSchema) {

		/** LINE COLUMN **/

		Schema.Field textCol = inputSchema.getField(config.lineCol);
		if (textCol == null) {
			throw new IllegalArgumentException(
					String.format("[%s] The input schema must contain the field that contains the lemma and assigned tokens.",
							this.getClass().getName()));
		}

		SchemaUtil.isString(inputSchema, config.lineCol);

	}

	public static class LemmaSinkConfig extends BaseLemmaConfig {

		private static final long serialVersionUID = 6743392367262183249L;

		@Description("The name of the field in the input schema that contains the lemma and assigned tokens.")
		@Macro
		public String lineCol;

		@Description("The delimiter to separate lemma and associated tokens in the corpus. Key & value delimiter must be different.")
		@Macro
		public String keyDelimiter;

		@Description("The delimiter to separate the tokens in the corpus. Key & value delimiter must be different.")
		@Macro
		public String valueDelimiter;

		public LemmaSinkConfig() {
			keyDelimiter = "->";
			valueDelimiter = "\\S+";
		}

		@Override
		public Map<String, Object> getParamsAsMap() {

			Map<String, Object> params = new HashMap<>();

			params.put("keyDelimiter", keyDelimiter);
			params.put("valueDelimiter", valueDelimiter);
			
			return params;

		}
		
		public void validate() {
			super.validate();
			
			if (Strings.isNullOrEmpty(lineCol)) {
				throw new IllegalArgumentException(String.format(
						"[%s] The name of the field that contains the lemma and assigned tokens.",
						this.getClass().getName()));
			}
			if (Strings.isNullOrEmpty(keyDelimiter)) {
				throw new IllegalArgumentException(
						String.format("[%s] The lemma delimiter must not be empty.",
								this.getClass().getName()));
			}
			if (Strings.isNullOrEmpty(valueDelimiter)) {
				throw new IllegalArgumentException(
						String.format("[%s] The token delimiter must not be empty.",
								this.getClass().getName()));
			}			
			if (keyDelimiter.equals(valueDelimiter))
				throw new IllegalArgumentException(
						String.format("[%s] The lemma & token delimiter must not be empty.",
								this.getClass().getName()));
			
		}
		
	}
}
