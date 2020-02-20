package de.kp.works.text.dep;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.common.base.Strings;
import com.google.gson.Gson;
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
import com.johnsnowlabs.nlp.annotators.parser.dep.DependencyParserModel;

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
@Name("DependencyBuilder")
@Description("A building stage for an Apache Spark-NLP based Unlabeled Dependency Parser model.")
public class DependencyBuilder extends TextSink {

	private static final long serialVersionUID = 3384231592795011247L;

	private DependencySinkConfig config;
	
	public DependencyBuilder(DependencySinkConfig config) {
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
		
		DepTrainer trainer = new DepTrainer();
		DependencyParserModel model = trainer.train(source, config.lineCol, params);

		Map<String,Object> metrics = new HashMap<>();
		String metricsJson = new Gson().toJson(metrics);

		String modelName = config.modelName;
		new DependencyManager().save(context, modelName, paramsJson, metricsJson, model);
	    
	}
	
	@Override
	public void validateSchema(Schema inputSchema) {

		/** LINE COLUMN **/

		Schema.Field textCol = inputSchema.getField(config.lineCol);
		if (textCol == null) {
			throw new IllegalArgumentException(
					String.format("[%s] The input schema must contain the field that contains the corpus document.",
							this.getClass().getName()));
		}

		SchemaUtil.isString(inputSchema, config.lineCol);

	}

	public static class DependencySinkConfig extends BaseDependencyConfig {

		private static final long serialVersionUID = -2112548496022872235L;

		@Description("The name of the field in the input schema that contains the annotated corpus document.")
		@Macro
		public String lineCol;

		@Description("The format of the training corpus. Supported values are 'conll-u' (CoNLL-U corpus) and 'treebank' (TreeBank corpus). Default is 'conll-u'.")
		@Macro
		public String format;

		@Description("The number of iterations to train the model. Default is 10.")
		@Macro
		public Integer numIter;
		
		public DependencySinkConfig() {
			format = "conll-u";
			numIter = 10;
		}

		@Override
		public Map<String, Object> getParamsAsMap() {
			
			Map<String, Object> params = new HashMap<>();

			params.put("numIter", numIter);
			params.put("format", format);

			return params;
		
		}
		
		public void validate() {
			super.validate();
			
			if (Strings.isNullOrEmpty(lineCol)) {
				throw new IllegalArgumentException(String.format(
						"[%s] The name of the field that contains the corpus document must not be empty.",
						this.getClass().getName()));
			}

			if (numIter < 1)
				throw new IllegalArgumentException(String.format(
						"[%s] The number of iterations must be at least 1.", this.getClass().getName()));
			
		}
	}
}
