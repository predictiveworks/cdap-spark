package de.kp.works.text.pos;
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
import com.johnsnowlabs.nlp.annotators.pos.perceptron.PerceptronModel;

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
@Name("POSBuilder")
@Description("A building stage for a Part of Speech model.")
public class POSBuilder extends TextSink {

	private static final long serialVersionUID = 1L;

	private POSSinkConfig config;

	public POSBuilder(POSSinkConfig config) {
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
		
		POSTrainer trainer = new POSTrainer();
		PerceptronModel model = trainer.train(source, config.lineCol, params);

		Map<String,Object> metrics = new HashMap<>();
		String metricsJson = new Gson().toJson(metrics);

		String modelName = config.modelName;
		new POSRecorder().track(context, modelName, paramsJson, metricsJson, model);
	    
	}

	@Override
	public void validateSchema(Schema inputSchema) {

		/** LINE COLUMN **/

		Schema.Field textCol = inputSchema.getField(config.lineCol);
		if (textCol == null) {
			throw new IllegalArgumentException(
					String.format("[%s] The input schema must contain the field that defines the annotated sentences.",
							this.getClass().getName()));
		}

		SchemaUtil.isString(inputSchema, config.lineCol);

	}

	public static class POSSinkConfig extends BasePOSConfig {

		private static final long serialVersionUID = 5634430019806584847L;

		@Description("Name of the input text field that contains the annotated sentences for training purpose.")
		@Macro
		public String lineCol;

		@Description("The delimiter in the input text line to separate tokens and POS tags. Default is '|'.")
		@Macro
		public String delimiter;

		@Description("The maximum number of iterations to train the Part-of-Speech model. Default is 5.")
		@Macro
		public Integer maxIter;

		public POSSinkConfig() {
			delimiter = "|";
			maxIter = 5;
		}

		@Override
		public Map<String, Object> getParamsAsMap() {

			Map<String, Object> params = new HashMap<>();
			params.put("maxIter", maxIter);
			return params;

		}

		public void validate() {
			super.validate();

			if (Strings.isNullOrEmpty(lineCol)) {
				throw new IllegalArgumentException(String.format(
						"[%s] The name of the field that contains the annotated sentences value must not be empty.",
						this.getClass().getName()));
			}

			if (Strings.isNullOrEmpty(delimiter)) {
				throw new IllegalArgumentException(
						String.format("[%s] The delimiter must not be empty.", this.getClass().getName()));
			}

			if (maxIter < 1)
				throw new IllegalArgumentException(String.format(
						"[%s] The maximum number of iterations must be at least 1.", this.getClass().getName()));

		}

	}

}
