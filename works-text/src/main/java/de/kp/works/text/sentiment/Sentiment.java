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

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.common.base.Strings;
import com.johnsnowlabs.nlp.annotators.sda.vivekn.ViveknSentimentModel;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.Params;
import de.kp.works.core.text.TextCompute;
import de.kp.works.text.config.ModelConfig;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("Sentiment")
@Description("A transformation stage that predicts sentiment labels (positive or negative) for "
		+ "text documents, leveraging a trained Sentiment Analysis model.")
public class Sentiment extends TextCompute {

	private static final long serialVersionUID = -5009925022021738613L;

	private SentimentConfig config;
	private ViveknSentimentModel model;

	public Sentiment(SentimentConfig config) {
		this.config = config;
	}

	@Override
	public void initialize(SparkExecutionPluginContext context) throws Exception {
		((SentimentConfig) config).validate();

		model = new SentimentRecorder().read(context, config.modelName, config.modelStage, config.modelOption);
		if (model == null)
			throw new IllegalArgumentException(
					String.format("[%s] A sentiment analysis model with name '%s' does not exist.",
							this.getClass().getName(), config.modelName));

	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {

		config.validate();

		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
		/*
		 * Try to determine input and output schema; if these schemas are not explicitly
		 * specified, they will be inferred from the provided data records
		 */
		inputSchema = stageConfigurer.getInputSchema();
		if (inputSchema != null) {
			validateSchema(inputSchema);
			/*
			 * In cases where the input schema is explicitly provided, we determine the
			 * output schema by explicitly adding the prediction column
			 */
			outputSchema = getOutputSchema(inputSchema, config.predictionCol);
			stageConfigurer.setOutputSchema(outputSchema);

		}

	}
	/**
	 * This method computes predictions either by applying a trained Sentiment
	 * Analysis model; as a result, the source dataset is enriched by an extra 
	 * column (predictionCol) that specifies the target variable in form of a 
	 * String value
	 */
	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

		SAPredictor predictor = new SAPredictor(model);
		Dataset<Row> predictions = predictor.predict(source, config.textCol, config.predictionCol);

		return predictions;
		
	}
	@Override
	public void validateSchema(Schema inputSchema) {

		/** TEXT COLUMN **/

		Schema.Field textCol = inputSchema.getField(config.textCol);
		if (textCol == null) {
			throw new IllegalArgumentException(
					String.format("[%s] The input schema must contain the field that defines the text document.",
							this.getClass().getName()));
		}

		isString(config.textCol);

	}

	/**
	 * A helper method to compute the output schema in that use cases where an input
	 * schema is explicitly given
	 */
	protected Schema getOutputSchema(Schema inputSchema, String predictionField) {

		List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());
		
		fields.add(Schema.Field.of(predictionField, Schema.of(Schema.Type.STRING)));
		return Schema.recordOf(inputSchema.getRecordName() + ".predicted", fields);

	}

	public static class SentimentConfig extends ModelConfig {

		private static final long serialVersionUID = -7796809782922479970L;

		@Description(Params.MODEL_OPTION)
		@Macro
		public String modelOption;

		@Description("The name of the field in the input schema that contains the document.")
		@Macro
		public String textCol;

		@Description("The name of the field in the output schema that contains the predicted sentiment.")
		@Macro
		public String predictionCol;

		public SentimentConfig() {
			modelOption = BEST_MODEL;
			modelStage = "experiment";
		}
		public void validate() {
			super.validate();

			if (Strings.isNullOrEmpty(textCol)) {
				throw new IllegalArgumentException(
						String.format("[%s] The name of the field that contains the document must not be empty.",
								this.getClass().getName()));
			}
			
			if (Strings.isNullOrEmpty(predictionCol)) {
				throw new IllegalArgumentException(String.format(
						"[%s] The name of the field that contains the predicted sentiment value must not be empty.",
						this.getClass().getName()));
			}
			
		}
	}
}
