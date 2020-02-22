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

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.common.base.Strings;
import com.johnsnowlabs.nlp.annotators.LemmatizerModel;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;

import de.kp.works.core.text.TextCompute;
import de.kp.works.text.config.ModelConfig;
import de.kp.works.text.util.Names;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("Lemmatizer")
@Description("A transformation stage requires a trained Lemmatization model. It extracts "
		+ "normalized terms from a text document and maps each term onto its trained lemma. "
		+ "This stage adds an extra field to the input schema that contains the whitespace "
		+ "separated set of lemmas.")
public class Lemmatizer extends TextCompute {

	private static final long serialVersionUID = 1494670903195615242L;
	/*
	 * The result of this lemmatization stage is an Array[String] for normalized
	 * tokens and lemmas. It can be used with Spark ML model building that starts
	 * from this as an input format
	 */
	
	private LemmatizerConfig config;
	private LemmatizerModel model;

	public Lemmatizer(LemmatizerConfig config) {
		this.config = config;
	}

	@Override
	public void initialize(SparkExecutionPluginContext context) throws Exception {
		config.validate();

		model = new LemmatizerRecorder().read(context, config.modelName, config.modelStage);
		if (model == null)
			throw new IllegalArgumentException(
					String.format("[%s] A Lemmatization model with name '%s' does not exist.",
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
			outputSchema = getOutputSchema(inputSchema);
			stageConfigurer.setOutputSchema(outputSchema);

		}

	}
	/**
	 * This method computes predictions either by applying a trained Lemmatization
	 * model; as a result, the source dataset is enriched by two extra columns of
	 * data type Array[String]
	 */
	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

		LemmaPredictor predictor = new LemmaPredictor(model);
		Dataset<Row> predictions = predictor.predict(source, config.textCol, config.lemmaCol);

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
	public Schema getOutputSchema(Schema inputSchema) {

		List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());
		
		fields.add(Schema.Field.of(config.lemmaCol, Schema.of(Schema.Type.STRING)));		
		return Schema.recordOf(inputSchema.getRecordName() + ".predicted", fields);

	}

	public static class LemmatizerConfig extends ModelConfig {

		private static final long serialVersionUID = 2764272986545420558L;

		@Description(Names.TEXT_COL)
		@Macro
		public String textCol;

		@Description(Names.LEMMA_COL)
		@Macro
		public String lemmaCol;

		public LemmatizerConfig() {
			modelStage = "experiment";
		}

		public void validate() {
			super.validate();
			
			if (Strings.isNullOrEmpty(textCol)) {
				throw new IllegalArgumentException(String.format(
						"[%s] The name of the field that contains the text document must not be empty.",
						this.getClass().getName()));
			}
			
			if (Strings.isNullOrEmpty(lemmaCol)) {
				throw new IllegalArgumentException(String.format(
						"[%s] The name of the field that contains the assigned lemmas must not be empty.",
						this.getClass().getName()));
			}
			
		}
		
	}
}
