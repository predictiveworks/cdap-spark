package de.kp.works.text.spell;
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
import java.util.List;

import de.kp.works.text.recording.SpellRecorder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.common.base.Strings;
import com.johnsnowlabs.nlp.annotators.spell.norvig.NorvigSweetingModel;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;

import de.kp.works.core.text.TextCompute;
import de.kp.works.text.config.ModelConfig;
import de.kp.works.text.util.Names;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("NorvigChecker")
@Description("A transformation stage that checks the spelling of each normalized term in a text document, "
		+ "leveraging a trained Spark NLP Norvig Spelling model. This stage adds an extra field to the input schema "
		+ "that contains the whitespace separated set of suggested spelling corrections.")
public class NorvigChecker extends TextCompute {

	private static final long serialVersionUID = 2369095136053899600L;

	private final SpellCheckerConfig config;
	private NorvigSweetingModel model;

	public NorvigChecker(SpellCheckerConfig config) {
		this.config = config;
	}

	@Override
	public void initialize(SparkExecutionPluginContext context) throws Exception {
		config.validate();

		/*
		 * Norvig models do not have any metrics, i.e. there
		 * is no model option: always the latest model is used
		 */
		model = new SpellRecorder(configReader)
				.read(context, config.modelName, config.modelStage, LATEST_MODEL);

		if (model == null)
			throw new IllegalArgumentException(
					String.format("[%s] A Norvig Spell Checking model with name '%s' does not exist.",
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
	 * This method computes predictions either by applying a trained Norvig Sweeting
	 * model; as a result, the source dataset is enriched by two extra columns of
	 * data type Array[String]
	 */
	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

		NorvigPredictor predictor = new NorvigPredictor(model);
		return predictor.predict(source, config.textCol, config.spellCol, config.threshold);
		
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

		isString(config.textCol);

	}

	/**
	 * A helper method to compute the output schema in that use cases where an input
	 * schema is explicitly given
	 */
	protected Schema getOutputSchema(Schema inputSchema) {

		assert inputSchema.getFields() != null;
		List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());
		fields.add(Schema.Field.of(config.spellCol, Schema.of(Schema.Type.STRING)));
		
		return Schema.recordOf(inputSchema.getRecordName() + ".predicted", fields);

	}

	public static class SpellCheckerConfig extends ModelConfig {

		private static final long serialVersionUID = 2295039730979859235L;

		@Description(Names.TEXT_COL)
		@Macro
		public String textCol;

		@Description(Names.SPELL_COL)
		@Macro
		public String spellCol;

		@Description("The probability threshold above which a suggested term spelling is accepted. Default is 0.75.")
		@Macro
		public Double threshold;

		public SpellCheckerConfig() {
			modelStage = "experiment";
			threshold = 0.75;
		}
		
		public void validate() {
			super.validate();
			
			if (Strings.isNullOrEmpty(textCol)) {
				throw new IllegalArgumentException(String.format(
						"[%s] The name of the field that contains the text document must not be empty.",
						this.getClass().getName()));
			}
			
			if (Strings.isNullOrEmpty(spellCol)) {
				throw new IllegalArgumentException(String.format(
						"[%s] The name of the field that contains the suggested spellings must not be empty.",
						this.getClass().getName()));
			}

			if (threshold < 0.0 || threshold > 1.0) {
				throw new IllegalArgumentException(String.format(
						"[%s] The probability threshold must be in the range [0, 1].",
						this.getClass().getName()));
			}
			
		}
		
	}

}
