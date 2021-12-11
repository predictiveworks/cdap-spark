package de.kp.works.text.ner;
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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.common.base.Strings;
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
import de.kp.works.core.BaseConfig;

import de.kp.works.text.associations.RelNER;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("NERRelation")
@Description("A transformation stage that leverages the result of an NER tagging stage and "
		+ "extracts relations between named entities from their co-occurring.")
public class NERRelation extends TextCompute {

	private static final long serialVersionUID = 1L;

	private final NERRelationConfig config;
	
	public NERRelation(NERRelationConfig config) {
		this.config = config;
	}

	@Override
	public void initialize(SparkExecutionPluginContext context) throws Exception {
		config.validate();

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
	
	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

		RelNER transformer = new RelNER();
		transformer.setTokenCol(config.tokenCol);
		transformer.setNerCol(config.nerCol);
		
		return transformer.transform(source);
		
	}
	
	@Override
	public void validateSchema(Schema inputSchema) {

		/* TOKEN COLUMN */

		Schema.Field tokenCol = inputSchema.getField(config.tokenCol);
		if (tokenCol == null) {
			throw new IllegalArgumentException(
					String.format("[%s] The input schema must contain the field that contained the extracted tokens.",
							this.getClass().getName()));
		}

		isArrayOfString(config.tokenCol);

		/* NER COLUMN */

		Schema.Field nerCol = inputSchema.getField(config.nerCol);
		if (nerCol == null) {
			throw new IllegalArgumentException(
					String.format("[%s] The input schema must contain the field that contained the extracted named entity tags.",
							this.getClass().getName()));
		}

		isArrayOfString(config.nerCol);

	}

	/**
	 * A helper method to compute the output schema in that use cases where an input
	 * schema is explicitly given
	 */
	public Schema getOutputSchema(Schema inputSchema) {

		List<Schema.Field> fields = new ArrayList<>();
		
		fields.add(Schema.Field.of("ante_token", Schema.of(Schema.Type.STRING)));
		fields.add(Schema.Field.of("ante_tag", Schema.of(Schema.Type.STRING)));
		
		fields.add(Schema.Field.of("cons_token", Schema.of(Schema.Type.STRING)));
		fields.add(Schema.Field.of("cons_tag", Schema.of(Schema.Type.STRING)));
		
		fields.add(Schema.Field.of("toc_score", Schema.of(Schema.Type.DOUBLE)));
		fields.add(Schema.Field.of("doc_score", Schema.of(Schema.Type.DOUBLE)));
		
		return Schema.recordOf(inputSchema.getRecordName() + ".related", fields);

	}
	
	public static class NERRelationConfig extends BaseConfig {

		private static final long serialVersionUID = 2008479535769627131L;

		@Description("The name of the field in the input schema that contains the extracted tokens.")
		@Macro
		public String tokenCol;

		@Description("The name of the field in the input schema that contains the extracted named entity tags.")
		@Macro
		public String nerCol;

		public void validate() {
			super.validate();
			
			if (Strings.isNullOrEmpty(tokenCol)) {
				throw new IllegalArgumentException(String.format(
						"[%s] The name of the field that contains the extracted tokens must not be empty.",
						this.getClass().getName()));
			}
			
			if (Strings.isNullOrEmpty(nerCol)) {
				throw new IllegalArgumentException(String.format(
						"[%s] The name of the field that contains the extracted named entity tags must not be empty.",
						this.getClass().getName()));
			}
			
		}
		
	}

}
