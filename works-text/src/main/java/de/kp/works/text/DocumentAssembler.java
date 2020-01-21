package de.kp.works.text;
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
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.common.base.Strings;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.BaseCompute;
import de.kp.works.core.BaseConfig;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("DocumentAssembler")
@Description("A transformation stage that leverages the Spark NLP Document Assembler to map an input "
		+ "text field into a document field, preparing it for further NLP annotations.")
public class DocumentAssembler extends BaseCompute {

	private static final long serialVersionUID = 5882041999274662218L;

	private DocumentAssemblerConfig config;

	public DocumentAssembler(DocumentAssemblerConfig config) {
		this.config = config;
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

			outputSchema = getOutputSchema(inputSchema);
			stageConfigurer.setOutputSchema(outputSchema);

		}

	}

	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

		Properties props = new Properties();
		props.setProperty("input.col", config.inputCol);
		props.setProperty("output.col", config.outputCol);

		return NLP.assembleDocument(source, props);

	}
	
	@Override
	public void validateSchema() {
		
		/** INPUT COLUMN **/

		Schema.Field textCol = inputSchema.getField(config.inputCol);
		if (textCol == null) {
			throw new IllegalArgumentException(String.format(
					"[%s] The input schema must contain the field that defines the text.", this.getClass().getName()));
		}

		isString(config.inputCol);
		
	}
	
	public Schema getOutputSchema(Schema inputSchema) {

		List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());
		
		fields.add(Schema.Field.of(config.outputCol, Schema.arrayOf(Schema.of(Schema.Type.STRING))));
		return Schema.recordOf(inputSchema.getRecordName() + ".transformed", fields);

	}	
	
	public static class DocumentAssemblerConfig extends BaseConfig {
		/*
		 * The DocumentAssembler operate with cleanUp mode 'disabled' to keep 
		 * the original text as we expect that the user needs to head back to 
		 * source
		 */
		private static final long serialVersionUID = 6075596749044302438L;

		@Description("The name of the field in the input schema that contains the text to annotate.")
		@Macro
		public String inputCol;

		@Description("The name of the field in the output schema that contains the document annotations.")
		@Macro
		public String outputCol;
		
		public void validate() {
			super.validate();

			if (Strings.isNullOrEmpty(inputCol))
				throw new IllegalArgumentException(
						String.format("[%s] The name of the field that contains the text must not be empty.",
								this.getClass().getName()));
			
			if (Strings.isNullOrEmpty(inputCol))
				throw new IllegalArgumentException(
						String.format("[%s] The name of the field that contains the document annotations must not be empty.",
								this.getClass().getName()));
			
		}
	}
}
