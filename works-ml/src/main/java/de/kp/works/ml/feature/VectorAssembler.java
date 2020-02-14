package de.kp.works.ml.feature;
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
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.NumericType;
import org.apache.spark.sql.types.StructField;

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

import de.kp.works.core.BaseConfig;
import de.kp.works.core.SchemaUtil;
import de.kp.works.core.feature.FeatureCompute;
import de.kp.works.ml.MLUtils;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("VectorAssembler")
@Description("A transformation stage that leverages the Apache Spark ML Vector Assembler to merge multiple numeric "
		+ "(or numeric vector) fields into a single feature vector.")
public class VectorAssembler extends FeatureCompute {

	private static final long serialVersionUID = 2001829171672928424L;

	private VectorAssemblerConfig config;
	
	public VectorAssembler(VectorAssemblerConfig config) {
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
			
			validateSchema(inputSchema);
			/*
			 * In cases where the input schema is explicitly provided, we determine the
			 * output schema by explicitly adding the output column
			 */
			outputSchema = getOutputSchema(inputSchema, config.outputCol);
			stageConfigurer.setOutputSchema(outputSchema);

		}

	}
	
	/**
	 * Determine whether the selected input fields are either of 
	 * NUMERIC data type or an Array[Numeric]
	 */
	@Override
	public void validateSchema(Schema inputSchema) {
		config.validateSchema(inputSchema);
	}
	
	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

		/*
		 * The list of all columns that are used by the assembler
		 */
		List<String> assembleCols = new ArrayList<>();
		/*
		 * Feature vectors have to be transformed into Apache Spark Vector
		 * data types; this is done by assigning new vector names;
		 */
		int vectorCnt = 0;
		List<String> vectorCols = new ArrayList<>();
		
		List<String> columns = Arrays.asList(config.getFields());
		StructField[] schemaFields = source.schema().fields();
		
		Dataset<Row> dataset = source;
		
		for (StructField schemaField: schemaFields) {
			
			String fieldName = schemaField.name();
			if (!columns.contains(fieldName)) continue;
			/*
			 * The field is selected by the user 
			 */
			DataType fieldType = schemaField.dataType();
			if (fieldType instanceof DoubleType || fieldType instanceof NumericType) {
				assembleCols.add(fieldName);
			
			} else if (fieldType instanceof ArrayType) {
				/*
				 * We expect (see schema validation) that the component
				 * data type of this Array is Numeric, i.e. no additional
				 * check is performed here.
				 */
				vectorCnt += 1;
				String vectorCol = "_vector" + vectorCnt;
				
				assembleCols.add(vectorCol);
				vectorCols.add(vectorCol);

				dataset = MLUtils.vectorize(source, fieldName, vectorCol, true);
				
			} else {
				throw new IllegalArgumentException(String.format("[VectorAssembler] Data type of field '%s' is not supported.", fieldName));
			}
		}

		org.apache.spark.ml.feature.VectorAssembler transformer = new org.apache.spark.ml.feature.VectorAssembler();
		
		String[] inputCols = assembleCols.toArray(new String[assembleCols.size()]);
		transformer.setInputCols(inputCols);

		/*
		 * An intermediate column _vector is used as Apache Spark outputs
		 * a Vector column that is no compliant with CDAPs data types
		 */
		transformer.setOutputCol("_vector");		
		/* Transform dataset and devectorize the output column */
		Dataset<Row> output = MLUtils.devectorize(transformer.transform(dataset), "_vector", config.outputCol).drop("_vector");
		if (vectorCols.size() > 0) {
			/*
			 * Finally temporary vector columns are removed from the output
			 */
			String[] dropCols = vectorCols.toArray(new String[vectorCols.size()]);
			return output.drop(dropCols);

		} else
			return output;

	}

	/**
	 * A helper method to compute the output schema in that use cases where an input
	 * schema is explicitly given
	 */
	public Schema getOutputSchema(Schema inputSchema, String outputField) {

		List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());
		
		fields.add(Schema.Field.of(outputField, Schema.arrayOf(Schema.of(Schema.Type.DOUBLE))));
		return Schema.recordOf(inputSchema.getRecordName() + ".transformed", fields);

	}	

	public static class VectorAssemblerConfig extends BaseConfig {

		private static final long serialVersionUID = 1127092936375337969L;
		
		@Description("The comma-separated list of numeric (or numeric vector) fields that have to be be assembled.")
		@Macro
		public String inputCols;

		@Description("The name of the field in the output schema that contains the transformed features.")
		@Macro
		public String outputCol;
		
		public VectorAssemblerConfig() {			
		}
		
		public String[] getFields() {
			
			String[] tokens = inputCols.split(",");
			List<String> trimmed = new ArrayList<>();

			for (String token: tokens) {
				trimmed.add(token.trim());
			}
			
			return trimmed.toArray(new String[trimmed.size()]);

		}
		
		public void validate() {
			super.validate();
			
			if (Strings.isNullOrEmpty(inputCols)) {
				throw new IllegalArgumentException(
						String.format("[%s] The fields must not be empty.", this.getClass().getName()));
			}

			if (Strings.isNullOrEmpty(outputCol)) {
				throw new IllegalArgumentException(String.format(
						"[%s] The name of the field that contains the transformed features must not be empty.",
						this.getClass().getName()));
			}
			
		}

		public void validateSchema(Schema inputSchema) {
		
			String[] columns = getFields();
			for (String column: columns) {

				Schema.Field field = inputSchema.getField(column);
				Schema.Type fieldType = field.getSchema().getType();
				
				/* Check whether the provided field is a numeric field */
				if (!SchemaUtil.isNumericType(fieldType)) {
					
					/* Check whether the field is an Array */
					if (!fieldType.equals(Schema.Type.ARRAY)) {
						throw new IllegalArgumentException(
								String.format("[%s] The field that defines the input must be either NUMERIC or an ARRAY.", this.getClass().getName()));
					}

					Schema.Type fieldCompType = field.getSchema().getComponentSchema().getType();
					if (!SchemaUtil.isNumericType(fieldCompType)) {
						throw new IllegalArgumentException(
								String.format("[%s] The data type of the input field components must be NUMERIC.", this.getClass().getName()));
					}
					
				}
				
			}
			
		}		
		
	}
}
