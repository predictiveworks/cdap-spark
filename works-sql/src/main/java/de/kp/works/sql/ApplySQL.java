package de.kp.works.sql;

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

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.StructType;
import org.spark_project.guava.base.Strings;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.spark.sql.DataFrames;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.plugin.common.Constants;
import de.kp.works.core.SessionHelper;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("ApplySQL")
@Description("A transformation stage that uses an Apache Spark SQL based data exploration engine "
		+ "to aggregate, group and filter pipeline data records.")
public class ApplySQL extends SparkCompute<StructuredRecord, StructuredRecord> {
	/**
	 * The [ApplySQL] plugin is a general purpose plugin which applies
	 * a user defined SQL statement (that is Apache Spark compliant)
	 * to the incoming data records
	 *
	 */	
	private static final long serialVersionUID = 4939234688659944895L;

	private ApplySQLConfig config;
	/*
	 * This is an internal flag that indicates whether the
	 * provided input schema is suitable for SQL evaluation
	 */
	private Boolean isValidSchema = false;
	/*
	 * This is an internal flag that indicates whether the
	 * provided SQL statement is valid and can be used with
	 * the input dataset
	 */
	private Boolean isValidSQL = false;
	/*
	 * The name of the table provided with the SQL statement;
	 * this argument is for internal use only in order to apply
	 * the SQL statement to the input dataset
	 */
	private String tableName;
	
	public ApplySQL(ApplySQLConfig config) {
		this.config = config;
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
		super.configurePipeline(pipelineConfigurer);
		/*
		 * Check whether the provided SQL statement is empty
		 */
		config.validate();
	}

	@Override
	public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext context, JavaRDD<StructuredRecord> input)
			throws Exception {
		/*
		 * This is an initial step, that checks whether the input
		 * is empty, and, if this is the case returns the input
		 */
		if (input.isEmpty()) {
			return input;
		}
		/*
		 * Next, we need a schema to compute the incoming dataset;
		 * the schema can either be explicitly provided or is derived
		 * from the first incoming data record
		 */
		Schema inputSchema = context.getInputSchema();

		if (inputSchema == null) {
			inputSchema = input.first().getSchema();
		}
		/*
		 * In order to apply a SQL statement to the incoming data records
		 * we have to make sure that the associated schema does not contain
		 * complex data types
		 */
		if (isValidSchema == false) {

			config.validate(inputSchema);
			isValidSchema = true;

		}
		/*
		 * We leverage CDAP's [DataFrames] to transform the extracted
		 * schema into an Apache Spark SQL [StructType]; this data type
		 * is needed to finally transform the incoming RDD into a Spark
		 * SQL compliant dataset of [Row]
		 */
		StructType structType = (StructType)DataFrames.toDataType(inputSchema);

		JavaSparkContext jsc = context.getSparkContext();	    
		SparkSession session = new SparkSession(jsc.sc());		
		/*
		 * Before we finally proceed and apply the provided SQL statement
		 * to the incoming dataset, we check whether the statement is valid
		 */
		if (isValidSQL == false) {
			
			tableName = config.validateAndTable(session, config.sql);
			isValidSQL = true;
			
		}
		
		Dataset<Row> rows = SessionHelper.toDataset(input, structType, session);
		/*
		 * Apply SQL statement and re-transform the derived dataset
		 * into an output RDD
		 */
		Dataset<Row> computed = applySQL(rows);
		Schema outputSchema = DataFrames.toSchema(computed.schema());
		
		JavaRDD<StructuredRecord> records = SessionHelper.fromDataset(computed, outputSchema);
		return records;

	}
	
	private Dataset<Row> applySQL(Dataset<Row> rows) {
		
		if (tableName == null)
			throw new IllegalArgumentException("[ApplySQL] Applying provided SQL statement failed, as no valid table name was found. "
					+ "Please check your SQL statement and try again.");

		/*
		 * Register dataset as temporary table
		 */
		rows.createOrReplaceTempView(tableName);
		/*
		 * Apply SQL statement ot temporary table
		 */
		SparkSession session = rows.sparkSession();
		return session.sql(config.sql);

	}

	/**
	 * Config for the ApplySQL SparkCompute.
	 */
	public static class ApplySQLConfig extends PluginConfig {

		private static final long serialVersionUID = -4037375709289068859L;
		
		private static final String SQL_DESCRIPTION = "Please provide an Apache Spark compliant SQL statement to filter, " 
				+ "group or aggregate the data records that have been published by your previous pipeline stage.";
		  
		@Name(Constants.Reference.REFERENCE_NAME)
		@Description(Constants.Reference.REFERENCE_NAME_DESCRIPTION)
		public String referenceName;

		@Name("sql")
		@Description(SQL_DESCRIPTION)
		@Macro
		private String sql;

		public ApplySQLConfig(String sql) {
			this.sql = sql;
		}
		/**
		 * Returns the non-nullable part of the given {@link Schema} if it is nullable;
		 * otherwise return it as is.
		 */
		private Schema getNonNullIfNullable(Schema schema) {
			return schema.isNullable() ? schema.getNonNullable() : schema;
		}
		
		public void validate() {

			if (!containsMacro("sql") && Strings.isNullOrEmpty(sql)) {
				throw new IllegalArgumentException("[ApplySQL] Your configuration contains an empty SQL statement. "
						+ "We do not know how to proceed. Please check and try again.");
			}

		}

		public void validate(Schema schema) {
			/*
			 * The schema must specify a record with fields that are defined by basic data
			 * types only
			 */
			if (schema.getType().equals(Schema.Type.RECORD) == false)
				throw new IllegalArgumentException(
						"[ApplySQL] The data format of the provided data records is not appropriate "
								+ "to be computed by this data machine. Please check your blueprint configuration and re-build this application.");

			/*
			 * Extract the fields from the provided record schema and check whether the
			 * specified data type are basic types
			 */
			List<Schema.Field> fields = schema.getFields();
			for (Schema.Field field : fields) {

				String fieldName = field.getName();
				/*
				 * IMPORTANT: Nullable fields are described by a CDAP field schema that is a
				 * UNION of a the nullable type and the non-nullable type;
				 * 
				 * to evaulate the respective schema, we MUST RESTRICT to the non- nullable type
				 * only; otherwise we also view UNION schemas
				 */
				Schema fieldSchema = getNonNullIfNullable(field.getSchema());
				switch (fieldSchema.getType()) {
				case NULL:
				case BOOLEAN:
				case BYTES:
				case DOUBLE:
				case ENUM: /* Transform onto [String] */
				case INT:
				case LONG:
				case FLOAT:
				case STRING:
					break;
				case ARRAY:
				case MAP:
				case RECORD:
				case UNION:
					throw new IllegalArgumentException(String.format(
							"[ApplySQL] The data format contains a field '%s' with a complex data type '%s'. "
									+ "This use case is not supported by this data machine. Please check your blueprint configuration and re-build this application.",
							fieldName, fieldSchema.getType().name()));
				}

			}

		}
		/*
		 * A private helper method to validate the provided SQL
		 * statement derive the specified table name for later
		 * usage
		 */
		private String validateAndTable(SparkSession session, String sql) {
			
			String tableName = null;
			/* 
			 * Check whether the SQL statement is compliant to Apache
			 * Spark's SQL syntax
			 */
			try {

				LogicalPlan plan = session.sessionState().sqlParser().parsePlan(sql);
				if (plan == null) {
					throw new Exception("Could not build a valid computation plan from SQL statement.");
				}
				
				List<String> tables = tableNames(plan);
				if (tables.size() != 1) 
					throw new Exception(String.format("Please configure a SQL statement with a single table name. Found: %s", tables.size()));

				tableName = tables.get(0);
				return tableName;

			} catch (Exception e) {
				throw new IllegalArgumentException(
						String.format("[ApplySQL] The provided SQL statement '%s' cannot be resolved. Please check your configuration and try again. "
								+ "Validation failed with: %s", sql, e.getLocalizedMessage()));

			}

		}
		
		/**
		 * This method is based on the logical plan created from the 
		 * provided SQL statement and extracts all tables names that
		 * refer to sub plan as <UnresolvedRelation>
		 * 
		 * @return
		 * @throws Exception
		 */
		private List<String> tableNames(LogicalPlan plan) throws Exception {

			List<String> tables = new ArrayList<String>();

			List<LogicalPlan> subplans = scala.collection.JavaConverters
					.seqAsJavaListConverter(plan.collectLeaves()).asJava();
			for (LogicalPlan subplan : subplans) {

				if (subplan instanceof UnresolvedRelation) {

					UnresolvedRelation relation = (UnresolvedRelation) subplan;
					String tableName = relation.tableName();

					tables.add(tableName);

				}

			}

			return tables;

		}
		
		
	}
}
