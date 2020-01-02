package de.kp.works.rules;

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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import org.drools.core.impl.InternalKnowledgeBase;
import org.drools.core.impl.KnowledgeBaseFactory;
import org.drools.core.io.impl.ByteArrayResource;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.KieSession;
import org.kie.internal.builder.KnowledgeBuilder;
import org.kie.internal.builder.KnowledgeBuilderFactory;
import org.spark_project.guava.base.Joiner;
import org.spark_project.guava.base.Strings;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.hydrator.common.Constants;

/**
 * We have to support two different use cases, i.e. filtering and scoring;
 * filtering returns the data records that match the rule filter conditions,
 * while scoring adds an additional field to the result, i.e. the score
 *
 */
@Plugin(type = "sparkcompute")
@Name("ApplyRules")
@Description("A Drools compliant business rule engine that applies rules to a batch data records.")
public class ApplyRules extends SparkCompute<StructuredRecord, StructuredRecord> {

	private static final long serialVersionUID = 7585185760627607283L;

	private ApplyRulesConfig config;
	private Broadcast<InternalKnowledgeBase> kbase;
	/*
	 * This is an internal flag that indicates whether the
	 * provided input schema is suitable for ruleset evaluation
	 */
	private Boolean isValidSchema = false;

	public ApplyRules(ApplyRulesConfig config) {
		this.config = config;
	}

	/*
	 * This method is used to build the Drools internal knowledge base from the
	 * user-defined rules (DRL format)
	 */
	@Override
	public void initialize(SparkExecutionPluginContext context) throws Exception {
		super.initialize(context);

		/* Initialize Drools rule engine */
		InternalKnowledgeBase base = config.loadKBase();

		/* Broadcast to all Apache Spark workers */
		JavaSparkContext jsc = context.getSparkContext();
		kbase = jsc.broadcast(base);

	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
		super.configurePipeline(pipelineConfigurer);
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

		return input.mapPartitions(new DroolsFunction(kbase, config.getRulesType()));

	}

	private static class DroolsFunction implements FlatMapFunction<Iterator<StructuredRecord>, StructuredRecord> {

		private static final long serialVersionUID = 5022685393133974680L;

		private transient Broadcast<InternalKnowledgeBase> kbase;
		private transient String rulesType;

		private transient Schema inputSchema;
		private transient Schema outputSchema;

		DroolsFunction(Broadcast<InternalKnowledgeBase> kbase, String rulesType) {
			this.kbase = kbase;
			this.rulesType = rulesType;
		}

		@Override
		public Iterator<StructuredRecord> call(Iterator<StructuredRecord> partition) throws Exception {

			try {
				/* Create Drools session from broadcasted knowledge base */
				KieSession session = kbase.getValue().newKieSession();

				/* Define results */
				List<DroolsResult> droolsResults = new ArrayList<>();
				session.setGlobal("droolsResults", droolsResults);

				/*
				 * Transform [StructuredRecord] into map and [DroolsFact] and insert each fact
				 * into the current session
				 */
				while (partition.hasNext()) {

					StructuredRecord record = partition.next();
					if (inputSchema == null)
						inputSchema = record.getSchema();

					session.insert(recordToFact(record));

				}

				/* Apply rules to provided fact base */
				session.fireAllRules();

				if (droolsResults.isEmpty()) {
					/*
					 * No data record of the provided partition matches one of the user defined
					 * rules. In this case an empty iterator is returned
					 */
					List<StructuredRecord> output = new ArrayList<>();
					return output.iterator();

				} else {
					/*
					 * Transform the Drools results into an interator of [StructuredRecord]
					 */
					return resultsToRecords(droolsResults, rulesType);

				}

			} catch (Exception e) {
				throw new RuntimeException(String.format(
						"[ApplyRules] Applying the provided business rules to the "
								+ "data records that have been published by your previous data stage failed: %s",
						e.getLocalizedMessage()));
			}

		}

		private Iterator<StructuredRecord> resultsToRecords(List<DroolsResult> results, String rulesType) {

			List<StructuredRecord> records = new ArrayList<>();
			if (outputSchema == null) {
				
				List<Schema.Field> inputFields = inputSchema.getFields();
				if (rulesType.equals("score")) {

					String fieldName = results.get(0).field;
					Schema.Field scoreField = Schema.Field.of(fieldName, Schema.of(Schema.Type.DOUBLE));

					inputFields.add(scoreField);
				}
				
				outputSchema = Schema.recordOf("rulesSchema", inputFields);

			}
			
			for (DroolsResult result : results) {
				/*
				 * The output schema is defined and the record builder can be used to assign
				 * values to the record
				 */
				StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
				for (Schema.Field field : outputSchema.getFields()) {

					String fieldName = field.getName();
					builder.set(fieldName, result.fact.get(fieldName));

				}
				
				if (rulesType.equals("score")) {
					builder.set(result.field, result.value);
				}

				records.add(builder.build());

			}

			return records.iterator();

		}

		private DroolsFact recordToFact(StructuredRecord record) {

			Map<String, Object> fact = new HashMap<>();
			List<Schema.Field> fields = record.getSchema().getFields();

			for (Schema.Field field : fields) {

				String fieldName = field.getName();
				Object fieldValu = (Object) record.get(fieldName);

				fact.put(fieldName, fieldValu);
			}

			return new DroolsFact(fact);

		}
	}

	/**
	 * Config for the ApplyRules SparkCompute.
	 */
	public static class ApplyRulesConfig extends PluginConfig {

		private static final long serialVersionUID = -367324950348889319L;

		private static final String RULES_DESCRIPTION = "Please provide Drools compliant specification of business rules "
				+ "to filter or score the data records that have been published by your previous data stage.";

		private static final String RULES_TYPE_DESCRIPTION = "This component supports business rules to either filter "
				+ "or score data records. The default rule type is 'filter'. If you select 'score' as an alternative use "
				+ "case, this component and extra field to the output records.";
		  
		@Name(Constants.Reference.REFERENCE_NAME)
		@Description(Constants.Reference.REFERENCE_NAME_DESCRIPTION)
		public String referenceName;

		@Name("rules")
		@Description(RULES_DESCRIPTION)
		@Macro
		private String rules;

		@Name("rulesType")
		@Description(RULES_TYPE_DESCRIPTION)
		@Macro
		private String rulesType;

		public ApplyRulesConfig(String rules) {

			this.rules = rules;
			this.rulesType = "filter";

		}
		/*
		 * The rule specification starts with rule ""
		 */
		public String getRules() {

			List<String> header = new ArrayList<String>() {
				private static final long serialVersionUID = 4772299596003284932L;

				{
					add("package de.kp.predictive.rules;");
					add(" ");
					add("global java.util.ArrayList droolsResults;");
					add(" ");
				}
			};

			String prefix = Joiner.on("\n").join(header);
			return prefix + rules;

		}

		public String getRulesType() {
			return rulesType;
		}

		public void validate() {

			if (!containsMacro("rules") && Strings.isNullOrEmpty(rules)) {
				throw new IllegalArgumentException("[ApplyRules] Your configuration contains an empty ruleset. "
						+ "We do not know how to proceed. Please check and try again.");
			}

		}
		/**
		 * Returns the non-nullable part of the given {@link Schema} if it is nullable;
		 * otherwise return it as is.
		 */
		private Schema getNonNullIfNullable(Schema schema) {
			return schema.isNullable() ? schema.getNonNullable() : schema;
		}

		public void validate(Schema schema) {
			/*
			 * The schema must specify a record with fields that are defined by basic data
			 * types only
			 */
			if (schema.getType().equals(Schema.Type.RECORD) == false)
				throw new IllegalArgumentException(
						"[ApplyRukes] The data format of the provided data records is not appropriate "
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
							"[ApplyRules] The data format contains a field '%s' with a complex data type '%s'. "
									+ "This use case is not supported by this data machine. Please check your blueprint configuration and re-build this application.",
							fieldName, fieldSchema.getType().name()));
				}

			}

		}

		private InternalKnowledgeBase loadKBase() throws Exception {

			KnowledgeBuilder kbuilder = KnowledgeBuilderFactory.newKnowledgeBuilder();
			kbuilder.add(new ByteArrayResource(rules.getBytes()), ResourceType.DRL);

			InternalKnowledgeBase kbase = KnowledgeBaseFactory.newKnowledgeBase();
			kbase.addPackages(kbuilder.getKnowledgePackages());

			return kbase;

		}

	}

}
