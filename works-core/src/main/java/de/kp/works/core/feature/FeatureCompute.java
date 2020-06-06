package de.kp.works.core.feature;
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

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import io.cdap.cdap.api.data.schema.Schema;
import de.kp.works.core.BaseCompute;
import de.kp.works.core.model.ModelProfile;

public class FeatureCompute extends BaseCompute {

	private static final long serialVersionUID = -852876404206487204L;

	protected Type annotationType = new TypeToken<List<Map<String, Object>>>() {
	}.getType();
	/*
	 * The annotation type supported by this feature stage
	 */
	protected static final String FEATURE_TYPE = "feature";

	/*
	 * Retrieving the prediction model that matches the user-defined model options
	 * (either best or latest) also determines the model profile; this profile is
	 * used to assign the unique model identifier to each prediction result
	 */
	protected ModelProfile profile;

	/**
	 * A helper method to compute the output schema in that use cases 
	 * where an input schema is explicitly given; the input schema is
	 * enriched by a Double field and an internal annotation field
	 */
	public Schema getOutputSchema(Schema inputSchema, String outputField, Schema.Type fieldType) {

		List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());		
		fields.add(Schema.Field.of(outputField, Schema.of(fieldType)));
		/* 
		 * Check whether the input schema already has an 
		 * annotation field defined; the feature stage
		 * may not be the first stage that annotates model
		 * specific metadata 
		 */
		if (inputSchema.getField(ANNOTATION_COL) == null)
			fields.add(Schema.Field.of(ANNOTATION_COL, Schema.of(Schema.Type.STRING)));
		
		return Schema.recordOf(inputSchema.getRecordName() + ".transformed", fields);

	}	
	
	/**
	 * A helper method to compute the output schema in that use cases 
	 * where an input schema is explicitly provided; the input schema
	 * is enriched by an Array[Double] field and an internal annotation
	 * field
	 */
	protected Schema getArrayOutputSchema(Schema inputSchema, String outputField, Schema.Type fieldType) {

		List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());		
		fields.add(Schema.Field.of(outputField, Schema.arrayOf(Schema.of(fieldType))));		
		/* 
		 * Check whether the input schema already has an 
		 * annotation field defined; the feature stage
		 * may not be the first stage that annotates model
		 * specific metadata 
		 */
		if (inputSchema.getField(ANNOTATION_COL) == null)
			fields.add(Schema.Field.of(ANNOTATION_COL, Schema.of(Schema.Type.STRING)));
		
		return Schema.recordOf(inputSchema.getRecordName() + ".transformed", fields);

	}	
	
	/**
	 * A helper method to enrich a feature result
	 * with model profile metadata
	 */
	protected Dataset<Row> annotate(Dataset<Row> dataset, String annonType) {
		
		List<Map<String, Object>> annonItems = new ArrayList<>();
		/*
		 * STEP #1: The annotation column is an internal column
		 * with a pre-defined internal column name. A feature
		 * stage may not be the first (or last) stage to enrich
		 * a certain result with model specific information
		 */
		if (hasColumn(dataset, ANNOTATION_COL)) {
			/*
			 * The current implementation enriches each row
			 * with the same annotation; we therefore extract
			 * them from the fist column
			 */
			Integer index = dataset.schema().fieldIndex(ANNOTATION_COL);

			String annotation = dataset.first().getString(index);			
			annonItems.addAll(annonToList(annotation));
			
		}
		/*
		 * STEP #2: Compute annotation for the current model
		 * and add to the list of existing annotations
		 */
		annonItems.add(annotateProfile(annonType));
		/*
		 * STEP #3: Serialize annotation and enrich each row
		 * with specified annotations
		 */
		String annotation = new Gson().toJson(annonItems);		
		return dataset.withColumn(ANNOTATION_COL, functions.lit(annotation));

	}
	
	protected Boolean hasColumn(Dataset<Row> dataset, String column) {

		try {
			List<String> columns = Arrays.asList(dataset.columns());
			return columns.contains(column);
			
		} catch(Exception e) {
			return false;
		}

	}
	
	protected List<Map<String,Object>> annonToList(String annotation) {
		return new Gson().fromJson(annotation, annotationType);
	}
	
	/*
	 * The current implementation is restricted to
	 * annotate the unique model identifier and the
	 * model trustability
	 */
	protected Map<String,Object> annotateProfile(String annonType) {

		Map<String, Object> model = new HashMap<>();
		/*
		 * Model properties that are currently used
		 */
		model.put("id", profile.id);
		model.put("trust", profile.trustability);
		/*
		 * Model type identifies one of the registry
		 * tables that are managed by PredictiveWorks
		 */
		model.put("type", annonType);

		Map<String, Object> annotation = new HashMap<>();
		annotation.put("model", model);

	    return annotation;

	}
	
}
