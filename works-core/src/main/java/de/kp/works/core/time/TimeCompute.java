package de.kp.works.core.time;
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

public class TimeCompute extends BaseCompute {

	private static final long serialVersionUID = -1433147745214918467L;
	/*
	 * The annotation type supported by this predictor stage
	 */
	protected static final String REGRESSOR_TYPE = "regressor";
	protected static final String TIME_TYPE = "time";

	protected Type annotationType = new TypeToken<List<Map<String, Object>>>() {
	}.getType();
	/*
	 * Retrieving the time model that matches the user-defined model options
	 * (either best or latest) also determines the model profile; this profile 
	 * is used to assign the unique model identifier to each forecast result
	 */
	protected ModelProfile profile;
	/*
	 * The status field describes whether a certain observation
	 * is forecasted or measured
	 */
	protected static final String STATUS_FIELD = "status";

	protected Schema inputSchema;
	/**
	 * A helper method to enrich a prediction result
	 * with model profile metadata
	 */
	
	protected Dataset<Row> annotate(Dataset<Row> predictions, String annonType) {
		
		List<Map<String, Object>> annonItems = new ArrayList<>();
		/*
		 * STEP #1: The annotation column is an internal column
		 * with a pre-defined internal column name. A predictor
		 * stage may not be the first (or last) stage to enrich
		 * a certain result with model specific information
		 */
		if (hasColumn(predictions, ANNOTATION_COL)) {
			/*
			 * The current implementation enriches each row
			 * with the same annotation; we therefore extract
			 * them from the fist column
			 */
			Integer index = predictions.schema().fieldIndex(ANNOTATION_COL);

			String annotation = predictions.first().getString(index);			
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
		return predictions.withColumn(ANNOTATION_COL, functions.lit(annotation));

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
