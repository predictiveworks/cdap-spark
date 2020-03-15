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

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import com.google.gson.Gson;

import co.cask.cdap.api.data.schema.Schema;
import de.kp.works.core.BaseCompute;
import de.kp.works.core.model.ModelProfile;

public class TimeCompute extends BaseCompute {

	private static final long serialVersionUID = -1433147745214918467L;
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
	protected Dataset<Row> annotate(Dataset<Row> predictions) {
		
		String annotation = profileToGson();
		return predictions.withColumn(ANNOTATION_COL, functions.lit(annotation));

	}
	/*
	 * The current implementation is restricted to
	 * annotate the unique model identifier
	 */
	protected String profileToGson() {

		Map<String, Object> model = new HashMap<>();
		model.put("id", profile.id);

		Map<String, Object> annotation = new HashMap<>();
		annotation.put("model", model);

	    return new Gson().toJson(annotation);

	}

}
