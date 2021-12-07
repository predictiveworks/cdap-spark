package de.kp.works.core;
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
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.google.common.base.Strings;
import com.google.gson.Gson;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.plugin.common.Constants;

public class BaseConfig extends PluginConfig {

	private static final long serialVersionUID = 1290979476413023858L;

	protected static final String BEST_MODEL = "best";
	protected static final String LATEST_MODEL = "latest";

	@Name(Constants.Reference.REFERENCE_NAME)
	@Description(Constants.Reference.REFERENCE_NAME_DESCRIPTION)
	public String referenceName;
    
	public Map<String, Object> getParamsAsMap() {
		return new HashMap<>();
	}
	
	public String getParamsAsJSON() {

		Gson gson = new Gson();			
		return gson.toJson(getParamsAsMap());
		
	}
	
	public double[] getDataSplits(String dataSplit) {
		
		String[] tokens = dataSplit.split(":");
		
		Double x = Double.parseDouble(tokens[0]) / 100D;
		Double y = Double.parseDouble(tokens[1]) / 100D;
		
		List<Double> splits = new ArrayList<>();
		splits.add(x);
		splits.add(y);

		Double[] array = splits.toArray(new Double[0]);
		return Stream.of(array).mapToDouble(Double::doubleValue).toArray();

	}
	
	public Boolean toBoolean(String value) {
		return value.equals("true");
	}

	public void validate() {
		
		if (Strings.isNullOrEmpty(referenceName)) {
			throw new IllegalArgumentException(
					String.format("[%s] The reference name must not be empty.", this.getClass().getName()));
		}

	}

	public static Schema getNonNullIfNullable(Schema schema) {
		return schema.isNullable() ? schema.getNonNullable() : schema;
	}

}
