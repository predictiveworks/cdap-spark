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
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.google.common.base.Strings;
import com.google.gson.Gson;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;

import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.hydrator.common.Constants;

public class BaseRegressorConfig extends PluginConfig {

	private static final long serialVersionUID = -840366122609747351L;

	@Name(Constants.Reference.REFERENCE_NAME)
	@Description(Constants.Reference.REFERENCE_NAME_DESCRIPTION)
	public String referenceName;

	@Description("The unique name of the regressor model.")
	@Macro
	public String modelName;

	@Description("The name of the field in the input schema that contains the feature vector.")
	@Macro
	public String featuresCol;

	@Description("The name of the field in the input schema that contains the label.")
	@Macro
	public String labelCol;

	@Description("The split of the dataset into train & test data, e.g. 80:20. Default is 70:30")
	@Macro
	public String dataSplit;
    
	public Map<String, Object> getParamsAsMap() {
		return null;
	}
	
	public String getParamsAsJSON() {

		Gson gson = new Gson();			
		return gson.toJson(getParamsAsMap());
		
	}
	
	public void validate() {
		
		if (!Strings.isNullOrEmpty(referenceName)) {
			throw new IllegalArgumentException(
					String.format("[%s] The reference name must not be empty.", this.getClass().getName()));
		}

		/** MODEL & COLUMNS **/
		if (!Strings.isNullOrEmpty(modelName)) {
			throw new IllegalArgumentException(
					String.format("[%s] The model name must not be empty.", this.getClass().getName()));
		}
		if (!Strings.isNullOrEmpty(featuresCol)) {
			throw new IllegalArgumentException(
					String.format("[%s] The name of the field that contains the feature vector must not be empty.",
							this.getClass().getName()));
		}
		if (!Strings.isNullOrEmpty(labelCol)) {
			throw new IllegalArgumentException(
					String.format("[%s] The name of the field that contains the label value must not be empty.",
							this.getClass().getName()));
		}
	}
	
	public double[] getSplits() {
		
		String[] tokens = dataSplit.split(":");
		
		Double x = Double.parseDouble(tokens[0]) / 100D;
		Double y = Double.parseDouble(tokens[1]) / 100D;
		
		List<Double> splits = new ArrayList<>();
		splits.add(x);
		splits.add(y);

		Double[] array = splits.toArray(new Double[splits.size()]);
		return Stream.of(array).mapToDouble(Double::doubleValue).toArray();

	}

}
