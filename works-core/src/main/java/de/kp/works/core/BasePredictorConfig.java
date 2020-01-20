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

import com.google.common.base.Strings;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.hydrator.common.Constants;

public class BasePredictorConfig extends PluginConfig {

	private static final long serialVersionUID = 7887257708238649738L;

	@Name(Constants.Reference.REFERENCE_NAME)
	@Description(Constants.Reference.REFERENCE_NAME_DESCRIPTION)
	public String referenceName;

	@Description("The unique name of the classifier or regressor model that is used for prediction.")
	@Macro
	public String modelName;

	@Description("The type of the model that is used for prediction, either 'classifier' or 'regressor'.")
	@Macro
	public String modelType;

	@Description("The name of the field in the input schema that contains the feature vector.")
	@Macro
	public String featuresCol;

	@Description("The name of the field in the output schema that contains the predicted label.")
	@Macro
	public String predictionCol;
	
	public void validate() {
		
		if (Strings.isNullOrEmpty(referenceName)) {
			throw new IllegalArgumentException(
					String.format("[%s] The reference name must not be empty.", this.getClass().getName()));
		}

		/** MODEL & COLUMNS **/
		if (Strings.isNullOrEmpty(modelName)) {
			throw new IllegalArgumentException(
					String.format("[%s] The model name must not be empty.", this.getClass().getName()));
		}
		if (Strings.isNullOrEmpty(featuresCol)) {
			throw new IllegalArgumentException(
					String.format("[%s] The name of the field that contains the feature vector must not be empty.",
							this.getClass().getName()));
		}
		if (Strings.isNullOrEmpty(predictionCol)) {
			throw new IllegalArgumentException(String.format(
					"[%s] The name of the field that contains the predicted label value must not be empty.",
					this.getClass().getName()));
		}

	}

}
