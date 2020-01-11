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

import javax.annotation.Nullable;

import com.google.common.base.Strings;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.hydrator.common.Constants;

public class BaseFeatureConfig extends PluginConfig {

	private static final long serialVersionUID = -1568798398931701098L;

	@Name(Constants.Reference.REFERENCE_NAME)
	@Description(Constants.Reference.REFERENCE_NAME_DESCRIPTION)
	public String referenceName;

	@Description("The unique name of the feature model, if this feature transformation requires a trained feature model. "
			+ "Examples are Count Vectorizer models, Word2Vec models etc.")
	@Macro
	@Nullable
	public String modelName;

	@Description("The name of the field in the input schema that contains the features.")
	@Macro
	public String inputCol;

	@Description("The name of the field in the output schema that contains the transformed features.")
	@Macro
	public String outputCol;

	public void validate() {

		/** COLUMNS **/
		if (!Strings.isNullOrEmpty(inputCol)) {
			throw new IllegalArgumentException(
					String.format("[%s] The name of the field that contains the features must not be empty.",
							this.getClass().getName()));
		}
		if (!Strings.isNullOrEmpty(outputCol)) {
			throw new IllegalArgumentException(String.format(
					"[%s] The name of the field that contains the transformed features must not be empty.",
					this.getClass().getName()));
		}

	}

}