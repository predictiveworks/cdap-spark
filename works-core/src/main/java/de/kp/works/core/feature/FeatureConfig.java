package de.kp.works.core.feature;
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.data.schema.Schema;
import de.kp.works.core.BaseConfig;

public class FeatureConfig extends BaseConfig {

	private static final long serialVersionUID = -1568798398931701098L;
	/*
	 * Two different feature stages exist: one stage type is model based,
	 * and the other is not. These stage types are managed with the same
	 * configuration but nullable fields 
	 */
	@Description("The unique name of the feature model, if this feature transformation requires a trained feature model. "
			+ "Examples are Count Vectorizer models, Word2Vec models etc.")
	@Macro
	@Nullable
	public String modelName;

	@Description("The stage of the ML model. Supported values are 'experiment', 'stagging', 'production' and 'archived'. Default is 'experiment'.")
	@Macro
	@Nullable
	public String modelStage;

	@Description("The name of the field in the input schema that contains the features.")
	@Macro
	public String inputCol;

	@Description("The name of the field in the output schema that contains the transformed features.")
	@Macro
	public String outputCol;

	public FeatureConfig() {
		modelStage = "experiment";
	}

	public void validate() {
		super.validate();

		/* COLUMNS */
		if (Strings.isNullOrEmpty(inputCol)) {
			throw new IllegalArgumentException(
					String.format("[%s] The name of the field that contains the features must not be empty.",
							this.getClass().getName()));
		}
		if (Strings.isNullOrEmpty(outputCol)) {
			throw new IllegalArgumentException(String.format(
					"[%s] The name of the field that contains the transformed features must not be empty.",
					this.getClass().getName()));
		}

	}

	public void validateSchema(Schema inputSchema) {

		/* INPUT COLUMN */

		Schema.Field inputField = inputSchema.getField(inputCol);
		if (inputField == null) {
			throw new IllegalArgumentException(String.format(
					"[%s] The input schema must contain the field that defines the features.", this.getClass().getName()));
		}
	}

}

