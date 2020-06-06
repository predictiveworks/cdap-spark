package de.kp.works.core.cluster;
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
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.data.schema.Schema;
import de.kp.works.core.BaseConfig;
import de.kp.works.core.SchemaUtil;

public class ClusterConfig extends BaseConfig {

	private static final long serialVersionUID = -1688551189042078478L;

	@Description("The unique name of the ML model.")
	@Macro
	public String modelName;

	@Description("The stage of the ML model. Supported values are 'experiment', 'stagging', 'production' and 'archived'. Default is 'experiment'.")
	@Macro
	public String modelStage;

	@Description("The name of the field in the input schema that contains the feature vector.")
	@Macro
	public String featuresCol;
	
	public void validate() {
		super.validate();

		if (Strings.isNullOrEmpty(modelName)) {
			throw new IllegalArgumentException(
					String.format("[%s] The model name must not be empty.", this.getClass().getName()));
		}
		if (Strings.isNullOrEmpty(featuresCol)) {
			throw new IllegalArgumentException(
					String.format("[%s] The name of the field that contains the feature vector must not be empty.",
							this.getClass().getName()));
		}

	}
	
	public void validateSchema(Schema inputSchema) {

		/** FEATURES COLUMN **/

		Schema.Field featuresField = inputSchema.getField(featuresCol);
		if (featuresField == null) {
			throw new IllegalArgumentException(String.format(
					"[%s] The input schema must contain the field that defines the feature vector.", this.getClass().getName()));
		}

		SchemaUtil.isArrayOfNumeric(inputSchema, featuresCol);

	}

}
