package de.kp.works.ts.ar;
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

import com.google.common.base.Strings;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import de.kp.works.core.time.TimeConfig;

public class ARConfig extends TimeConfig {

	private static final long serialVersionUID = -2081104520718533381L;
	/*
	 * TimeConfig contributes common 'timeCol' and 'valueCol'
	 * columns to specify the data points
	 */
	@Description("The unique name of the AR model.")
	@Macro
	public String modelName;

	@Description("The stage of the ML model. Supported values are 'experiment', 'staging', 'production' and 'archived'. Default is 'experiment'.")
	@Macro
	public String modelStage;

	public void validate() {
		super.validate();

		if (Strings.isNullOrEmpty(modelName)) {
			throw new IllegalArgumentException(
					String.format("[%s] The model name must not be empty.", this.getClass().getName()));
		}

	}

}
