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

public class BaseTimeConfig extends PluginConfig {

	private static final long serialVersionUID = 3721883166765717447L;

	@Name(Constants.Reference.REFERENCE_NAME)
	@Description(Constants.Reference.REFERENCE_NAME_DESCRIPTION)
	public String referenceName;

	@Description("The name of the field in the input schema that contains the time value.")
	@Macro
	public String timeCol;

	@Description("The name of the field in the input schema that contains the value.")
	@Macro
	public String valueCol;

	public void validate() {
		
		if (!Strings.isNullOrEmpty(referenceName)) {
			throw new IllegalArgumentException(
					String.format("[%s] The reference name must not be empty.", this.getClass().getName()));
		}

		if (!Strings.isNullOrEmpty(timeCol)) {
			throw new IllegalArgumentException(
					String.format("[%s] The name of the field that contains the time value must not be empty.",
							this.getClass().getName()));
		}
		if (!Strings.isNullOrEmpty(valueCol)) {
			throw new IllegalArgumentException(
					String.format("[%s] The name of the field that contains the value must not be empty.",
							this.getClass().getName()));
		}

	}
}
