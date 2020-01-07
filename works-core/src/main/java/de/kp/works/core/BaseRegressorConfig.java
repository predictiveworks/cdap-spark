package de.kp.works.core;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;

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

	@Description("The name of the field that contains the feature vector.")
	@Macro
	public String featuresCol;

	@Description("The name of the field that contains the label.")
	@Macro
	public String labelCol;

}
