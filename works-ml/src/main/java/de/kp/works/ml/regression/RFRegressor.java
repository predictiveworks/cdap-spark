package de.kp.works.ml.regression;

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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.hydrator.common.Constants;
import de.kp.works.core.BaseRegressorSink;

@Plugin(type = "sparksink")
@Name("RFRegressor")
@Description("A building stage for an Apache Spark based Random Forest Trees regressor model.")
public class RFRegressor extends BaseRegressorSink {

	private static final long serialVersionUID = 1969655374719118217L;
	
	private RFRegressorConfig config;
	
	public RFRegressor(RFRegressorConfig config) {
		this.config = config;
	}

	public static class RFRegressorConfig extends PluginConfig {

		private static final long serialVersionUID = 7150058657685557952L;

		@Name(Constants.Reference.REFERENCE_NAME)
		@Description(Constants.Reference.REFERENCE_NAME_DESCRIPTION)
		public String referenceName;
		
	    @Description("The unique name of the Random Forest regressor model.")
	    @Macro
	    private String modelName;
		
		public void validate() {
			
		}
		
	}
}
