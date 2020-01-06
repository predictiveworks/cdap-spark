package de.kp.works.ml.regression;

/*
 * Copyright (c) 2019 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * This software is the confidential and proprietary information of 
 * Dr. Krusche & Partner PartG ("Confidential Information"). 
 * 
 * You shall not disclose such Confidential Information and shall use 
 * it only in accordance with the terms of the license agreement you 
 * entered into with Dr. Krusche & Partner PartG.
 * 
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 * 
 */

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.hydrator.common.Constants;
import de.kp.works.core.BaseRegressorSink;

@Plugin(type = "sparksink")
@Name("IsotonicRegressor")
@Description("A building stage for an Apache Spark based Isotonic regressor model.")
public class IsotonicRegressor extends BaseRegressorSink {

	private static final long serialVersionUID = 185956615279200366L;

	private IsotonicConfig config;
	
	public IsotonicRegressor(IsotonicConfig config) {
		this.config = config;
	}

	public static class IsotonicConfig extends PluginConfig {
		  
		private static final long serialVersionUID = -4928234679795163044L;

		@Name(Constants.Reference.REFERENCE_NAME)
		@Description(Constants.Reference.REFERENCE_NAME_DESCRIPTION)
		public String referenceName;
		
		public void validate() {
			
		}
				
	}

}
