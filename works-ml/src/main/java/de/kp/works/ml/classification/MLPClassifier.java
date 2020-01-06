package de.kp.works.ml.classification;

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
import de.kp.works.core.BaseClassifierSink;

@Plugin(type = "sparksink")
@Name("MLPClassifer")
@Description("A building stage for an Apache Spark based Multilayer Perceptron classifier model.")
public class MLPClassifier extends BaseClassifierSink {

	private static final long serialVersionUID = -7445401286046769822L;
	
	private MLPClassifierConfig config;
	
	public MLPClassifier(MLPClassifierConfig config) {
		this.config = config;
	}

	public static class MLPClassifierConfig extends PluginConfig {

		private static final long serialVersionUID = 7824534239299625292L;
		  
		@Name(Constants.Reference.REFERENCE_NAME)
		@Description(Constants.Reference.REFERENCE_NAME_DESCRIPTION)
		public String referenceName;
		
		public void validate() {
			
		}
		
	}
}
