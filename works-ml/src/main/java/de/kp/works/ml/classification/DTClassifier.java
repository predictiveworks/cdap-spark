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
@Name("DTClassifer")
@Description("A building stage for an Apache Spark based Decision Tree classifier model.")
public class DTClassifier extends BaseClassifierSink {

	private static final long serialVersionUID = -4324297354460233205L;

	private DTClassifierConfig config;
	
	public DTClassifier(DTClassifierConfig config) {
		this.config = config;
	}

	public static class DTClassifierConfig extends PluginConfig {

		private static final long serialVersionUID = -5216062714694933745L;
		  
		@Name(Constants.Reference.REFERENCE_NAME)
		@Description(Constants.Reference.REFERENCE_NAME_DESCRIPTION)
		public String referenceName;
		
		public void validate() {
			
		}
		
	}
}
