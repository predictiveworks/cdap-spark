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
@Name("OVRClassifer")
@Description("A building stage for an Apache Spark based OneVsRest classifier model.")
public class OVRClassifier extends BaseClassifierSink {

	private static final long serialVersionUID = 3428241323322243511L;
	
	private OVRClassifierConfig config;
	
	public OVRClassifier(OVRClassifierConfig config) {
		this.config = config;
	}

	public static class OVRClassifierConfig extends PluginConfig {

		private static final long serialVersionUID = 1504291913928740352L;
		  
		@Name(Constants.Reference.REFERENCE_NAME)
		@Description(Constants.Reference.REFERENCE_NAME_DESCRIPTION)
		public String referenceName;
		
		public void validate() {
			
		}
		
	}
}
