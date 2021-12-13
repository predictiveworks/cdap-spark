package de.kp.works.core.recording.recommendation;
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

import de.kp.works.core.recording.AbstractRecorder;
import de.kp.works.core.recording.SparkMLManager;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;

public class RecommenderRecorder extends AbstractRecorder {

	protected String algoType = SparkMLManager.RECOMMENDER;

	public String getModelPath(SparkExecutionPluginContext context, String algoName, String modelName, String modelStage, String modelOption) throws Exception {
		return getPath(context, algoType, algoName, modelName, modelStage, modelOption);
	}

}
