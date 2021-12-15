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

import de.kp.works.core.configuration.ConfigReader;
import de.kp.works.core.model.ModelProfile;
import de.kp.works.core.model.ModelSpec;
import de.kp.works.core.recording.AbstractRecorder;
import de.kp.works.core.recording.SparkMLManager;
import de.kp.works.core.recording.metadata.MetadataWriter;
import de.kp.works.core.recording.metadata.CDAPWriter;
import de.kp.works.core.recording.metadata.RemoteWriter;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;

public class RecommenderRecorder extends AbstractRecorder {

	protected MetadataWriter metadataWriter;

	public RecommenderRecorder(ConfigReader configReader) {
		super(configReader);
		/*
		 * Assign the algorithm type to this recorder to
		 * support type specific functionality
		 */
		algoType = SparkMLManager.RECOMMENDER;
		/*
		 * Determine the metadata writer for this recorder;
		 * the current implementation supports remote metadata
		 * registration leveraging a Postgres instance, and also
		 * internal storage using a CDAP Table dataset.
		 */
		String metadataOption = configReader.getMetadataOption();
		switch (metadataOption) {
			case ConfigReader.REMOTE_OPTION: {
				metadataWriter = new RemoteWriter(algoType);
				break;
			}
			case ConfigReader.CDAP_OPTION: {
				metadataWriter = new CDAPWriter(algoType);
				break;
			}
			default:
				metadataWriter = null;

		}
	}

	public ModelProfile getProfile() {
		return metadataWriter.getProfile();
	}

	protected String getModelPath(SparkExecutionPluginContext context, String modelName, String modelStage, String modelOption) throws Exception {
		return metadataWriter.getModelPath(context, algoName, modelName, modelStage, modelOption);
	}

	protected String buildModelPath(SparkExecutionPluginContext context, String fsPath) throws Exception {
		return metadataWriter.buildModelPath(context, fsPath);
	}

	protected void setMetadata(SparkExecutionPluginContext context, ModelSpec modelSpec) throws Exception {
		modelSpec.setAlgoName(algoName);
		metadataWriter.setMetadata(context, modelSpec);
	}

}
