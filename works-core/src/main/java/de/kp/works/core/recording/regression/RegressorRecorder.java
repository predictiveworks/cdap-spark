package de.kp.works.core.recording.regression;

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

import de.kp.works.core.configuration.ConfigReader;
import de.kp.works.core.model.ModelProfile;
import de.kp.works.core.model.ModelSpec;
import de.kp.works.core.recording.AbstractRecorder;
import de.kp.works.core.recording.SparkMLManager;
import de.kp.works.core.recording.metadata.MetadataWriter;
import de.kp.works.core.recording.metadata.CDAPWriter;
import de.kp.works.core.recording.metadata.RemoteWriter;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;

public class RegressorRecorder extends AbstractRecorder {

	protected MetadataWriter metadataWriter;
	public RegressorRecorder(ConfigReader configReader) {
		super(configReader);
		/*
		 * Assign the algorithm type to this recorder to
		 * support type specific functionality
		 */
		algoType = SparkMLManager.REGRESSOR;
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

	public String getModelPath(SparkExecutionPluginContext context, String modelName, String modelStage, String modelOption) throws Exception {
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
