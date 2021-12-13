package de.kp.works.text.recording;
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

import java.util.Date;

import com.johnsnowlabs.nlp.annotators.sda.vivekn.ViveknSentimentModel;

import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;

import de.kp.works.core.Algorithms;
import de.kp.works.core.recording.TextRecorder;
import de.kp.works.core.recording.SparkMLManager;

public class SentimentRecorder extends TextRecorder {

	public ViveknSentimentModel read(SparkExecutionPluginContext context, String modelName, String modelStage, String modelOption)
			throws Exception {

		String algorithmName = Algorithms.VIVEKN_SENTIMENT;

		String modelPath = getModelPath(context, algorithmName, modelName, modelStage, modelOption);
		if (modelPath == null) return null;
		/*
		 * Leverage Apache Spark mechanism to read the Sentiment Analysis model from a
		 * model specific file set
		 */
		return (ViveknSentimentModel) ViveknSentimentModel.load(modelPath);

	}

	public void track(SparkExecutionPluginContext context, String modelName, String modelStage, String modelParams,
			String modelMetrics, ViveknSentimentModel model) throws Exception {

		String algorithmName = Algorithms.VIVEKN_SENTIMENT;

		/* ARTIFACTS */

		long ts = new Date().getTime();
		String fsPath = algorithmName + "/" + ts + "/" + modelName;

		String modelPath = buildModelPath(context, fsPath);
		model.save(modelPath);

		/* METADATA */

		String modelPack = "WorksText";

		Table table = SparkMLManager.getTextTable(context);
		String namespace = context.getNamespace();

		setMetadata(ts, table, namespace, algorithmName, modelName, modelPack, modelStage, modelParams, modelMetrics, fsPath);

	}

}
