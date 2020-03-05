package de.kp.works.text.embeddings;
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

import java.util.Date;

import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.api.batch.SparkPluginContext;

import de.kp.works.core.Algorithms;
import de.kp.works.core.ml.SparkMLManager;
import de.kp.works.core.ml.TextRecorder;

import de.kp.works.text.embeddings.Word2VecModel;

public class Word2VecRecorder extends TextRecorder {

	/**
	 * The Word2Vec model is used with other builders; as their initializtion
	 * phase is based on the basic plugin context, we need an extra read method
	 */
	public Word2VecModel read(SparkPluginContext context, String modelName, String modelStage, String modelOption) throws Exception {

		FileSet fs = SparkMLManager.getTextFS(context);
		Table table = SparkMLManager.getTextTable(context);
		
		return read(fs, table, modelName, modelStage, modelOption);
	}
	
	public Word2VecModel read(SparkExecutionPluginContext context, String modelName, String modelStage, String modelOption) throws Exception {

		FileSet fs = SparkMLManager.getTextFS(context);
		Table table = SparkMLManager.getTextTable(context);
		
		return read(fs, table, modelName, modelStage, modelOption);
	}
		
	private Word2VecModel read(FileSet fs, Table table, String modelName, String modelStage, String modelOption) throws Exception {

		String algorithmName = Algorithms.WORD2VEC;
		
		String fsPath = null;
		switch (modelOption) {
		case "best" : {
			fsPath = getBestModelFsPath(table, algorithmName, modelName, modelStage);
			break;
		}
		case "latest" : {
			fsPath = getLatestModelFsPath(table, algorithmName, modelName, modelStage);
			break;
		}
		default:
			throw new Exception(String.format("Model option '%s' is not supported yet.", modelOption));
		}

		if (fsPath == null) return null;
		/*
		 * Leverage Apache Spark mechanism to read the Word2Vec model
		 * from a model specific file set
		 */
		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		return Word2VecModel.load(modelPath);
		
	}

	public void track(SparkExecutionPluginContext context, String modelName, String modelStage, String modelParams, String modelMetrics,
			Word2VecModel model) throws Exception {

		String algorithmName = Algorithms.WORD2VEC;

		/***** ARTIFACTS *****/

		Long ts = new Date().getTime();
		String fsPath = algorithmName + "/" + ts.toString() + "/" + modelName;

		FileSet fs = SparkMLManager.getTextFS(context);

		String modelPath = fs.getBaseLocation().append(fsPath).toURI().getPath();
		model.save(modelPath);

		/***** METADATA *****/

		String modelPack = "WorksText";
		Table table = SparkMLManager.getTextTable(context);

		setMetadata(ts, table, algorithmName, modelName, modelPack, modelStage, modelParams, modelMetrics, fsPath);
		
	}

	public Object getParam(Table table, String modelName, String paramName) {

		String algorithmName = Algorithms.WORD2VEC;
		return getModelParam(table, algorithmName, modelName, paramName);
	
	}

}

