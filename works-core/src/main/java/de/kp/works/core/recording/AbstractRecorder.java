package de.kp.works.core.recording;
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

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import de.kp.works.core.Names;
import de.kp.works.core.model.ModelProfile;
import de.kp.works.core.model.ModelScanner;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.api.dataset.table.Row;
import io.cdap.cdap.api.dataset.table.Scanner;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkPluginContext;

import java.lang.reflect.Type;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Map;

public abstract class AbstractRecorder {

	protected String algoType;
	protected ModelProfile profile;

	public ModelProfile getProfile() {
		return profile;
	}

	protected void setMetadata(SparkExecutionPluginContext context, long ts, String modelName, String modelPack, String modelStage,
							   String modelParams, String modelMetrics, String fsPath) throws Exception {

		/*
		 * CDAP specific operation of the metadata
		 * registration
		 */
		Table table = getTable(context);
		String modelNS = context.getNamespace();

		setMetadata(ts, table, modelNS, modelName, modelPack, modelStage, modelParams, modelMetrics, fsPath);

	}

	protected abstract void setMetadata(long ts, Table table, String modelNS, String modelName, String modelPack,
							   String modelStage, String modelParams, String modelMetrics, String fsPath);

	/*
	 * Metadata schemata for different ML model share common fields; this method is
	 * used to populate this shared fields
	 */
	public Put buildRow(byte[] key, Long timestamp, String namespace, String name, String version, String fsName, String fsPath,
			String pack, String stage, String algorithm, String params) {
		/*
		 * Build unique model identifier from all information that is available for a
		 * certain model
		 */
		String mid;
		try {
			String[] parts = { String.valueOf(timestamp), name, version, fsName, fsPath, pack, stage, algorithm,
					params };

			String serialized = String.join("|", parts);
			mid = Arrays.toString(MessageDigest.getInstance("MD5").digest(serialized.getBytes()));

		} catch (Exception e) {
			mid = String.valueOf(timestamp);

		}

		return new Put(key)
				.add(Names.TIMESTAMP, timestamp)
				.add("id", mid)
				.add("namespace", namespace)
				.add("name", name)
				.add("version", version)
				.add("fsName", fsName)
				.add(Names.FS_PATH, fsPath)
				.add("pack", pack)
				.add("stage", stage)
				.add("algorithm", algorithm)
				.add("params", params);

	}

	public Object getModelParam(Table table, String algoName, String modelName, String paramName) {

		String strParams = null;
		Row row;
		/*
		 * Scan through all baseline models and determine the latest params of the model
		 * with the same name
		 */
		Scanner rows = table.scan(null, null);
		while ((row = rows.next()) != null) {

			String algorithm = row.getString("algorithm");
			String name = row.getString("name");

			assert algorithm != null;
			if (algorithm.equals(algoName)) {
				assert name != null;
				if (name.equals(modelName)) {
					strParams = row.getString("params");
				}
			}
		}

		if (strParams == null)
			return null;

		Type paramsType = new TypeToken<Map<String, Object>>() {
		}.getType();
		Map<String, Object> params = new Gson().fromJson(strParams, paramsType);

		return params.get(paramName);

	}

	public String getLatestVersion(Table table, String algoName, String modelNS, String modelName, String modelStage) {

		String strVersion = null;

		Row row;
		/*
		 * Scan through all baseline models and determine the latest version of the
		 * model with the same name
		 */
		Scanner rows = table.scan(null, null);
		while ((row = rows.next()) != null) {

			String algorithm = row.getString("algorithm");
			assert algorithm != null;
			if (algorithm.equals(algoName)) {

				String name = row.getString("name");

				assert name != null;
				if (name.equals(modelName)) {

					String namespace = row.getString("namespace");

					assert namespace != null;
					if (namespace.equals(modelNS)) {
	
						String stage = row.getString("stage");

						assert stage != null;
						if (stage.equals(modelStage))
							strVersion = row.getString("version");

					}
				}
			}
		}

		if (strVersion == null) {
			return "M-1";

		} else {

			String[] tokens = strVersion.split("-");
			int numVersion = Integer.parseInt(tokens[1]) + 1;

			return Integer.toString(numVersion);
		}

	}

	public String getPath(SparkExecutionPluginContext context, String algoName,
						  String modelName, String modelStage, String modelOption) throws Exception {

		profile = getProfile(context, algoName, modelName, modelStage, modelOption);
		if (profile.fsPath == null) return null;

		return buildPath(context, profile.fsPath);

	}

	public String buildPath(SparkExecutionPluginContext context, String fsPath) throws Exception {

		FileSet fs = getFileSet(context);
		return fs.getBaseLocation().append(fsPath).toURI().getPath();

	}

	public String buildPath(SparkPluginContext context, String fsPath) throws Exception {
		/*
		 * This method generates the model path from the
		 * provided fsPath and the respective CDAP file system
		 */
		FileSet fs = getFileSet(context);
		return fs.getBaseLocation().append(fsPath).toURI().getPath();

	}

	public String getPath(SparkPluginContext context, String algoName,
						  String modelName, String modelStage, String modelOption) throws Exception {

		profile = getProfile(context, algoName, modelName, modelStage, modelOption);
		if (profile.fsPath == null) return null;

		return buildPath(context, profile.fsPath);

	}

	public ModelProfile getProfile(SparkExecutionPluginContext context, String algoName,
										String modelName, String modelStage, String modelOption) throws Exception {

		switch (modelOption) {
			case "best" : {
				profile = getBestProfile(context, algoName, modelName, modelStage);
				break;
			}
			case "latest" : {
				profile = getLatestProfile(context, algoName, modelName, modelStage);
				break;
			}
			default:
				throw new Exception(String.format("Model option '%s' is not supported yet.", modelOption));
		}

		return profile;

	}

	public ModelProfile getProfile(SparkPluginContext context, String algoName,
								   String modelName, String modelStage, String modelOption) throws Exception {

		switch (modelOption) {
			case "best" : {
				profile = getBestProfile(context, algoName, modelName, modelStage);
				break;
			}
			case "latest" : {
				profile = getLatestProfile(context, algoName, modelName, modelStage);
				break;
			}
			default:
				throw new Exception(String.format("Model option '%s' is not supported yet.", modelOption));
		}

		return profile;

	}

	protected ModelProfile getBestProfile(SparkExecutionPluginContext context, String algoName,
										  String modelName, String modelStage) throws Exception {

		ModelScanner scanner = new ModelScanner();

		ModelProfile profile = scanner.bestProfile(context, algoType, algoName, modelName, modelStage);
		if (profile == null)
			profile = getLatestProfile(context, algoName, modelName, modelStage);

		return profile;

	}
	protected ModelProfile getBestProfile(SparkPluginContext context, String algoName,
										  String modelName, String modelStage) throws Exception {

		ModelScanner scanner = new ModelScanner();

		ModelProfile profile = scanner.bestProfile(context, algoType, algoName, modelName, modelStage);
		if (profile == null)
			profile = getLatestProfile(context, algoName, modelName, modelStage);

		return profile;

	}

	public ModelProfile getLatestProfile(SparkExecutionPluginContext context,
										 String algoName, String modelName, String modelStage) throws Exception {

		Table table = getTable(context);
		return getLatestProfile(table, algoName, modelName, modelStage);

	}

	public ModelProfile getLatestProfile(SparkPluginContext context,
										 String algoName, String modelName, String modelStage) throws Exception {

		Table table = getTable(context);
		return getLatestProfile(table, algoName, modelName, modelStage);

	}

	private ModelProfile getLatestProfile(Table table, String algoName, String modelName, String modelStage) {

		ModelProfile profile = null;

		Row row;
		/*
		 * Scan through all baseline models and determine the latest fileset path
		 */
		Scanner rows = table.scan(null, null);
		while ((row = rows.next()) != null) {

			String algorithm = row.getString("algorithm");

			assert algorithm != null;
			if (algorithm.equals(algoName)) {

				String name = row.getString("name");

				assert name != null;
				if (name.equals(modelName)) {

					String stage = row.getString("stage");

					assert stage != null;
					if (stage.equals(modelStage))
						profile = new ModelProfile().setId(row.getString("id")).setPath(row.getString("fsPath"));

				}
			}

		}

		return profile;

	}

	private Table getTable(SparkExecutionPluginContext context) throws Exception {

		Table table;
		switch (algoType) {
			case SparkMLManager.CLASSIFIER: {
				table = context.getDataset(SparkMLManager.CLASSIFICATION_TABLE);
				break;
			}
			case SparkMLManager.CLUSTER: {
				table = context.getDataset(SparkMLManager.CLUSTERING_TABLE);
				break;
			}
			case SparkMLManager.FEATURE: {
				table = context.getDataset(SparkMLManager.FEATURE_TABLE);
				break;
			}
			case SparkMLManager.RECOMMENDER: {
				table = context.getDataset(SparkMLManager.RECOMMENDATION_TABLE);
				break;
			}
			case SparkMLManager.REGRESSOR: {
				table = context.getDataset(SparkMLManager.REGRESSION_TABLE);
				break;
			}
			case SparkMLManager.TEXT: {
				table = context.getDataset(SparkMLManager.TEXTANALYSIS_TABLE);
				break;
			}
			case SparkMLManager.TIME: {
				table = context.getDataset(SparkMLManager.TIMESERIES_TABLE);
				break;
			}
			default:
				throw new Exception(String.format("The algorithm type '%s' is not supported.", algoType));

		}

		return table;

	}

	private FileSet getFileSet(SparkExecutionPluginContext context) throws Exception {

		FileSet fs;
		switch (algoType) {
			case SparkMLManager.CLASSIFIER: {
				fs = SparkMLManager.getClassificationFS(context);
				break;
			}
			case SparkMLManager.CLUSTER: {
				fs = SparkMLManager.getClusteringFS(context);
				break;
			}
			case SparkMLManager.FEATURE: {
				fs = SparkMLManager.getFeatureFS(context);
				break;
			}
			case SparkMLManager.RECOMMENDER: {
				fs = SparkMLManager.getRecommendationFS(context);
				break;
			}
			case SparkMLManager.REGRESSOR: {
				fs = SparkMLManager.getRegressionFS(context);
				break;
			}
			case SparkMLManager.TEXT: {
				fs = SparkMLManager.getTextFS(context);
				break;
			}
			case SparkMLManager.TIME: {
				fs = SparkMLManager.getTimeFS(context);
				break;
			}
			default:
				throw new Exception(String.format("The algorithm type '%s' is not supported.", algoType));

		}

		return fs;

	}

	private FileSet getFileSet(SparkPluginContext context) throws Exception {

		FileSet fs;
		switch (algoType) {
			case SparkMLManager.CLASSIFIER: {
				fs = SparkMLManager.getClassificationFS(context);
				break;
			}
			case SparkMLManager.CLUSTER: {
				fs = SparkMLManager.getClusteringFS(context);
				break;
			}
			case SparkMLManager.FEATURE: {
				fs = SparkMLManager.getFeatureFS(context);
				break;
			}
			case SparkMLManager.RECOMMENDER: {
				fs = SparkMLManager.getRecommendationFS(context);
				break;
			}
			case SparkMLManager.REGRESSOR: {
				fs = SparkMLManager.getRegressionFS(context);
				break;
			}
			case SparkMLManager.TEXT: {
				fs = SparkMLManager.getTextFS(context);
				break;
			}
			case SparkMLManager.TIME: {
				fs = SparkMLManager.getTimeFS(context);
				break;
			}
			default:
				throw new Exception(String.format("The algorithm type '%s' is not supported.", algoType));

		}

		return fs;

	}

	private Table getTable(SparkPluginContext context) throws Exception {

		Table table;
		switch (algoType) {
			case SparkMLManager.CLASSIFIER: {
				table = context.getDataset(SparkMLManager.CLASSIFICATION_TABLE);
				break;
			}
			case SparkMLManager.CLUSTER: {
				table = context.getDataset(SparkMLManager.CLUSTERING_TABLE);
				break;
			}
			case SparkMLManager.FEATURE: {
				table = context.getDataset(SparkMLManager.FEATURE_TABLE);
				break;
			}
			case SparkMLManager.RECOMMENDER: {
				table = context.getDataset(SparkMLManager.RECOMMENDATION_TABLE);
				break;
			}
			case SparkMLManager.REGRESSOR: {
				table = context.getDataset(SparkMLManager.REGRESSION_TABLE);
				break;
			}
			case SparkMLManager.TEXT: {
				table = context.getDataset(SparkMLManager.TEXTANALYSIS_TABLE);
				break;
			}
			case SparkMLManager.TIME: {
				table = context.getDataset(SparkMLManager.TIMESERIES_TABLE);
				break;
			}
			default:
				throw new Exception(String.format("The algorithm type '%s' is not supported.", algoType));

		}

		return table;

	}

}
