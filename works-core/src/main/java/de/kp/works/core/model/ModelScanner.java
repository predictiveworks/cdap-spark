package de.kp.works.core.model;
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

import java.util.ArrayList;
import java.util.List;

import de.kp.works.core.recording.SparkMLManager;
import io.cdap.cdap.api.dataset.table.Row;
import io.cdap.cdap.api.dataset.table.Scanner;
import io.cdap.cdap.api.dataset.table.Table;
import de.kp.works.core.Algorithms;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkPluginContext;

public class ModelScanner {
	/**
	 * This method retrieves the profile of the best (most accurate)
	 * machine learning model; the current implementation retrieves
	 * the metadata and the associated profile from an internal
	 * dataset.
	 */
	public ModelProfile bestProfile(SparkExecutionPluginContext context, String algoType, String algoName,
									String modelName, String modelStage) throws Exception {

		switch (algoType) {
			case SparkMLManager.CLASSIFIER: {
				Table table = context.getDataset(SparkMLManager.CLASSIFICATION_TABLE);
				return bestClassifier(table, algoName, modelName, modelStage);
			}
			case SparkMLManager.CLUSTER: {
				Table table = context.getDataset(SparkMLManager.CLUSTERING_TABLE);
				return bestCluster(table, algoName, modelName, modelStage);
			}
			case SparkMLManager.FEATURE: {
				Table table = context.getDataset(SparkMLManager.FEATURE_TABLE);
				return bestFeature(table, algoName, modelName, modelStage);
			}
			case SparkMLManager.RECOMMENDER: {
				Table table = context.getDataset(SparkMLManager.RECOMMENDATION_TABLE);
				return bestRecommender(table, algoName, modelName, modelStage);
			}
			case SparkMLManager.REGRESSOR: {
				Table table = context.getDataset(SparkMLManager.REGRESSION_TABLE);
				return bestRegressor(table, algoName, modelName, modelStage);
			}
			case SparkMLManager.TEXT: {
				Table table = context.getDataset(SparkMLManager.TEXTANALYSIS_TABLE);
				return bestText(table, algoName, modelName, modelStage);
			}
			case SparkMLManager.TIME: {
				Table table = context.getDataset(SparkMLManager.TIMESERIES_TABLE);
				return bestTime(table, algoName, modelName, modelStage);
			}
			default:
				throw new Exception(String.format("The algorithm type '%s' is not supported.", algoType));

		}

	}

	public ModelProfile bestProfile(SparkPluginContext context, String algoType, String algoName,
									String modelName, String modelStage) throws Exception {

		switch (algoType) {
			case SparkMLManager.CLASSIFIER: {
				Table table = context.getDataset(SparkMLManager.CLASSIFICATION_TABLE);
				return bestClassifier(table, algoName, modelName, modelStage);
			}
			case SparkMLManager.CLUSTER: {
				Table table = context.getDataset(SparkMLManager.CLUSTERING_TABLE);
				return bestCluster(table, algoName, modelName, modelStage);
			}
			case SparkMLManager.FEATURE: {
				Table table = context.getDataset(SparkMLManager.FEATURE_TABLE);
				return bestFeature(table, algoName, modelName, modelStage);
			}
			case SparkMLManager.RECOMMENDER: {
				Table table = context.getDataset(SparkMLManager.RECOMMENDATION_TABLE);
				return bestRecommender(table, algoName, modelName, modelStage);
			}
			case SparkMLManager.REGRESSOR: {
				Table table = context.getDataset(SparkMLManager.REGRESSION_TABLE);
				return bestRegressor(table, algoName, modelName, modelStage);
			}
			case SparkMLManager.TEXT: {
				Table table = context.getDataset(SparkMLManager.TEXTANALYSIS_TABLE);
				return bestText(table, algoName, modelName, modelStage);
			}
			case SparkMLManager.TIME: {
				Table table = context.getDataset(SparkMLManager.TIMESERIES_TABLE);
				return bestTime(table, algoName, modelName, modelStage);
			}
			default:
				throw new Exception(String.format("The algorithm type '%s' is not supported.", algoType));

		}

	}

	private ModelProfile bestClassifier(Table table, String algoName, String modelName, String modelStage) {
		/*
		 * All classifiers are evaluated leveraging the same evaluator, i.e. no
		 * distinction between different algorithms is required
		 */
		List<ClassifierMetric> metrics = new ArrayList<>();
		Row row;

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
					if (stage.equals(modelStage)) {

						ClassifierMetric metric = new ClassifierMetric();
						metric.fromRow(row);

						metrics.add(metric);

					}
				}
			}

		}

		return ModelFinder.findClassifier(algoName, metrics);

	}

	public ModelProfile bestCluster(Table table, String algoName, String modelName, String modelStage) {

		List<ClusterMetric> metrics = new ArrayList<>();
		Row row;

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
					if (stage.equals(modelStage)) {

						ClusterMetric metric = new ClusterMetric();
						metric.fromRow(row);

						metrics.add(metric);

					}
				}

			}

		}
		
		return ModelFinder.findCluster(algoName, metrics);
		
	}

	public ModelProfile bestFeature(Table table, String algoName, String modelName, String modelStage) {
		return null;
	}

	public ModelProfile bestRecommender(Table table, String algoName, String modelName, String modelStage) {

		if (Algorithms.ALS.equals(algoName)) {
			return bestRegressor(table, algoName, modelName, modelStage);
		}
		return null;

	}

	public ModelProfile bestRegressor(Table table, String algoName, String modelName, String modelStage) {

		/*
		 * All regressors are evaluated leveraging the same evaluator, i.e. no
		 * distinction between different algorithms is requied
		 */
		List<RegressorMetric> metrics = new ArrayList<>();
		Row row;

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
					if (stage.equals(modelStage)) {

						RegressorMetric metric = new RegressorMetric();
						metric.fromRow(row);

						metrics.add(metric);

					}
				}

			}

		}
		
		return ModelFinder.findRegressor(algoName, metrics);
		
	}

	public ModelProfile bestText(Table table, String algoName, String modelName, String modelStage) {

		if (Algorithms.VIVEKN_SENTIMENT.equals(algoName)) {
			return bestRegressor(table, algoName, modelName, modelStage);
		}
		return null;

	}

	public ModelProfile bestTime(Table table, String algoName, String modelName, String modelStage) {

		switch (algoName) {
		case Algorithms.ACF: {
			return null;
		}
		case Algorithms.AR:
		case Algorithms.ARIMA:
		case Algorithms.ARMA:
		case Algorithms.AUTO_AR:
		case Algorithms.AUTO_ARIMA:
		case Algorithms.AUTO_ARMA:
		case Algorithms.AUTO_MA:
		case Algorithms.DIFF_AR:
		case Algorithms.MA:
		case Algorithms.RANDOM_FOREST_TREE:
		case Algorithms.YULE_WALKER: {
			return bestRegressor(table, algoName, modelName, modelStage);
		}
		default:
			return null;
		}

	}
}
