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

import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import de.kp.works.core.Algorithms;

public class ModelScanner {

	public String bestClassifier(Table table, String algoName, String modelName, String modelStage) {

		String fsPath = null;
		/*
		 * All classifiers are evaluated leveraging the same evaluator, i.e. no
		 * distinction between different algorithms is requied
		 */
		List<ClassifierMetric> metrics = new ArrayList<>();
		Row row;

		Scanner rows = table.scan(null, null);
		while ((row = rows.next()) != null) {

			String algorithm = row.getString("algorithm");
			if (algorithm.equals(algoName)) {

				String name = row.getString("name");
				if (name.equals(modelName)) {

					String stage = row.getString("stage");
					if (stage.equals(modelStage)) {

						ClassifierMetric metric = new ClassifierMetric();
						metric.fromRow(row);

						metrics.add(metric);

					}
				}
			}

		}

		fsPath = new ModelFinder().findClassifier(algoName, metrics);
		return fsPath;

	}

	public String bestCluster(Table table, String algoName, String modelName, String modelStage) {

		String fsPath = null;

		List<ClusterMetric> metrics = new ArrayList<>();
		Row row;

		Scanner rows = table.scan(null, null);
		while ((row = rows.next()) != null) {

			String algorithm = row.getString("algorithm");
			if (algorithm.equals(algoName)) {

				String name = row.getString("name");
				if (name.equals(modelName)) {

					String stage = row.getString("stage");
					if (stage.equals(modelStage)) {

						ClusterMetric metric = new ClusterMetric();
						metric.fromRow(row);

						metrics.add(metric);

					}
				}

			}

		}
		
		fsPath = new ModelFinder().findCluster(algoName, metrics);
		return fsPath;
		
	}

	public String bestFeature(Table table, String algoName, String modelName, String modelStage) {
		/*
		 * Feature models do not have any evaluation metric assign
		 */
		throw new IllegalArgumentException("Determine the best feature model is not supported.");

	}

	public String bestRecommender(Table table, String algoName, String modelName, String modelStage) {

		String fsPath = null;
		switch (algoName) {
		case Algorithms.ALS: {
			fsPath = bestRegressor(table, algoName, modelName, modelStage);
			break;
		}
		default:
			throw new IllegalArgumentException(
					String.format("Searching for the best recommender model is not supported for : %s.", algoName));
		}

		return fsPath;

	}

	public String bestRegressor(Table table, String algoName, String modelName, String modelStage) {

		String fsPath = null;
		/*
		 * All regressors are evaluated leveraging the same evaluator, i.e. no
		 * distinction between different algorithms is requied
		 */
		List<RegressorMetric> metrics = new ArrayList<>();
		Row row;

		Scanner rows = table.scan(null, null);
		while ((row = rows.next()) != null) {

			String algorithm = row.getString("algorithm");
			if (algorithm.equals(algoName)) {

				String name = row.getString("name");
				if (name.equals(modelName)) {

					String stage = row.getString("stage");
					if (stage.equals(modelStage)) {

						RegressorMetric metric = new RegressorMetric();
						metric.fromRow(row);

						metrics.add(metric);

					}
				}

			}

		}
		
		fsPath = new ModelFinder().findRegressor(algoName, metrics);
		return fsPath;
		
	}

	public String bestText(Table table, String algoName, String modelName, String modelStage) {

		String fsPath = null;
		switch (algoName) {

		case Algorithms.VIVEKN_SENTIMENT: {
			fsPath = bestRegressor(table, algoName, modelName, modelStage);
			break;
		}
		default:
			throw new IllegalArgumentException(
					String.format("Searching for best natural language model is not supported for : %s.", algoName));
		}

		return fsPath;
	}

	public String bestTime(Table table, String algoName, String modelName, String modelStage) {

		String fsPath = null;
		switch (algoName) {
		case Algorithms.ACF: {
			throw new IllegalArgumentException(
					String.format("Searching for best time series model is not supported for : %s.", algoName));
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
			fsPath = bestRegressor(table, algoName, modelName, modelStage);
			break;
		}
		default:
			throw new IllegalArgumentException(
					String.format("Searching for best time series model is not supported for : %s.", algoName));
		}

		return fsPath;
	}
}
