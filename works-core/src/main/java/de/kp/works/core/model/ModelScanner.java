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

import io.cdap.cdap.api.dataset.table.Row;
import io.cdap.cdap.api.dataset.table.Scanner;
import io.cdap.cdap.api.dataset.table.Table;
import de.kp.works.core.Algorithms;

public class ModelScanner {

	public ModelProfile bestClassifier(Table table, String algoName, String modelName, String modelStage) {
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

		return ModelFinder.findClassifier(algoName, metrics);

	}

	public ModelProfile bestCluster(Table table, String algoName, String modelName, String modelStage) {

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
		
		return ModelFinder.findCluster(algoName, metrics);
		
	}

	public ModelProfile bestFeature(Table table, String algoName, String modelName, String modelStage) {
		return null;
	}

	public ModelProfile bestRecommender(Table table, String algoName, String modelName, String modelStage) {

		switch (algoName) {
		case Algorithms.ALS: {
			return bestRegressor(table, algoName, modelName, modelStage);
		}
		default:
			return null;
		}

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
		
		return ModelFinder.findRegressor(algoName, metrics);
		
	}

	public ModelProfile bestText(Table table, String algoName, String modelName, String modelStage) {

		switch (algoName) {

		case Algorithms.VIVEKN_SENTIMENT: {
			return bestRegressor(table, algoName, modelName, modelStage);
		}
		default:
			return null;
		}

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
