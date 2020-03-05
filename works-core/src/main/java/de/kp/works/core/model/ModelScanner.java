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

public class ModelScanner {

	public String bestClassifier(Table table, String algoName, String modelName, String modelStage) {
		/*
		 * All classifiers are evaluated leveraging the
		 * same evaluator, i.e. no distinction between
		 * different algorithms is requied
		 */
		List<ClassifierMetric> metrics = new ArrayList<>();
		Row row;

		Scanner rows = table.scan(null, null);
		while ((row = rows.next()) != null) {
			
			String algorithm = row.getString("algorithm");
			if (algorithm.equals(algoName)) {

				String name = row.getString("name");
				if (name.equals(modelName)) {

					ClassifierMetric metric = new ClassifierMetric();
					metric.fromRow(row);
					
					metrics.add(metric);
					
				}
				
			}

		}
		
		// TODO
		
		return null;
	}

	public String bestCluster(Table table, String algoName, String modelName, String modelStage) {
		// TODO
		return null;
	}

	public String bestFeature(Table table, String algoName, String modelName, String modelStage) {
		/*
		 * Feature models do not have any evaluation
		 * metric assign
		 */
		throw new IllegalArgumentException("Determine the best feature model is not supported.");

	}

	public String bestRecommender(Table table, String algoName, String modelName, String modelStage) {
		// TODO
		return null;
	}

	public String bestRegressor(Table table, String algoName, String modelName, String modelStage) {
		/*
		 * All regressors are evaluated leveraging the
		 * same evaluator, i.e. no distinction between
		 * different algorithms is requied
		 */
		List<RegressorMetric> metrics = new ArrayList<>();
		Row row;

		Scanner rows = table.scan(null, null);
		while ((row = rows.next()) != null) {
			
			String algorithm = row.getString("algorithm");
			if (algorithm.equals(algoName)) {

				String name = row.getString("name");
				if (name.equals(modelName)) {

					RegressorMetric metric = new RegressorMetric();
					metric.fromRow(row);
					
					metrics.add(metric);
					
				}
				
			}

		}
		
		return null;
	}

	public String bestText(Table table, String algoName, String modelName, String modelStage) {
		
		// TODO
		return null;
	}

	public String bestTime(Table table, String algoName, String modelName, String modelStage) {
		
		// TODO
		return null;
	}
}
