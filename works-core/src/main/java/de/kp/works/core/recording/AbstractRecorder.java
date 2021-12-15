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
import de.kp.works.core.configuration.ConfigReader;
import io.cdap.cdap.api.dataset.table.Row;
import io.cdap.cdap.api.dataset.table.Scanner;
import io.cdap.cdap.api.dataset.table.Table;

import java.lang.reflect.Type;
import java.util.Map;

public abstract class AbstractRecorder {

	protected ConfigReader configReader;
	/*
	 * Static context information about the machine
	 * learning model to record
	 */
	protected String algoName;
	protected String algoType;

	public AbstractRecorder(ConfigReader configReader) {
		this.configReader = configReader;
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

}
