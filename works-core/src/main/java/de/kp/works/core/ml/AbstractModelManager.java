package de.kp.works.core.ml;

import java.lang.reflect.Type;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

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

import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;

public class AbstractModelManager {

	public Object getModelParam(Table table, String algorithmName, String modelName, String paramName) {

		String strParams = null;
		Row row;
		/*
		 * Scan through all baseline models and determine the latest params of the
		 * model with the same name
		 */
		Scanner rows = table.scan(null, null);
		while ((row = rows.next()) != null) {

			String algorithm = row.getString("algorithm");
			String name = row.getString("name");

			if (algorithm.equals(algorithmName) && name.equals(modelName)) {
				strParams = row.getString("params");
			}
		}
		
		if (strParams == null)
			return null;

		Type paramsType = new TypeToken<Map<String, Object>>(){}.getType();
		Map<String, Object> params = new Gson().fromJson(strParams, paramsType);
		
		return params.get(paramName);
		
	}

	protected String getModelVersion(Table table, String algorithmName, String modelName) {

		String strVersion = null;

		Row row;
		/*
		 * Scan through all baseline models and determine the latest version of the
		 * model with the same name
		 */
		Scanner rows = table.scan(null, null);
		while ((row = rows.next()) != null) {

			String algorithm = row.getString("algorithm");
			String name = row.getString("name");

			if (algorithm.equals(algorithmName) && name.equals(modelName)) {
				strVersion = row.getString("version");
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

	protected String getModelFsPath(Table table, String algorithmName, String modelName) {

		String fsPath = null;

		Row row;
		/*
		 * Scan through all baseline models and determine the latest 
		 * fileset path 
		 */
		Scanner rows = table.scan(null, null);
		while ((row = rows.next()) != null) {

			String algorithm = row.getString("algorithm");
			String name = row.getString("name");

			if (algorithm.equals(algorithmName) && name.equals(modelName)) {
				fsPath = row.getString("fsPath");
			}
		}

		return fsPath;
		
	}
}
