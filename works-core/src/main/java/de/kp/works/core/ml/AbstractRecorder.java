package de.kp.works.core.ml;
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

import java.lang.reflect.Type;
import java.security.MessageDigest;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import io.cdap.cdap.api.dataset.table.Put;

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

import io.cdap.cdap.api.dataset.table.Row;
import io.cdap.cdap.api.dataset.table.Scanner;
import io.cdap.cdap.api.dataset.table.Table;
import de.kp.works.core.Names;
import de.kp.works.core.model.ModelProfile;

public class AbstractRecorder {

	protected ModelProfile profile;

	public ModelProfile getProfile() {
		return profile;
	}

	/*
	 * Metadata schemata for different ML model share common fields; this method is
	 * used to populate this shared fields
	 */
	public Put buildRow(byte[] key, Long timestamp, String name, String version, String fsName, String fsPath,
			String pack, String stage, String algorithm, String params) {
		/*
		 * Build unique model identifier from all information that is available for a
		 * certain model
		 */
		String mid = null;
		try {
			String[] parts = { String.valueOf(timestamp), name, version, fsName, fsPath, pack, stage, algorithm,
					params };

			String serialized = String.join("|", parts);
			mid = MessageDigest.getInstance("MD5").digest(serialized.getBytes()).toString();

		} catch (Exception e) {
			mid = String.valueOf(timestamp);

		}

		Put row = new Put(key).add(Names.TIMESTAMP, timestamp).add("id", mid).add("name", name).add("version", version)
				.add("fsName", fsName).add(Names.FS_PATH, fsPath).add("pack", pack).add("stage", stage)
				.add("algorithm", algorithm).add("params", params);

		return row;

	}

	public Object getModelParam(Table table, String algorithmName, String modelName, String paramName) {

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

			if (algorithm.equals(algorithmName) && name.equals(modelName)) {
				strParams = row.getString("params");
			}
		}

		if (strParams == null)
			return null;

		Type paramsType = new TypeToken<Map<String, Object>>() {
		}.getType();
		Map<String, Object> params = new Gson().fromJson(strParams, paramsType);

		return params.get(paramName);

	}

	public String getLatestModelVersion(Table table, String algorithmName, String modelName, String modelStage) {

		String strVersion = null;

		Row row;
		/*
		 * Scan through all baseline models and determine the latest version of the
		 * model with the same name
		 */
		Scanner rows = table.scan(null, null);
		while ((row = rows.next()) != null) {

			String algorithm = row.getString("algorithm");
			if (algorithm.equals(algorithmName)) {

				String name = row.getString("name");
				if (name.equals(modelName)) {

					String stage = row.getString("stage");
					if (stage.equals(modelStage))
						strVersion = row.getString("version");

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

	public ModelProfile getLatestModelProfile(Table table, String algorithmName, String modelName, String modelStage) {

		ModelProfile profile = null;

		Row row;
		/*
		 * Scan through all baseline models and determine the latest fileset path
		 */
		Scanner rows = table.scan(null, null);
		while ((row = rows.next()) != null) {

			String algorithm = row.getString("algorithm");
			if (algorithm.equals(algorithmName)) {

				String name = row.getString("name");
				if (name.equals(modelName)) {

					String stage = row.getString("stage");
					if (stage.equals(modelStage))
						profile = new ModelProfile().setId(row.getString("id")).setPath(row.getString("fsPath"));

				}
			}

		}

		return profile;

	}
}
