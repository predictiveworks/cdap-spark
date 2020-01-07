package de.kp.works.core;

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

import co.cask.cdap.api.data.schema.Schema;

import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.api.batch.SparkPluginContext;
import de.kp.works.core.ml.SparkMLManager;

public class BaseClassifierSink extends BaseSink {

	private static final long serialVersionUID = -5552264323513756802L;

	protected FileSet modelFs;
	protected Table modelMeta;

	@Override
	public void prepareRun(SparkPluginContext context) throws Exception {
		/*
		 * Classification model components and metadata are persisted in a CDAP FileSet
		 * as well as a Table; at this stage, we have to make sure that these internal
		 * metadata structures are present
		 */
		SparkMLManager.createClassificationIfNotExists(context);
		/*
		 * Retrieve classification specified dataset for later use incompute
		 */
		modelFs = SparkMLManager.getClassificationFS(context);
		modelMeta = SparkMLManager.getClassificationMeta(context);

	}

	public void validateSchema(Schema inputSchema, BaseClassifierConfig config, String className) {

		if (inputSchema == null) {
			throw new IllegalArgumentException(String.format("[%s] The input schema must not be empty.", className));
		}

		/** FEATURES COLUMN **/

		Schema.Field featuresCol = inputSchema.getField(config.featuresCol);
		if (featuresCol == null) {
			throw new IllegalArgumentException(String.format(
					"[%s] The input schema must contain the field that defines the feature vector.", className));
		}

		Schema.Type featuresType = featuresCol.getSchema().getType();
		if (!featuresType.equals(Schema.Type.ARRAY)) {
			throw new IllegalArgumentException(
					String.format("[%s] The field that defines the feature vector must be an ARRAY.", className));
		}

		Schema.Type featureType = featuresCol.getSchema().getComponentSchema().getType();
		if (!featureType.equals(Schema.Type.DOUBLE)) {
			throw new IllegalArgumentException(
					String.format("[%s] The data type of the feature value must be a DOUBLE.", className));
		}

		/** LABEL COLUMN **/

		Schema.Field labelCol = inputSchema.getField(config.labelCol);
		if (labelCol == null) {
			throw new IllegalArgumentException(String
					.format("[%s] The input schema must contain the field that defines the label value.", className));
		}

		Schema.Type labelType = labelCol.getSchema().getType();
		/*
		 * The label must be a numeric data type (double, float, int, long), which then
		 * is casted to Double (see classification trainer)
		 */
		if (isNumericType(labelType) == false) {
			throw new IllegalArgumentException("The data type of the label field must be numeric.");
		}
	}
}
