package de.kp.works.ml.config;
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

import java.util.HashMap;
import java.util.Map;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import de.kp.works.core.CRConfig;

public class GBTConfig extends CRConfig {

	private static final long serialVersionUID = -8822497679358372752L;

	@Description("The type of the loss function the Gradient-Boosted Trees algorithm tries to minimize. Default is 'logistic'.")
	@Macro
	public String lossType;

	@Description("The maximum number of bins used for discretizing continuous features and for choosing how to split "
			+ " on features at each node. More bins give higher granularity. Must be at least 2. Default is 32.")
	@Macro
	public Integer maxBins;

	@Description("Nonnegative value that maximum depth of the tree. E.g. depth 0 means 1 leaf node; "
			+ " depth 1 means 1 internal node + 2 leaf nodes. Default is 5.")
	@Macro
	public Integer maxDepth;

	@Description("The maximum number of iterations to train the Gradient-Boosted Trees model. Default is 20.")
	@Macro
	public Integer maxIter;

	@Description("The minimum information gain for a split to be considered at a tree node. The value should be at least 0.0. Default is 0.0.")
	@Macro
	public Double minInfoGain;

	@Description("The learning rate for shrinking the contribution of each estimator. Must be in interval (0, 1]. Default is 0.1.")
	@Macro
	public Double stepSize;

	public GBTConfig() {

		dataSplit = "70:30";

		lossType = "logistic";
		minInfoGain = 0D;

		maxBins = 32;
		maxDepth = 5;

		maxIter = 20;
		stepSize = 0.1;

	}

	@Override
	public Map<String, Object> getParamsAsMap() {

		Map<String, Object> params = new HashMap<>();

		params.put("lossType", lossType);
		params.put("minInfoGain", minInfoGain);

		params.put("maxBins", maxBins);
		params.put("maxDepth", maxDepth);

		params.put("maxIter", maxIter);
		params.put("stepSize", stepSize);

		params.put("dataSplit", dataSplit);
		return params;

	}

	public void validate() {
		super.validate();

		if (maxBins < 2)
			throw new IllegalArgumentException(
					String.format("[%s] The maximum bins must be at least 2.", this.getClass().getName()));

		if (maxDepth < 0)
			throw new IllegalArgumentException(
					String.format("[%s] The maximum depth must be nonnegative.", this.getClass().getName()));

		if (minInfoGain < 0D)
			throw new IllegalArgumentException(String
					.format("[%s] The minimum information gain must be at least 0.0.", this.getClass().getName()));

		if (maxIter < 1)
			throw new IllegalArgumentException(String.format(
					"[%s] The maximum number of iterations must be at least 1.", this.getClass().getName()));

		if (stepSize <= 0D || stepSize > 1D)
			throw new IllegalArgumentException(
					String.format("[%s] The learning rate must be in interval (0, 1].", this.getClass().getName()));

	}


}
