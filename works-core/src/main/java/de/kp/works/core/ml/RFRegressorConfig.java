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

import java.util.HashMap;
import java.util.Map;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import de.kp.works.core.BaseRegressorConfig;

public class RFRegressorConfig extends BaseRegressorConfig {

	private static final long serialVersionUID = 7150058657685557952L;

	/*
	 * Impurity is set to 'variance' and cannot be changed; therefore no external 
	 * parameter is provided. This is the difference to the Random Forest classifier
	 * configuration
	 */

	@Description("The maximum number of bins used for discretizing continuous features and for choosing how to split "
			+ " on features at each node. More bins give higher granularity. Must be at least 2. Default is 32.")
	@Macro
	public Integer maxBins;

	@Description("Nonnegative value that maximum depth of the tree. E.g. depth 0 means 1 leaf node; "
			+ " depth 1 means 1 internal node + 2 leaf nodes. Default is 5.")
	@Macro
	public Integer maxDepth;

	@Description("The minimum information gain for a split to be considered at a tree node. The value should be at least 0.0. Default is 0.0.")
	@Macro
	public Double minInfoGain;

	@Description("The number of trees to train the model. Default is 20.")
	@Macro
	public Integer numTrees;
	
	public RFRegressorConfig() {

		dataSplit = "70:30";

		maxBins = 32;
		maxDepth = 5;

		minInfoGain = 0D;
		numTrees = 20;

	}
    
	@Override
	public Map<String, Object> getParamsAsMap() {

		Map<String, Object> params = new HashMap<>();

		params.put("maxBins", maxBins);
		params.put("maxDepth", maxDepth);

		params.put("minInfoGain", minInfoGain);
		params.put("numTrees", numTrees);

		return params;

	}

	public void validate() {
		super.validate();

		/** PARAMETERS **/
		if (maxBins < 2)
			throw new IllegalArgumentException(
					String.format("[%s] The maximum bins must be at least 2.", this.getClass().getName()));

		if (maxDepth < 0)
			throw new IllegalArgumentException(
					String.format("[%s] The maximum depth must be nonnegative.", this.getClass().getName()));

		if (minInfoGain < 0D)
			throw new IllegalArgumentException(String
					.format("[%s] The minimum information gain must be at least 0.0.", this.getClass().getName()));

		if (numTrees < 1)
			throw new IllegalArgumentException(
					String.format("[%s] The number of trees must be at least 1.", this.getClass().getName()));

	}
	
}
