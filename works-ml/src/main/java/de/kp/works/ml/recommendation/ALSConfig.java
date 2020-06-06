package de.kp.works.ml.recommendation;
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

import com.google.common.base.Strings;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import de.kp.works.core.recommender.RecommenderConfig;

public class ALSConfig extends RecommenderConfig {

	private static final long serialVersionUID = -2675778811255294711L;

	@Description("The name of the input field that defines the user identifiers. The values must be within the integer value range.")
	@Macro
	public String userCol;

	@Description("The name of the input field that defines the item identifiers. The values must be within the integer value range.")
	@Macro
	public String itemCol;

	public void validate() {
		super.validate();
		
		if (Strings.isNullOrEmpty(userCol))
			throw new IllegalArgumentException(
					String.format("[%s] The name of the field that contains the user identifiers must not be empty.",
							this.getClass().getName()));
		
		
		if (Strings.isNullOrEmpty(itemCol))
			throw new IllegalArgumentException(
					String.format("[%s] The name of the field that contains the item identifiers must not be empty.",
							this.getClass().getName()));
	
	}
}
