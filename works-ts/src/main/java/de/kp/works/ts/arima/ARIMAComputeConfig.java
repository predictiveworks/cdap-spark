package de.kp.works.ts.arima;
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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import de.kp.works.core.Params;

public class ARIMAComputeConfig extends ARIMAConfig {

	private static final long serialVersionUID = 8844644617570325922L;

	@Description(Params.MODEL_OPTION)
	@Macro
	public String modelOption;

	@Description("The positive number of discrete time steps to look ahead. Default is 1.")
	@Macro
	public Integer steps;
	
	public void validate() {
		super.validate();

		if (steps < 1)
			throw new IllegalArgumentException(String.format(
					"[%s] The number of time steps to look ahead must be positive.", this.getClass().getName()));
	
	}
}
