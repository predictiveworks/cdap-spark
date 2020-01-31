package de.kp.works.ts.stl;
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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import de.kp.works.core.TimeConfig;

public class BaseSTLConfig extends TimeConfig {

	private static final long serialVersionUID = 4387623595034151300L;

	@Description("The name of the field in the input schema that contains the group value required by "
			+ "decomposition algorithm.")
	@Macro
	public String groupCol;

	@Description("The positive number of cycles through the outer loop. More cycles here reduce the affect of outliers. "
			+ " For most situations this can be quite small. Default is 1.")
	@Macro
	public Integer outerIter;
	
	@Description("The positive number of cycles through the inner loop. Number of cycles should be large enough to reach "
			+ "convergence,  which is typically only two or three. When multiple outer cycles, the number of inner cycles "
			+ "can be smaller as they do not necessarily help get overall convergence. Default value is 2.")
	@Macro
	public Integer innerIter;
	
	@Description("The length of the seasonal LOESS smoother.")
	@Macro
	public Integer seasonalLoessSize;
	
	@Description("The length of the trend LOESS smoother.")
	@Macro
	public Integer trendLoessSize;
	
	@Description("The length of the level LOESS smoother.")
	@Macro
	public Integer levelLoessSize;
	
	public void validate() {
		super.validate();

		if (Strings.isNullOrEmpty(groupCol))
			throw new IllegalArgumentException(
					String.format("[%s] The name of the field that is used for grouping must not be empty.", this.getClass().getName()));
		
		if (outerIter < 1)
			throw new IllegalArgumentException(String.format(
					"[%s] The number of outer cycles must be at least 1.", this.getClass().getName()));
		
		if (innerIter < 1)
			throw new IllegalArgumentException(String.format(
					"[%s] The number of inner cycles must be at least 1.", this.getClass().getName()));

		if (seasonalLoessSize < 1)
			throw new IllegalArgumentException(String.format(
					"[%s] The size of the seasonal smoother must be at least 1.", this.getClass().getName()));

		if (trendLoessSize < 1)
			throw new IllegalArgumentException(String.format(
					"[%s] The size of the trend smoother must be at least 1.", this.getClass().getName()));

		if (levelLoessSize < 1)
			throw new IllegalArgumentException(String.format(
					"[%s] The size of the level smoother must be at least 1.", this.getClass().getName()));
		
	}

}
