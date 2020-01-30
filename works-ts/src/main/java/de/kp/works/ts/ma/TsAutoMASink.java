package de.kp.works.ts.ma;
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
import de.kp.works.ts.params.ModelParams;

public class TsAutoMASink {

	private TsAutoMASinkConfig config;
	
	public TsAutoMASink(TsAutoMASinkConfig config) {
		this.config = config;
	}

	/* OK */
	public static class TsAutoMASinkConfig extends MASinkConfig {

		private static final long serialVersionUID = -5576870373916014257L;

		@Description(ModelParams.Q_MAX_PARAM_DESC)
		@Macro
		public Integer qmax;

		@Description(ModelParams.CRITERION_PARAM_DESC)
		@Macro
		public String criterion;

		public TsAutoMASinkConfig() {

			dataSplit = "70:30";
			
			elasticNetParam = 0.0;
			regParam = 0.0;
			
			fitIntercept = "true";
			standardization = "true";
			
			meanOut = "false";
			criterion = "aic";
			
		}
	    
		@Override
		public Map<String, Object> getParamsAsMap() {
			
			Map<String, Object> params = new HashMap<>();			
			params.put("dataSplit", dataSplit);
			
			params.put("qmax", qmax);

			params.put("elasticNetParam", elasticNetParam);
			params.put("regParam", regParam);
			
			params.put("fitIntercept", fitIntercept);
			params.put("standardization", standardization);

			params.put("criterion", criterion);
			params.put("meanOut", meanOut);

			return params;
		
		}
		
		public void validate() {
			super.validate();

			if (qmax < 1)
				throw new IllegalArgumentException(String
						.format("[%s] The upper limit of the order of the moving average must be positive.", this.getClass().getName()));
			
		}
		
	}

}
