package de.kp.works.ts.ar;
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

public class TsAutoARSink {

	private TsAutoARSinkConfig config;
	
	public TsAutoARSink(TsAutoARSinkConfig config) {
		this.config = config;
	}

	/* OK */
	public static class TsAutoARSinkConfig extends ARSinkConfig {

		private static final long serialVersionUID = -5935359961422461769L;

		@Description(ModelParams.MEAN_OUT_DESC)
		@Macro
		public String meanOut;

		@Description(ModelParams.P_MAX_PARAM_DESC)
		@Macro
		public Integer pmax;

		@Description(ModelParams.CRITERION_PARAM_DESC)
		@Macro
		public String criterion;

		public TsAutoARSinkConfig() {

			dataSplit = "70:30";

			elasticNetParam = 0.0;
			regParam = 0.0;

			criterion = "aic";
			meanOut = "false";			
			
			fitIntercept = "true";
			standardization = "true";
			
		}
	    
		@Override
		public Map<String, Object> getParamsAsMap() {
			
			Map<String, Object> params = new HashMap<>();
			
			params.put("dataSplit", dataSplit);
			params.put("pmax", pmax);

			params.put("elasticNetParam", elasticNetParam);
			params.put("regParam", regParam);
			
			params.put("fitIntercept", fitIntercept);
			params.put("standardization", standardization);

			params.put("meanOut", meanOut);
			params.put("criterion", criterion);

			return params;
		
		}

		public void validate() {
			super.validate();

			if (pmax < 1)
				throw new IllegalArgumentException(String
						.format("[%s] The upper limit of the number of lag observations must be positive.", this.getClass().getName()));
			
		}
		
	}

}
