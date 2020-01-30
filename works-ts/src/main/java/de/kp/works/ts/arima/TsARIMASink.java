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

import java.util.HashMap;
import java.util.Map;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import de.kp.works.ts.params.ModelParams;
public class TsARIMASink {

	private TsARIMASinkConfig config;

	public TsARIMASink(TsARIMASinkConfig config) {
		this.config = config;
	}
	
	/* OK */
	public static class TsARIMASinkConfig extends ARIMASinkConfig {

		private static final long serialVersionUID = -4780210026820266699L;

		@Description(ModelParams.P_PARAM_DESC)
		@Macro
		public Integer p;

		@Description(ModelParams.D_PARAM_DESC)
		@Macro
		public Integer d;

		@Description(ModelParams.Q_PARAM_DESC)
		@Macro
		public Integer q;
		
		public TsARIMASinkConfig() {

			dataSplit = "70:30";

			elasticNetParam = 0.0;
			regParam = 0.0;
			
			fitIntercept = "true";
			standardization = "true";

			meanOut = "false";
			
		}
	    
		@Override
		public Map<String, Object> getParamsAsMap() {
			
			Map<String, Object> params = new HashMap<>();			
			params.put("dataSplit", dataSplit);
			
			params.put("p", p);
			params.put("d", d);
			params.put("q", q);

			params.put("elasticNetParam", elasticNetParam);
			params.put("regParam", regParam);
			
			params.put("fitIntercept", fitIntercept);
			params.put("standardization", standardization);

			params.put("meanOut", meanOut);
			return params;
		
		}
		
		public void validate() {
			super.validate();

			if (p < 1)
				throw new IllegalArgumentException(String
						.format("[%s] The number of lag observations must be positive.", this.getClass().getName()));
			
			if (d < 1)
				throw new IllegalArgumentException(String
						.format("[%s] The degree of differencing must be positive.", this.getClass().getName()));

			if (q < 1)
				throw new IllegalArgumentException(String
						.format("[%s] The order of the moving average must be positive.", this.getClass().getName()));

			
		}
		
	}

}
