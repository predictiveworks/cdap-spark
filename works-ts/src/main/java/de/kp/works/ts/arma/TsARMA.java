package de.kp.works.ts.arma;

import de.kp.works.ts.model.ARMAModel;

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

public class TsARMA {

	private TsARMAConfig config;
	private ARMAModel model;
	
	public TsARMA(TsARMAConfig config) {
		this.config = config;
	}

	public static class TsARMAConfig extends ARMAConfig {

		private static final long serialVersionUID = -2565279706592741956L;

		public void validate() {
			super.validate();
		}
		
	}
}
