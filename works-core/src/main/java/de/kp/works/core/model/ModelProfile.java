package de.kp.works.core.model;
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
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

public class ModelProfile {

	public String fsPath;
	public String id;
	/*
	 * This is a factor between [0, 1] with is best value at 1.
	 * It is derived from the normalized sum metric:
	 * 
	 * 				1 -  min / max
	 * 
	 */
	public Double trustability;
	
	public ModelProfile() {
		/*
		 * The default trustability is set to 1.0. This value
		 * is used for models that were not (or could not be)
		 * evaluated. 
		 */
		trustability = 1.0;
		
	}
	
	public ModelProfile setId(String id) {
		this.id = id;
		return this;
	}
	
	public ModelProfile setPath(String fsPath) {
		this.fsPath = fsPath;
		return this;
	}
	public ModelProfile setTrustability(Double trustability) {

		this.trustability = trustability;
		return this;
		
	}
}
