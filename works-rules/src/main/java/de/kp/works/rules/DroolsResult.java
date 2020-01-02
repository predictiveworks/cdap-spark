package de.kp.works.rules;

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

import java.util.Map;

/**
 * [DroolsResult] is a Java class that specifies the result of a certain rule
 * match.
 *
 */
public class DroolsResult {

	/*
	 * The initial fact that defines the basis for rule based filtering
	 */
	public final Map<String, Object> fact;
	/*
	 * The name of the field that contains a decision criterion
	 */
	public final String field;
	/*
	 * The value of the decision criterion
	 */
	public final Double value;

	public DroolsResult(Map<String, Object> fact) {
		this(fact, null, 0D);
	}

	public DroolsResult(Map<String, Object> fact, String field, Double value) {
		this.fact = fact;
		this.field = field;
		this.value = value;
	}
}
