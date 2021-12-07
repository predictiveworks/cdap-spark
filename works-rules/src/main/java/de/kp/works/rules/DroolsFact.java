package de.kp.works.rules;

/*
 * Copyright (c) 2019- 2021 Dr. Krusche & Partner PartG. All rights reserved.
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
/*
 * Example of a simple rule that demonstrates how to leverage
 * the [ApplyRules] component; note, setting up a rules requires
 * a minimum of programming knowledge 
 * 
 * package de.kp.predictive.rules;
 * 
 * global java.util.ArrayList droolsResults
 * 
 * rule "myrule"
 * 
 * when
 * 
 * DroolsFact($fact:fact)
 * eval(((String)$fact.get("field1")).equals("1313361152") && ((Integer)$fact.get("field2")) >= 20)
 * 
 * then
 *   droolsResults.add(new DroolsResult($fact,"decision1",0.25));
 *   
 * end
 * 
 */
public class DroolsFact {
	
	public final Map<String,Object> fact;
	
	public DroolsFact(Map<String,Object> fact) {
		this.fact = fact;
	}

}
