package de.kp.works.text.embeddings;
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

import com.google.common.base.Strings;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;

import de.kp.works.text.config.ModelConfig;
import de.kp.works.text.util.Names;

public class BaseWord2VecConfig extends ModelConfig {

	private static final long serialVersionUID = -2427759073495034110L;

	@Description(Names.TEXT_COL)
	@Macro
	public String textCol;

	@Description(Names.NORMALIZATION)
	@Macro
	public String normalization;

	public Boolean getNormalization() {
		return normalization.equals("true");
	}
	
	public void validate() {
		super.validate();

		if (Strings.isNullOrEmpty(modelName)) {
			throw new IllegalArgumentException(
					String.format("[%s] The model name must not be empty.", this.getClass().getName()));
		}
		
		if (Strings.isNullOrEmpty(textCol)) {
			throw new IllegalArgumentException(String.format(
					"[%s] The name of the field that contains the text document must not be empty.",
					this.getClass().getName()));
		}
		
	}

}
