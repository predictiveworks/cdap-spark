package de.kp.works.text.ner;
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
import de.kp.works.core.BaseConfig;

public class BaseNERConfig  extends BaseConfig {

	private static final long serialVersionUID = 8733733573014386425L;

	@Description("The unique name of NER (CRF) model.")
	@Macro
	public String modelName;

	@Description("The unique name of Word2Vec embedding model.")
	@Macro
	public String embeddingName;

	@Description("The name of the field in the input schema that contains the document.")
	@Macro
	public String textCol;

	public void validate() {
		super.validate();

		if (Strings.isNullOrEmpty(modelName)) {
			throw new IllegalArgumentException(
					String.format("[%s] The model name must not be empty.", this.getClass().getName()));
		}

		if (Strings.isNullOrEmpty(embeddingName)) {
			throw new IllegalArgumentException(
					String.format("[%s] The name of the Word2Vec embedding model must not be empty.", this.getClass().getName()));
		}
		
		if (Strings.isNullOrEmpty(textCol)) {
			throw new IllegalArgumentException(String.format(
					"[%s] The name of the field that contains the text document must not be empty.",
					this.getClass().getName()));
		}
		
	}

}