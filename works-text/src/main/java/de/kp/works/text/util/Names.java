package de.kp.works.text.util;
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

public class Names {

	public static final String SENTENCE_COL = "The name of the field in the output schema that contains the extracted sentences.";
	public static final String TEXT_COL = "The name of the field in the input schema that contains the text document.";
	
	public static final String NORM_COL = "The name of the field in the output schema that contains the normalized tokens.";
	public static final String TOKEN_COL = "The name of the field in the output schema that contains the extracted tokens.";

	public static final String LEMMA_COL = "The name of the field in the output schema that contains the detected lemmas.";
	public static final String STEM_COL = "The name of the field in the output schema that contains the stemmed tokens.";
	public static final String CLEAN_COL = "The name of the field in the output schema that contains the cleaned tokens.";
	
	public static final String SPELL_COL = "The name of the field in the output schema that contains the suggested spellings.";
	
	public static final String NORMALIZATION = "The indicator to determine whether token normalization has to be applied. "
			+ "Normalization restricts the characters of a token to [A-Za-z0-9-]. Supported values are 'true' and 'false'. "
			+ "Default is 'true'.";
	
	
	public static final String POOLING_STRATEGY = "The pooling strategy how to merge word embeddings into sentence embeddings. "
			+ "Supported values are 'average' and 'sum'. Default is 'average'.";
	
}
