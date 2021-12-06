package de.kp.works.core;
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

public class Params {
	
	public static final String CHART_LIMIT = "The maximum number of data points taken into account "
			+ "to generate charts from. Default value is 1000.";
	
	public static final String CHART_SAMPLING = "The sampling method to consistently reduce the number "
			+ "of data points to generate charts from. Supported values are 'LLT-Buckets'. Default is 'LLT-Buckets'.";
	
	public static final String MODEL_OPTION = "An indicator to determine which model variant is used "
			+ "for predictions. "
			+ "Supported values are 'best' and 'latest'. Default is 'best'.";
	
	public static final String TIME_COL = "The name of the field that contains the timestamp.";
	
	public static final String TTL = "The Time-to-Live (TTL) property governs how long the time series data will "
			+ "be persisted in the specified table. TTL is configured as the maximum age (in seconds) that data "
			+ "should be retained. Default is 15 minutes, i.e. TTL = 900.";
	
	public static final String VALUE_COL = "The name of the field that contains the feature value.";
}
