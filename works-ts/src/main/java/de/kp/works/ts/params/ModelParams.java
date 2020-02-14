package de.kp.works.ts.params;
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

public class ModelParams {

	public static final String CRITERION_PARAM_DESC = "The information criterion to calculate for model parameter tuning. "
			+ "Supported values are 'aic' (Akaike Information Criterion), 'aicc' (AIC with correction for finite sample sizes) "
			+ "and 'bic' (Bayesian Information Criterion). Default is 'aic'.";

	public static final String MEAN_OUT_DESC = "The indicator to determine whether to remove the mean value from the value "
			+ "from the value of the time series before training model. Default is 'false'.";

	/** MODEL PARAMETERS **/
	
	public static final String P_PARAM_DESC = "The positive number of lag observations included in the "
			+ "time series model (also called the lag order).";

	public static final String P_MAX_PARAM_DESC = "The positive upper limit for tuning the number of "
			+ "lag operations (p).";

	public static final String Q_PARAM_DESC = "A positive number that specifies the size of the moving "
			+ "average window (also called the order of moving average).";

	public static final String Q_MAX_PARAM_DESC = "The positive upper limit for tuning the size of the "
			+ "moving average window (q).";

	public static final String D_PARAM_DESC = "The positive number of times that the raw observations "
			+ "are differenced (also called the degree of differencing).";

	public static final String D_MAX_PARAM_DESC = "The positive upper limit for tuning the degree of "
			+ "differencing (d).";

	/** LINEAR REGRESSION **/
	
	public static final String ELASTIC_NET_PARAM_DESC = "The ElasticNet mxing parameter. For value = 0.0, "
			+ "the penalty is an L2 penalty. For value = 1.0, it is an L1 penalty. For 0.0 < value < 1.0, "
			+ "the penalty is a combination of L1 and L2. Default is 0.0.";

	public static final String REG_PARAM_DESC = "The nonnegative regularization parameter. Default is 0.0.";
	
	public static final String STANDARDIZATION_DESC = "The indicator to determine whether to standardize the "
			+ "training features before fitting the model. Default is 'true'.";

	public static final String FIT_INTERCEPT_DESC = "The indicator to determine whether to fit an "
			+ "intercept value.";

	/** COLUMNS **/
	
	public static final String GROUP_COL_DESC = "The name of the input field for optional grouping & aggregation.";
	
	public static final String TIME_COL_DESC = "The name of the input field that contains the time of the time series.";
	
	public static final String VALUE_COL_DESC = "The name of the input field that contains the value of the time series";
	
}
