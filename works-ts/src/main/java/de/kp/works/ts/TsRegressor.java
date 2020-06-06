package de.kp.works.ts;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.spark.ml.regression.RandomForestRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.common.base.Strings;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkSink;
import de.kp.works.core.regressor.RFRRecorder;
import de.kp.works.core.regressor.RegressorSink;
import de.kp.works.core.time.TimeConfig;
import de.kp.works.core.ml.RegressorEvaluator;

@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name("TsRegressor")
@Description("A building stage for an Apache Spark ML Random Forest regressor model adjusted to machine learning for time series datasets.")
public class TsRegressor extends RegressorSink {
	/*
	 * Time series regression is a means for predicting future
	 * values in time series data; this regressor operates on
	 * a univariate time series and is limited to a Random
	 * Forest regression model 
	 */
	private static final long serialVersionUID = 1296032351141014689L;
	/*
	 * IMPORTANT: Suppose you have time series signals with seasonality
	 * and trend, make sure that the specified time column refers to the
	 * 'remainder' column after an STL decomposition has been applied.
	 */
	private TsRegressorConfig config;

	public TsRegressor(TsRegressorConfig config) {
		this.config = config;
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
		super.configurePipeline(pipelineConfigurer);

		/* Validate configuration */
		config.validate();
		
		/* Validate schema */
		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
		inputSchema = stageConfigurer.getInputSchema();
		if (inputSchema != null)
			validateSchema(inputSchema);

	}
	
	@Override
	public void compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {
		/*
		 * Random forest regression for time series datasets is slightly different
		 * from non-time series data. First, the split of the input must be performed
		 * in time, and, second, this split must be performed BEFORE vectorization to
		 * avoid data leakage from neighboring values
		 */
		Map<String, Object> params = config.getParamsAsMap();
		String modelParams = config.getParamsAsJSON();
		/*
		 * STEP #1: Split dataset into training & test timeseries
		 */
		TimeSplit splitter = new TimeSplit();
		splitter.setTimeCol(config.timeCol);
		splitter.setTimeSplit(config.timeSplit);
		
		Dataset<Row>[] splitted = splitter.timeSplit(source);
		/*
		 * STEP #2: Vectorization & labeling of the trainset
		 * and vectorization of the testset 
		 */		
		Lagging lagging = new Lagging();
		lagging.setLag(config.timeLag);
		
		lagging.setFeaturesCol("features");
		lagging.setLabelCol("label");
		
		/* Transform trainset into labeled features 
		 * 
		 * Features are built from the past k values 
		 * and the current value is used as label
		 */
		lagging.setLaggingType("featuresAndLabels");		
	    Dataset<Row> trainset = lagging.transform(splitted[0]);
	    
	    /* Transform testset into features only
	     * 
	     * Features are built from the past k values;
	     * the current value is excluded from lagging
	     */
		lagging.setLaggingType("pastFeatures");		
	    Dataset<Row> testset = lagging.transform(splitted[1]);
	    
	    /* STEP #3: Train Random Forest regression model */
		
	    RFRegressor trainer = new RFRegressor();
	    RandomForestRegressionModel model = trainer.train(trainset, "features", "label", params);
		/*
		 * STEP #4: Evaluate regression model and compute
		 * approved list of metrics
		 */
	    String predictionCol = "_prediction";
	    model.setPredictionCol(predictionCol);

	    Dataset<Row> predictions = model.transform(testset);
	    String modelMetrics = RegressorEvaluator.evaluate(predictions, "label", predictionCol);
		
		String modelName = config.modelName;
		String modelStage = config.modelStage;
		
		String modelPack = "WorksTS";
		new RFRRecorder().track(context, modelName, modelPack, modelStage, modelParams, modelMetrics, model);

	}

	@Override
	public void validateSchema(Schema inputSchema) {
		config.validateSchema(inputSchema);
	}

	public static class TsRegressorConfig extends TimeConfig {

		private static final long serialVersionUID = -2466365568045974575L;

		@Description("The unique name of the time prediction (regression) model.")
		@Macro
		public String modelName;

		@Description("The stage of the ML model. Supported values are 'experiment', 'stagging', 'production' and 'archived'. Default is 'experiment'.")
		@Macro
		public String modelStage;

		@Description("The split of the dataset into train & test data, e.g. 80:20. Note, this is a split time "
				+ "and is computed from the total time span (min, max) of the time series. Default is 70:30")
		@Macro
		public String timeSplit;
		
		/** LAGGING FOR VECTORIZATION **/
	    
		@Description("The positive number of past points of time to take into account for vectorization. Default is 20.")
		@Macro
		public Integer timeLag;
		
		/** RANDOM FOREST REGRESSOR **/

		/*
		 * Impurity is set to 'variance' and cannot be changed; therefore no external 
		 * parameter is provided. This is the difference to the Random Forest classifier
		 * configuration
		 */

		@Description("The maximum number of bins used for discretizing continuous features and for choosing how to split "
				+ " on features at each node. More bins give higher granularity. Must be at least 2. Default is 32.")
		@Macro
		public Integer maxBins;

		@Description("Nonnegative value that maximum depth of the tree. E.g. depth 0 means 1 leaf node; "
				+ " depth 1 means 1 internal node + 2 leaf nodes. Default is 5.")
		@Macro
		public Integer maxDepth;

		@Description("The minimum information gain for a split to be considered at a tree node. The value should be at least 0.0. Default is 0.0.")
		@Macro
		public Double minInfoGain;

		@Description("The number of trees to train the model. Default is 20.")
		@Macro
		public Integer numTrees;
		 		
		public TsRegressorConfig() {

			timeSplit = "70:30";
			modelStage = "experiment";
			
			timeLag = 20;

			maxBins = 32;
			maxDepth = 5;

			minInfoGain = 0D;
			numTrees = 20;

		}
	    
		@Override
		public Map<String, Object> getParamsAsMap() {

			Map<String, Object> params = new HashMap<>();

			params.put("maxBins", maxBins);
			params.put("maxDepth", maxDepth);

			params.put("minInfoGain", minInfoGain);
			params.put("numTrees", numTrees);

			params.put("timeSplit", timeSplit);
			params.put("timeLag", timeLag);
			return params;

		}
		
		public void validate() {
			super.validate();

			if (Strings.isNullOrEmpty(modelName)) {
				throw new IllegalArgumentException(
						String.format("[%s] The model name must not be empty.", this.getClass().getName()));
			}

			if (Strings.isNullOrEmpty(timeSplit)) {
				throw new IllegalArgumentException(
						String.format("[%s] The time split must not be empty.", this.getClass().getName()));
			}

			/** PARAMETERS **/
			if (timeLag < 1)
				throw new IllegalArgumentException(
						String.format("[%s] The number of past time points to take into account must be at least 1.", this.getClass().getName()));
			
			if (maxBins < 2)
				throw new IllegalArgumentException(
						String.format("[%s] The maximum bins must be at least 2.", this.getClass().getName()));

			if (maxDepth < 0)
				throw new IllegalArgumentException(
						String.format("[%s] The maximum depth must be nonnegative.", this.getClass().getName()));

			if (minInfoGain < 0D)
				throw new IllegalArgumentException(String
						.format("[%s] The minimum information gain must be at least 0.0.", this.getClass().getName()));

			if (numTrees < 1)
				throw new IllegalArgumentException(
						String.format("[%s] The number of trees must be at least 1.", this.getClass().getName()));
			
		}
		
		public double[] getSplits() {
			
			String[] tokens = timeSplit.split(":");
			
			Double x = Double.parseDouble(tokens[0]) / 100D;
			Double y = Double.parseDouble(tokens[1]) / 100D;
			
			List<Double> splits = new ArrayList<>();
			splits.add(x);
			splits.add(y);

			Double[] array = splits.toArray(new Double[splits.size()]);
			return Stream.of(array).mapToDouble(Double::doubleValue).toArray();

		}
	 		
	}
}
