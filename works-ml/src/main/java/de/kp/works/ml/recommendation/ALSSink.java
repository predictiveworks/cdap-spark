package de.kp.works.ml.recommendation;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import de.kp.works.core.recording.recommendation.ALSRecorder;
import org.apache.spark.ml.recommendation.ALSModel;
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

import de.kp.works.core.SchemaUtil;
import de.kp.works.core.recording.RegressorEvaluator;
import de.kp.works.core.recommender.RecommenderSink;

@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name("ALSSink")
@Description("A building stage for an Apache Spark ML Collaborative Filtering model. This technique "
		+ "is commonly used for recommender systems and aims to fill in the missing entries of a "
		+ "User-Item association matrix. Spark ML uses the ALS (alternating least squares) algorithm.")
public class ALSSink extends RecommenderSink {
	/*
	 * Alternating Least Squares (ALS) matrix factorization.
	 *
	 * ALS attempts to estimate the ratings matrix `R` as the product of two lower-rank matrices,
	 * `X` and `Y`, i.e. `X * Yt = R`. Typically these approximations are called 'factor' matrices.
	 * The general approach is iterative. During each iteration, one of the factor matrices is held
	 * constant, while the other is solved for using least squares. The newly-solved factor matrix is
	 * then held constant while solving for the other factor matrix.
	 *
	 * This is a blocked implementation of the ALS factorization algorithm that groups the two sets
	 * of factors (referred to as "users" and "products") into blocks and reduces communication by only
	 * sending one copy of each user vector to each product block on each iteration, and only for the
	 * product blocks that need that user's feature vector. This is achieved by pre-computing some
	 * information about the ratings matrix to determine the "out-links" of each user (which blocks of
	 * products it will contribute to) and "in-link" information for each product (which of the feature
	 * vectors it receives from each user block it will depend on). This allows us to send only an
	 * array of feature vectors between each user block and product block, and have the product block
	 * find the users' ratings and update the products based on these messages.
	 *
	 * For implicit preference data, the algorithm used is based on
	 * "Collaborative Filtering for Implicit Feedback Datasets", available at
	 * http://dx.doi.org/10.1109/ICDM.2008.22, adapted for the blocked approach used here.
	 *
	 * Essentially instead of finding the low-rank approximations to the rating matrix `R`,
	 * this finds the approximations for a preference matrix `P` where the elements of `P` are 1 if
	 * r is greater than 0 and 0 if r is less than or equal to 0. The ratings then act as 'confidence'
	 * values related to strength of indicated user
	 * preferences rather than explicit ratings given to items.
	 */
	private static final long serialVersionUID = -3516142642574309010L;

	private final ALSSinkConfig config;
	
	public ALSSink(ALSSinkConfig config) {
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

		Map<String, Object> params = config.getParamsAsMap();
		String paramsJson = config.getParamsAsJSON();
		/*
		 * Split the dataset into a train & test dataset for later
		 * evaluation
		 */
		Dataset<Row>[] splitted =source.randomSplit(config.getSplits());

		Dataset<Row> trainset = splitted[0];
		Dataset<Row> testset = splitted[1];
		
		ALSTrainer trainer = new ALSTrainer();
		ALSModel model = trainer.train(trainset, config.userCol, config.itemCol, config.ratingCol, params);
		/*
		 * Evaluate recommendation model and compute approved
		 * list of metrics
		 */
		String predictionCol = "_prediction";
		model.setPredictionCol(predictionCol);

		Dataset<Row> predictions = model.transform(testset);
	    String modelMetrics = RegressorEvaluator.evaluate(predictions, config.ratingCol, predictionCol);
		/*
		 * STEP #3: Store trained recommendation model including
		 * its associated parameters and metrics
		 */		
		String modelName = config.modelName;
		String modelStage = config.modelStage;
		
		new ALSRecorder(configReader)
				.track(context, modelName, modelStage, paramsJson, modelMetrics, model);
		
	}

	@Override
	public void validateSchema(Schema inputSchema) {
		config.validateSchema(inputSchema);
	}
	
	public static class ALSSinkConfig extends ALSConfig {

		private static final long serialVersionUID = 1963471261850453801L;

		@Description("The name of the input field that defines the item ratings. The values must be within the integer value range.")
		@Macro
		public String ratingCol;
		
		@Description("A positive number that defines the rank of the matrix factorization. Default is 10.")
		@Macro
		public Integer rank;
		
		@Description("The number of user blocks. Default is 10.")
		@Macro
		public Integer numUserBlocks;
		
		@Description("The number of item blocks. Default is 10.")
		@Macro
		public Integer numItemBlocks;

		@Description("The maximum number of iterations to train the ALS model. Default is 10.")
		@Macro
		public Integer maxIter;

		@Description("The indicator to determine whether to use implicit preference. Support values are 'true' and 'false'. Default is 'false'.")
		@Macro
		public String implicitPrefs;
		
		@Description("The nonnegative alpha parameter in the implicit preference formulation. Default is 1.0.")
		@Macro
		public Double alpha;
		
		@Description("The indicator to determine whether to apply nonnegativity constraints for least squares. Support values are 'true' and 'false'. Default is 'false'.")
		@Macro
		public String nonnegative;

		@Description("The nonnegative regularization parameter. Default is 0.1.")
		@Macro
		public Double regParam;

		@Description("The split of the dataset into train & test data, e.g. 80:20. Default is 70:30.")
		@Macro
		public String dataSplit;
		
		public ALSSinkConfig() {

			dataSplit = "70:30";
			modelStage = "experiment";
			
			numUserBlocks = 10;
			numItemBlocks = 10;
			
			maxIter = 10;
			rank = 10;
		
			implicitPrefs = "false";
			alpha = 1.0;
			
			nonnegative = "false";
			regParam = 0.1;
			
		}
		
		@Override
		public Map<String, Object> getParamsAsMap() {

			Map<String, Object> params = new HashMap<>();

			params.put("rank", rank);
			params.put("maxIter", maxIter);

			params.put("numUserBlocks", numUserBlocks);
			params.put("numItemBlocks", numItemBlocks);

			params.put("implicitPrefs", implicitPrefs);
			params.put("alpha", alpha);
			
			params.put("nonnegative", nonnegative);
			params.put("regParam", regParam);
			
			params.put("dataSplit", dataSplit);

			return params;

		}
		
		public double[] getSplits() {
			
			String[] tokens = dataSplit.split(":");
			
			Double x = Double.parseDouble(tokens[0]) / 100D;
			Double y = Double.parseDouble(tokens[1]) / 100D;
			
			List<Double> splits = new ArrayList<>();
			splits.add(x);
			splits.add(y);

			Double[] array = splits.toArray(new Double[0]);
			return Stream.of(array).mapToDouble(Double::doubleValue).toArray();

		}
		
		public void validate() {
			super.validate();
			
			if (Strings.isNullOrEmpty(ratingCol))
				throw new IllegalArgumentException(
						String.format("[%s] The name of the field that contains the item ratings must not be empty.",
								this.getClass().getName()));

			if (Strings.isNullOrEmpty(dataSplit)) {
				throw new IllegalArgumentException(
						String.format("[%s] The data split must not be empty.",
								this.getClass().getName()));
			}

			if (rank < 1) {
				throw new IllegalArgumentException(String.format("[%s] The rank of the matrix factorization must be greater than 0.",
						this.getClass().getName()));
			}

			if (maxIter < 1)
				throw new IllegalArgumentException(String.format(
						"[%s] The maximum number of iterations must be at least 1.", this.getClass().getName()));

			if (numUserBlocks < 1) {
				throw new IllegalArgumentException(String.format("[%s] The number of user blocks must be greater than 0.",
						this.getClass().getName()));
			}

			if (numItemBlocks < 1) {
				throw new IllegalArgumentException(String.format("[%s] The number of item blocks must be greater than 0.",
						this.getClass().getName()));
			}

			if (implicitPrefs.equals("true") && alpha < 0.0) {
				throw new IllegalArgumentException(String.format("[%s] The alpha parameter for implicit preference must be nonnegative.",
						this.getClass().getName()));
			}

			if (regParam < 0.0) {
				throw new IllegalArgumentException(String.format("[%s] The regularization parameter must be nonnegative.",
						this.getClass().getName()));
			}
			
		}

		public void validateSchema(Schema inputSchema) {

			/* USER COLUMN */

			Schema.Field userField = inputSchema.getField(userCol);
			if (userField == null) {
				throw new IllegalArgumentException(String.format(
						"[%s] The input schema must contain the field that defines the user identifier.", this.getClass().getName()));
			}
			
			Schema.Type userType = getNonNullIfNullable(userField.getSchema()).getType();
			if (!SchemaUtil.isNumericType(userType)) {
				throw new IllegalArgumentException("The data type of the user field must be NUMERIC.");
			}

			/* ITEM COLUMN */

			Schema.Field itemField = inputSchema.getField(itemCol);
			if (itemField == null) {
				throw new IllegalArgumentException(String.format(
						"[%s] The input schema must contain the field that defines the user identifier.", this.getClass().getName()));
			}
			
			Schema.Type itemType = getNonNullIfNullable(itemField.getSchema()).getType();
			if (!SchemaUtil.isNumericType(itemType)) {
				throw new IllegalArgumentException("The data type of the item field must be NUMERIC.");
			}

			/* RATING COLUMN */

			Schema.Field ratingField = inputSchema.getField(ratingCol);
			if (ratingField == null) {
				throw new IllegalArgumentException(String.format(
						"[%s] The input schema must contain the field that defines the user identifier.", this.getClass().getName()));
			}
			
			Schema.Type ratingType = getNonNullIfNullable(ratingField.getSchema()).getType();
			if (!SchemaUtil.isNumericType(ratingType)) {
				throw new IllegalArgumentException("The data type of the rating field must be NUMERIC.");
			}

		}
				
	}
	
}
