package de.kp.works.ml.regression;
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

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.regression.GeneralizedLinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.gson.Gson;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.BaseRegressorConfig;
import de.kp.works.core.BaseRegressorSink;

@Plugin(type = "sparksink")
@Name("GLRegressor")
@Description("A building stage for an Apache Spark based Generalized Linear regressor model.")
public class GLRegressor extends BaseRegressorSink {

	private static final long serialVersionUID = -3133087003110797539L;

	public GLRegressor(GLRegressorConfig config) {
		this.config = config;
		this.className = GLRegressor.class.getName();
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
		super.configurePipeline(pipelineConfigurer);

		/* Validate configuration */
		((GLRegressorConfig)config).validate();

		/* Validate schema */
		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
		inputSchema = stageConfigurer.getInputSchema();
		if (inputSchema != null)
			validateSchema(inputSchema, config);

	}
	
	@Override
	public void compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {
		
		GLRegressorConfig regressorConfig = (GLRegressorConfig)config;
		/*
		 * STEP #1: Extract parameters and train regression model
		 */
		String featuresCol = regressorConfig.featuresCol;
		String labelCol = regressorConfig.labelCol;

		Map<String, Object> params = regressorConfig.getParamsAsMap();
		/*
		 * The vectorCol specifies the internal column that has
		 * to be built from the featuresCol and that is used for
		 * training purposes
		 */
		String vectorCol = "_vector";
		/*
		 * Prepare provided dataset by vectorizing the feature
		 * column which is specified as Array[Numeric]
		 */
		GLTrainer trainer = new GLTrainer();
		Dataset<Row> vectorset = trainer.vectorize(source, featuresCol, vectorCol);
		/*
		 * Split the vectorset into a train & test dataset for
		 * later regression evaluation
		 */
	    Dataset<Row>[] splitted = vectorset.randomSplit(regressorConfig.getSplits());
		
	    Dataset<Row> trainset = splitted[0];
	    Dataset<Row> testset = splitted[1];

	    GeneralizedLinearRegressionModel model = trainer.train(trainset, vectorCol, labelCol, params);
		/*
		 * STEP #2: Compute accuracy of the trained regression
		 * model
		 */
	    String predictionCol = "_prediction";
	    model.setPredictionCol(predictionCol);

	    Dataset<Row> predictions = model.transform(testset);

	    RegressionEvaluator evaluator = new RegressionEvaluator();
	    evaluator.setLabelCol(labelCol);
	    evaluator.setPredictionCol(predictionCol);
	    
	    String metricName = "rmse";
	    evaluator.setMetricName(metricName);
	    
	    double accuracy = evaluator.evaluate(predictions);
		/*
		 * The accuracy coefficent is specified as JSON metrics for
		 * this regression model and stored by the DTRegressorManager
		 */
		Map<String,Object> metrics = new HashMap<>();
		
		metrics.put("name", metricName);
		metrics.put("coefficient", accuracy);
		/*
		 * STEP #3: Store trained regression model including its associated
		 * parameters and metrics
		 */
		String paramsJson = regressorConfig.getParamsAsJSON();
		String metricsJson = new Gson().toJson(metrics);
		
		String modelName = regressorConfig.modelName;
		new GLRegressorManager().save(modelFs, modelMeta, modelName, paramsJson, metricsJson, model);

	}

	public static class GLRegressorConfig extends BaseRegressorConfig {

		private static final long serialVersionUID = -17449766299180019L;

		@Description("The maximum number of iterations to train the Linear Regression model. Default is 25.")
		@Macro
		public Integer maxIter;

		@Description("The nonnegative regularization parameter. Default is 0.0.")
		@Macro
		public Double regParam;

		@Description("The positive convergence tolerance of iterations. Smaller values will lead to higher accuracy with the cost "
				+ "of more iterations. Default is 1e-6")
		@Macro
		public Double tol;

		/*
		 * The only solver algorithm is currently 'irls'; therefore, this parameter is
		 * omitted
		 */

		@Description("The name of the family which is a description of the error distribution used in this model. "
				+ "Supported values are: 'gaussian', 'binomial', 'poisson' and 'gamma'. The family values are correlated with the name of the link function. Default is 'gaussian'.")
		@Macro
		public String family;

		@Description("The name of the link function which provides the relationship between the linear predictor and the mean of the distribution function. "
				+ "Supported values are: 'identity', 'log', 'inverse', 'logit', 'probit', 'cloglog' and 'sqrt'. Default is 'identity' (gaussian).")
		@Macro
		public String link;

		public GLRegressorConfig() {

			dataSplit = "70:30";
			maxIter = 25;

			regParam = 0D;
			tol = 1e-6;

			family = "gaussian";

		}

		@Override
		public Map<String, Object> getParamsAsMap() {

			Map<String, Object> params = new HashMap<>();
			params.put("maxIter", maxIter);

			params.put("regParam", regParam);
			params.put("tol", tol);

			params.put("family", family);
			params.put("link", link);

			return params;

		}

		private Boolean validateFamilyAndLink() {

			switch (family) {
			case "gaussian": {

				if (link.equals("identity") || link.equals("log") || link.equals("inverse"))
					return true;
				else
					return false;
			}

			case "binomial": {

				if (link.equals("logit") || link.equals("probit") || link.equals("cloglog"))
					return true;
				else
					return false;

			}
			case "poisson": {

				if (link.equals("log") || link.equals("identity") || link.equals("sqrt"))
					return true;
				else
					return false;

			}
			case "gamma": {

				if (link.equals("inverse") || link.equals("identity") || link.equals("log"))
					return true;
				else
					return false;

			}
			default:
				return false;
			}

		}

		public void validate() {
			super.validate();

			/** PARAMETERS **/
			if (maxIter < 1)
				throw new IllegalArgumentException(String.format(
						"[%s] The maximum number of iterations must be at least 1.", this.getClass().getName()));

			if (regParam < 0D)
				throw new IllegalArgumentException(String
						.format("[%s] The regularization parameter must be at least 0.0.", this.getClass().getName()));

			if (tol <= 0D)
				throw new IllegalArgumentException(
						String.format("[%s] The iteration tolerance must be positive.", this.getClass().getName()));

			if (validateFamilyAndLink() == false) {
				throw new IllegalArgumentException(String.format(
						"[%s] The combination of the family parameter and the selected link function is not supported.",
						this.getClass().getName()));

			}
		}

	}

}
