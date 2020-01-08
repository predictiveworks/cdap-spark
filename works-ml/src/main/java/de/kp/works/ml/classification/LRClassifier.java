package de.kp.works.ml.classification;
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

import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.common.base.Strings;
import com.google.gson.Gson;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.BaseClassifierConfig;
import de.kp.works.core.BaseClassifierSink;

@Plugin(type = "sparksink")
@Name("LRClassifer")
@Description("A building stage for an Apache Spark based Logistic Regression classifier model.")
public class LRClassifier extends BaseClassifierSink {

	private static final long serialVersionUID = 8968908020294101566L;
	
	public LRClassifier(LRClassifierConfig config) {
		this.config = config;
		this.className = LRClassifier.class.getName();
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
			validateSchema(inputSchema, config);

	}
	
	@Override
	public void compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {
		/*
		 * STEP #1: Extract parameters and train classifier model
		 */
		String featuresCol = config.featuresCol;
		String labelCol = config.labelCol;

		Map<String, Object> params = config.getParamsAsMap();
		/*
		 * The vectorCol specifies the internal column that has
		 * to be built from the featuresCol and that is used for
		 * training purposes
		 */
		String vectorCol = "_vector";
		/*
		 * Prepare provided dataset by vectorizing the feature
		 * column which is specified as Array[Double]
		 */
		LRTrainer trainer = new LRTrainer();
		Dataset<Row> vectorset = trainer.vectorize(source, featuresCol, vectorCol);
		/*
		 * Split the vectorset into a train & test dataset for
		 * later classification evaluation
		 */
	    Dataset<Row>[] splitted = vectorset.randomSplit(config.getSplits());
		
	    Dataset<Row> trainset = splitted[0];
	    Dataset<Row> testset = splitted[1];
		
	    LogisticRegressionModel model = trainer.train(trainset, vectorCol, labelCol, params);
		/*
		 * STEP #2: Compute accuracy of the trained classification
		 * model
		 */
	    String predictionCol = "_prediction";
	    model.setPredictionCol(predictionCol);

	    Dataset<Row> predictions = model.transform(testset);
	    /*
	     * This Logistic Regression plugin leverages the multiclass evaluator
	     * independent of whether the algorithm is used for binary classification
	     * or not.
	     */
	    MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator();
	    evaluator.setLabelCol(labelCol);
	    evaluator.setPredictionCol(predictionCol);
	    
	    String metricName = "accuracy";
	    evaluator.setMetricName(metricName);
	    
	    double accuracy = evaluator.evaluate(predictions);
		/*
		 * The accuracy coefficent is specified as JSON
		 * metrics for this classification model and stored
		 * by the LRClassifierManager
		 */
		Map<String,Object> metrics = new HashMap<>();
		
		metrics.put("name", metricName);
		metrics.put("coefficient", accuracy);
		/*
		 * STEP #3: Store trained classification model 
		 * including its associated parameters and metrics
		 */
		String paramsJson = config.getParamsAsJSON();
		String metricsJson = new Gson().toJson(metrics);
		
		String modelName = config.modelName;
		new LRClassifierManager().save(modelFs, modelMeta, modelName, paramsJson, metricsJson, model);

	}

	public static class LRClassifierConfig extends BaseClassifierConfig {

		private static final long serialVersionUID = -4962647798125981464L;

		@Description("The maximum number of iterations to train the Gradient-Boosted Trees model. Default is 20.")
		@Macro
		public Integer maxIter;
		
		@Description("The ElasticNet mxing parameter. For value = 0.0, the penalty is an L2 penalty."
				+ "For value = 1.0, it is an L1 penalty. For 0.0 < value < 1.0, the penalty is a combination of "
				+ "L1 and L2. Default is 0.0.")
		
		@Macro
		public Double elasticNetParam;
		
		@Description("The nonnegative regularization parameter. Default is 0.0.")
		@Macro
		public Double regParam;
		
		@Description("The positive convergence tolerance of iterations. Smaller values wuth lead to higher accuracy with the cost "
				+ "of more iterations. Default is 1e-6")
		@Macro
		public Double tol;
		
		public LRClassifierConfig() {
			/*
			 * The family of the label distribution is set to 'auto', which selects
			 * the distribution family based on the number of classes:
			 * 
			 * numClasses = 1 || numClasses = 2, set to 'binomal' (binary logistic
			 * regression with pivoting) and otherwise 'multinominal' (softmax 
			 * logistic regression without pivoting)
			 */
			dataSplit = "70:30";
			maxIter = 20;
			
			elasticNetParam = 0D;
			regParam = 0D;
			tol = 1e-6;
			
		}
	    
		@Override
		public Map<String, Object> getParamsAsMap() {
			
			Map<String, Object> params = new HashMap<>();

			params.put("maxIter", maxIter);

			params.put("elasticNetParam", elasticNetParam);
			params.put("regParam", regParam);

			params.put("tol", tol);
			params.put("split", dataSplit);

			return params;
		
		}

		public void validate() {

			/** MODEL & COLUMNS **/
			if (!Strings.isNullOrEmpty(modelName)) {
				throw new IllegalArgumentException("[LRClassifierConfig] The model name must not be empty.");
			}
			if (!Strings.isNullOrEmpty(featuresCol)) {
				throw new IllegalArgumentException("[LRClassifierConfig] The name of the field that contains the feature vector must not be empty.");
			}
			if (!Strings.isNullOrEmpty(labelCol)) {
				throw new IllegalArgumentException("[LRClassifierConfig] The name of the field that contains the label value must not be empty.");
			}
			
			/** PARAMETERS **/			
			if (maxIter < 1)
				throw new IllegalArgumentException("[LRClassifierConfig] The maximum number of iterations must be at least 1.");
			
			if (elasticNetParam < 0D || elasticNetParam > 1D)
				throw new IllegalArgumentException("[LRClassifierConfig] The ElasticNet mixing parameter must be in interval [0, 1].");
			
			if (regParam < 0D)
				throw new IllegalArgumentException("[LRClassifierConfig] The regularization parameter must be at least 0.0.");
			
			if (tol <= 0D)
				throw new IllegalArgumentException("[LRClassifierConfig] The iteration tolerance must be positive.");
						
		}
		
	}
}
