package de.kp.works.ml.clustering;

/*
 * Copyright (c) 2019 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * This software is the confidential and proprietary information of 
 * Dr. Krusche & Partner PartG ("Confidential Information"). 
 * 
 * You shall not disclose such Confidential Information and shall use 
 * it only in accordance with the terms of the license agreement you 
 * entered into with Dr. Krusche & Partner PartG.
 * 
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 * 
 */

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

import org.apache.spark.ml.clustering.KMeansModel;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.api.batch.SparkPluginContext;
import co.cask.hydrator.common.Constants;
import de.kp.works.core.BaseSink;
import de.kp.works.ml.SparkMLManager;

@Plugin(type = "sparksink")
@Name("KMeansSink")
@Description("A KMeans model building stage.")

public class KMeansSink extends BaseSink {

	private static final long serialVersionUID = 8351695775316345380L;

	private KMeansConfig config;
	
	private FileSet modelFs;
	private Table modelMeta;
	
	public KMeansSink(KMeansConfig config) {
		this.config = config;
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
		super.configurePipeline(pipelineConfigurer);

		/* Validate configuration */
		config.validate();
		
		/*
		 * Validate whether the input schema exists, contains the specified
		 * field for the feature vector and defines the feature vector as an
		 * ARRAY[DOUBLE]
		 */
		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
		Schema inputSchema = stageConfigurer.getInputSchema();
		
		if (inputSchema == null) {
			throw new IllegalArgumentException("[KMeansSink] The input schema must not be empty.");
		}
		
		Schema.Field featuresCol = inputSchema.getField(config.featuresCol);
		if (featuresCol == null) {
			throw new IllegalArgumentException("[KMeansSink] The input schema must contain the field that defines the feature vector.");
		}
		
		Schema.Type featuresType = featuresCol.getSchema().getType();
		if (!featuresType.equals(Schema.Type.ARRAY)) {
			throw new IllegalArgumentException("[KMeansSink] The field that defines the feature vector must be an ARRAY.");
		}

		Schema.Type featureType = featuresCol.getSchema().getComponentSchema().getType();
		if (!featureType.equals(Schema.Type.DOUBLE)) {
			throw new IllegalArgumentException("[KMeansSink] The data type of the feature value must be a DOUBLE.");
		}
		
	}
	
	@Override
	public void prepareRun(SparkPluginContext context) throws Exception {
		/*
		 * KMeans model components and metadata are persisted in a CDAP FileSet
		 * as well as a Table; at this stage, we have to make sure that these internal
		 * metadata structures are present
		 */
		SparkMLManager.createClusteringIfNotExists(context);
		/*
		 * Retrieve clustering specified dataset for later use incompute
		 */
		modelFs = SparkMLManager.getClusteringFS(context);
		modelMeta = SparkMLManager.getClusteringMeta(context);
	}
	
	@Override
	public void compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

		/*
		 * STEP #1: Extract parameters and train KMeans model
		 */
		String featuresCol = config.featuresCol;
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
		KMeansTrainer trainer = new KMeansTrainer();
		Dataset<Row> vectorset = trainer.vectorize(source, featuresCol, vectorCol);

		KMeansModel model = trainer.train(vectorset, vectorCol, params);
		/*
		 * STEP #2: Compute silhouette coefficient as metric for this
		 * KMeans parameter setting: to this end, the predictions are
		 * computed based on the trained model and the vectorized data
		 * set
		 */
		String predictionCol = "_cluster";
		model.setPredictionCol(predictionCol);
		
		Dataset<Row> predictions = model.transform(vectorset);
		/*
		 * The KMeans evaluator computes the silhouette coefficent 
		 * of the computed predictions as a means to evaluate the
		 * quality of the chosen parameters
		 */
		KMeansEvaluator evaluator = new KMeansEvaluator();
		
		evaluator.setPredictionCol(predictionCol);
		evaluator.setVectorCol(vectorCol);
		
		evaluator.setMetricName("silhouette");
		evaluator.setDistanceMeasure("squaredEuclidean");
		
		Double coefficent = evaluator.evaluate(predictions);
		/*
		 * The silhouette coefficent is specified as JSON
		 * metrics for this KMeans model and stored by the
		 * KMeans manager
		 */
		Map<String,Object> metrics = new HashMap<>();
		
		metrics.put("name", "silhouette");
		metrics.put("measure", "squaredEuclidean");
		metrics.put("coefficient", coefficent);
		/*
		 * STEP #3: Store trained KMeans model including
		 * its associated parameters and metrics
		 */
		String paramsJson = config.getParamsAsJSON();
		String metricsJson = new Gson().toJson(metrics);
		
		String modelName = config.modelName;
		new KMeansManager().save(modelFs, modelMeta, modelName, paramsJson, metricsJson, model);
		
	}

	public static class KMeansConfig extends PluginConfig {

		private static final long serialVersionUID = -1071711175500255534L;
		  
		@Name(Constants.Reference.REFERENCE_NAME)
		@Description(Constants.Reference.REFERENCE_NAME_DESCRIPTION)
		public String referenceName;
		
	    @Description("The unique name of the KMeans clustering model.")
	    @Macro
	    private String modelName;
		
	    @Description("The name of the field that contains the feature vector.")
	    @Macro
	    private String featuresCol;
		
	    @Description("The number of cluster that have to be created.")
	    @Macro
	    private Integer k;
		
	    @Description("The (maximum) number of iterations the algorithm has to execute. Default value: 20")
		@Nullable
	    @Macro
	    private Integer maxIter;
		
	    @Description("The convergence tolerance of the algorithm. Default value: 1e-4")
		@Nullable
	    @Macro
	    private Double tolerance;
		
	    @Description("The number of steps for the initialization mode of the parallel KMeans algorithm. Default value: 2")
		@Nullable
	    @Macro
	    private Integer initSteps;
		
	    @Description("The initialization model of the algorithm. This can be either 'random' to choose random "
	    		+"random points as initial cluster center, 'parallel' to use the parallel variant of KMeans++. Default value: 'parallel'")
		@Nullable
	    @Macro
	    private String initMode;
	    
	    public KMeansConfig() {
	    	
	    		referenceName = "KMeansSink";
	    		
	    		maxIter = 20;
	    		tolerance = 1e-4;
	    		
	    		initSteps = 2;
	    		initMode = "parallel";
	    				
	    }
	    
		public Map<String, Object> getParamsAsMap() {
			
			Map<String,Object> params = new HashMap<String,Object>();
			params.put("k", k);
			params.put("maxIter", maxIter);
			
			params.put("tolerance", tolerance);
			
			params.put("initSteps", initSteps);
			params.put("initMode", initMode);
			
			return params;

		}
		
		public String getParamsAsJSON() {

			Gson gson = new Gson();			
			return gson.toJson(getParamsAsMap());
			
		}
		
		public void validate() {

			if (!Strings.isNullOrEmpty(modelName)) {
				throw new IllegalArgumentException("[KMeansConfig] The model name must not be empty.");
			}
			if (!Strings.isNullOrEmpty(featuresCol)) {
				throw new IllegalArgumentException("[KMeansConfig] The name of the field that contains the feature vector must not be empty.");
			}
			if (k <= 1) {
				throw new IllegalArgumentException("[KMeansConfig] The number of clusters must be greater than 1.");
			}
			if (maxIter <= 0) {
				throw new IllegalArgumentException("[KMeansConfig] The number of iterations must be greater than 0.");
			}
			if (initSteps <= 0) {
				throw new IllegalArgumentException("[KMeansConfig] The number of initial steps must be greater than 0.");
			}
			if (!(initMode.equals("random") || initMode.equals("parallel"))) {
				throw new IllegalArgumentException("[KMeansConfig] The initialization mode must be either 'parallel' or 'random'.");
			}
			
		}
	}
}
