package de.kp.works.core.ml;
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

import java.util.ArrayList;
import java.util.List;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.dataset.table.TableProperties;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.api.batch.SparkPluginContext;

/**
 * [SparkMLManager] defines the entry point for Apache Spark based model
 * management; the current version supports the following Spark native models:
 * 
 * - classification 
 * - clustering 
 * - recommendation 
 * - regression 
 * - textanalysis
 * - timeseries
 *
 */
public class SparkMLManager {

	/*
	 * The model of the internal dataset that is used to persist metadata with
	 * respect to classification models.
	 */
	public static String CLASSIFICATION_META = "classificationMeta";
	/*
	 * The fileset name of the internal fileset that is used to store classification
	 * models
	 */
	public static String CLASSIFICATION_FS = "classificationFs";
	/*
	 * Note, we do NOT specify an absolute base path (begins with /) as this would
	 * be interpreted as an absolute path in the file system
	 */
	public static String CLASSIFICATION_FS_BASE = "models/classification/";

	/*
	 * The model of the internal dataset that is used to persist metadata with
	 * respect to clustering models.
	 */
	public static String CLUSTERING_META = "clusteringMeta";
	/*
	 * The fileset name of the internal fileset that is used to store clustering
	 * models
	 */
	public static String CLUSTERING_FS = "clusteringFs";
	/*
	 * Note, we do NOT specify an absolute base path (begins with /) as this would
	 * be interpreted as an absolute path in the file system
	 */
	public static String CLUSTERING_FS_BASE = "models/clustering/";
	/*
	 * The model of the internal dataset that is used to persist metadata with
	 * respect to feature models.
	 */
	public static String FEATURE_META = "featureMeta";
	/*
	 * The fileset name of the internal fileset that is used to store feature models
	 * (e.g. count vectorizer or word2vec models)
	 */
	public static String FEATURE_FS = "featureFs";
	/*
	 * Note, we do NOT specify an absolute base path (begins with /) as this would
	 * be interpreted as an absolute path in the file system
	 */
	public static String FEATURE_FS_BASE = "models/feature/";
	/*
	 * The model of the internal dataset that is used to persist metadata with
	 * respect to recommendation models.
	 */
	public static String RECOMMENDATION_META = "recommendationMeta";
	/*
	 * The fileset name of the internal fileset that is used to store recommendation
	 * models
	 */
	public static String RECOMMENDATION_FS = "recommendationFs";
	/*
	 * Note, we do NOT specify an absolute base path (begins with /) as this would
	 * be interpreted as an absolute path in the file system
	 */
	public static String RECOMMENDATION_FS_BASE = "models/recommendation/";
	/*
	 * The model of the internal dataset that is used to persist metadata with
	 * respect to regression models.
	 */
	public static String REGRESSION_META = "regressionMeta";
	/*
	 * The fileset name of the internal fileset that is used to store regression
	 * models
	 */
	public static String REGRESSION_FS = "regressionFs";
	/*
	 * Note, we do NOT specify an absolute base path (begins with /) as this would
	 * be interpreted as an absolute path in the file system
	 */
	public static String REGRESSION_FS_BASE = "models/regression/";
	
	/*
	 * The model of the internal dataset that is used to persist metadata with
	 * respect to text analysis models.
	 */
	public static String TEXTANALYSIS_META = "textanalysisMeta";
	/*
	 * The fileset name of the internal fileset that is used to store text analysis
	 * models
	 */
	public static String TEXTANALYSIS_FS = "textanalysisFs";
	/*
	 * Note, we do NOT specify an absolute base path (begins with /) as this would
	 * be interpreted as an absolute path in the file system
	 */
	public static String TEXTANALYSIS_FS_BASE = "models/textanalysis/";
	/*
	 * The model of the internal dataset that is used to persist metadata with
	 * respect to time series models.
	 */
	public static String TIMESERIES_META = "timeseriesMeta";
	/*
	 * The fileset name of the internal fileset that is used to store time series
	 * models
	 */
	public static String TIMESERIES_FS = "timeseriesFs";
	/*
	 * Note, we do NOT specify an absolute base path (begins with /) as this would
	 * be interpreted as an absolute path in the file system
	 */
	public static String TIMESERIES_FS_BASE = "models/timeseries/";

	/***** CLASSIFICATION *****/

	public static FileSet getClassificationFS(SparkPluginContext context) throws DatasetManagementException, Exception {

		if (context.datasetExists(CLASSIFICATION_FS) == false)
			throw new Exception("Fileset to store classification model components does not exist.");

		FileSet fs = context.getDataset(CLASSIFICATION_FS);
		return fs;

	}

	public static FileSet getClassificationFS(SparkExecutionPluginContext context)
			throws DatasetManagementException, Exception {

		FileSet fs = context.getDataset(CLASSIFICATION_FS);
		return fs;

	}

	public static Table getClassificationMeta(SparkPluginContext context) throws DatasetManagementException, Exception {

		if (context.datasetExists(CLASSIFICATION_META) == false)
			throw new Exception("Table to store classification model metadata does not exist.");

		Table table = context.getDataset(CLASSIFICATION_META);
		return table;

	}

	public static Table getClassificationMeta(SparkExecutionPluginContext context)
			throws DatasetManagementException, Exception {

		Table table = context.getDataset(CLASSIFICATION_META);
		return table;

	}

	public static void createClassificationIfNotExists(SparkPluginContext context) throws DatasetManagementException {

		if (context.datasetExists(CLASSIFICATION_FS) == false) {
			/*
			 * The classification fileset does not exist yet; this path is relative to the
			 * data directory of the CDAP namespace in which the FileSet is created.
			 * 
			 * Note, we do NOT specify an absolute base path (begins with /) as this would
			 * be interpreted as an absolute path in the file system
			 */
			context.createDataset(CLASSIFICATION_FS, FileSet.class.getName(),
					FileSetProperties.builder().setBasePath(CLASSIFICATION_FS_BASE).build());
		}

		if (context.datasetExists(CLASSIFICATION_META) == false) {
			/*
			 * This is the first time, that we train a classification model; therefore, the
			 * associated metadata dataset has to be created
			 */
			Schema metaSchema = createClassificationSchema();
			/*
			 * Create a CDAP table with the schema provided
			 */
			TableProperties.Builder builder = TableProperties.builder();
			builder.setDescription(
					"This table contains a timeseries of metadata information about Predictive Works classification models.");
			builder.setSchema(metaSchema);

			context.createDataset(CLASSIFICATION_META, Table.class.getName(), builder.build());

		}
		;

	}

	/***** CLUSTERING *****/

	public static FileSet getClusteringFS(SparkPluginContext context) throws DatasetManagementException, Exception {

		if (context.datasetExists(CLUSTERING_FS) == false)
			throw new Exception("Fileset to store clustering model components does not exist.");

		FileSet fs = context.getDataset(CLUSTERING_FS);
		return fs;

	}

	public static FileSet getClusteringFS(SparkExecutionPluginContext context)
			throws DatasetManagementException, Exception {

		FileSet fs = context.getDataset(CLUSTERING_FS);
		return fs;

	}

	public static Table getClusteringMeta(SparkPluginContext context) throws DatasetManagementException, Exception {

		if (context.datasetExists(CLUSTERING_META) == false)
			throw new Exception("Table to store clustering model metadata does not exist.");

		Table table = context.getDataset(CLUSTERING_META);
		return table;

	}

	public static Table getClusteringMeta(SparkExecutionPluginContext context)
			throws DatasetManagementException, Exception {

		Table table = context.getDataset(CLUSTERING_META);
		return table;

	}

	public static void createClusteringIfNotExists(SparkPluginContext context) throws DatasetManagementException {

		if (context.datasetExists(CLUSTERING_FS) == false) {
			/*
			 * The clustering fileset does not exist yet; this path is relative to the data
			 * directory of the CDAP namespace in which the FileSet is created.
			 * 
			 * Note, we do NOT specify an absolute base path (begins with /) as this would
			 * be interpreted as an absolute path in the file system
			 */
			context.createDataset(CLUSTERING_FS, FileSet.class.getName(),
					FileSetProperties.builder().setBasePath(CLUSTERING_FS_BASE).build());
		}

		if (context.datasetExists(CLUSTERING_META) == false) {
			/*
			 * This is the first time, that we train a clustering model; therefore, the
			 * associated metadata dataset has to be created
			 */
			Schema metaSchema = createMetaSchema("clusteringSchema");
			/*
			 * Create a CDAP table with the schema provided
			 */
			TableProperties.Builder builder = TableProperties.builder();
			builder.setDescription(
					"This table contains a timeseries of metadata information about Predictive Works clustering models.");
			builder.setSchema(metaSchema);

			context.createDataset(CLUSTERING_META, Table.class.getName(), builder.build());

		}
		;

	}

	/***** FEATURES *****/

	public static FileSet getFeatureFS(SparkPluginContext context) throws DatasetManagementException, Exception {

		if (context.datasetExists(FEATURE_FS) == false)
			throw new Exception("Fileset to store feature model components does not exist.");

		FileSet fs = context.getDataset(FEATURE_FS);
		return fs;

	}

	public static FileSet getFeatureFS(SparkExecutionPluginContext context)
			throws DatasetManagementException, Exception {

		FileSet fs = context.getDataset(FEATURE_FS);
		return fs;

	}

	public static Table getFeatureMeta(SparkPluginContext context) throws DatasetManagementException, Exception {

		if (context.datasetExists(FEATURE_META) == false)
			throw new Exception("Table to store feature model metadata does not exist.");

		Table table = context.getDataset(FEATURE_META);
		return table;

	}

	public static Table getFeatureMeta(SparkExecutionPluginContext context)
			throws DatasetManagementException, Exception {

		Table table = context.getDataset(FEATURE_META);
		return table;

	}

	public static void createFeatureIfNotExists(SparkPluginContext context) throws DatasetManagementException {

		if (context.datasetExists(FEATURE_FS) == false) {
			/*
			 * The feature fileset does not exist yet; this path is relative to the data
			 * directory of the CDAP namespace in which the FileSet is created.
			 * 
			 * Note, we do NOT specify an absolute base path (begins with /) as this would
			 * be interpreted as an absolute path in the file system
			 */
			context.createDataset(FEATURE_FS, FileSet.class.getName(),
					FileSetProperties.builder().setBasePath(FEATURE_FS_BASE).build());
		}

		if (context.datasetExists(FEATURE_META) == false) {
			/*
			 * This is the first time, that we train a feature model; therefore, the
			 * associated metadata dataset has to be created
			 */
			Schema metaSchema = createMetaSchema("featureSchema");
			/*
			 * Create a CDAP table with the schema provided
			 */
			TableProperties.Builder builder = TableProperties.builder();
			builder.setDescription(
					"This table contains a timeseries of metadata information about Predictive Works feature models.");
			builder.setSchema(metaSchema);

			context.createDataset(FEATURE_META, Table.class.getName(), builder.build());

		}
		;

	}

	/***** RECOMMENDATION *****/

	public static FileSet getRecommendationFS(SparkPluginContext context) throws DatasetManagementException, Exception {

		if (context.datasetExists(RECOMMENDATION_FS) == false)
			throw new Exception("Fileset to store recommendation model components does not exist.");

		FileSet fs = context.getDataset(RECOMMENDATION_FS);
		return fs;

	}

	public static FileSet getRecommendationFS(SparkExecutionPluginContext context)
			throws DatasetManagementException, Exception {

		FileSet fs = context.getDataset(RECOMMENDATION_FS);
		return fs;

	}

	public static Table getRecommendationMeta(SparkPluginContext context) throws DatasetManagementException, Exception {

		if (context.datasetExists(RECOMMENDATION_META) == false)
			throw new Exception("Table to store recommendation model metadata does not exist.");

		Table table = context.getDataset(RECOMMENDATION_META);
		return table;

	}

	public static Table getRecommendationMeta(SparkExecutionPluginContext context)
			throws DatasetManagementException, Exception {

		Table table = context.getDataset(RECOMMENDATION_META);
		return table;

	}

	public static void createRecommendationIfNotExists(SparkPluginContext context) throws DatasetManagementException {

		if (context.datasetExists(RECOMMENDATION_FS) == false) {
			/*
			 * The recommendation fileset does not exist yet; this path is relative to the
			 * data directory of the CDAP namespace in which the FileSet is created.
			 * 
			 * Note, we do NOT specify an absolute base path (begins with /) as this would
			 * be interpreted as an absolute path in the file system
			 */
			context.createDataset(RECOMMENDATION_FS, FileSet.class.getName(),
					FileSetProperties.builder().setBasePath(RECOMMENDATION_FS_BASE).build());
		}

		if (context.datasetExists(RECOMMENDATION_META) == false) {
			/*
			 * This is the first time, that we train a recommendation model; therefore, the
			 * associated metadata dataset has to be created
			 */
			Schema metaSchema = createMetaSchema("recommendationSchema");
			/*
			 * Create a CDAP table with the schema provided
			 */
			TableProperties.Builder builder = TableProperties.builder();
			builder.setDescription(
					"This table contains a timeseries of metadata information about Predictive Works recommendation models.");
			builder.setSchema(metaSchema);

			context.createDataset(RECOMMENDATION_META, Table.class.getName(), builder.build());

		}
		;

	}

	/***** REGRESSION *****/

	public static FileSet getRegressionFS(SparkPluginContext context) throws DatasetManagementException, Exception {

		if (context.datasetExists(REGRESSION_FS) == false)
			throw new Exception("Fileset to store regression model components does not exist.");

		FileSet fs = context.getDataset(REGRESSION_FS);
		return fs;

	}

	public static FileSet getRegressionFS(SparkExecutionPluginContext context)
			throws DatasetManagementException, Exception {

		FileSet fs = context.getDataset(REGRESSION_FS);
		return fs;

	}

	public static Table getRegressionMeta(SparkPluginContext context) throws DatasetManagementException, Exception {

		if (context.datasetExists(REGRESSION_META) == false)
			throw new Exception("Table to store regression model metadata does not exist.");

		Table table = context.getDataset(REGRESSION_META);
		return table;

	}

	public static Table getRegressionMeta(SparkExecutionPluginContext context)
			throws DatasetManagementException, Exception {

		Table table = context.getDataset(REGRESSION_META);
		return table;

	}

	public static void createRegressionIfNotExists(SparkPluginContext context) throws DatasetManagementException {

		if (context.datasetExists(REGRESSION_FS) == false) {
			/*
			 * The regression fileset does not exist yet; this path is relative to the data
			 * directory of the CDAP namespace in which the FileSet is created.
			 * 
			 * Note, we do NOT specify an absolute base path (begins with /) as this would
			 * be interpreted as an absolute path in the file system
			 */
			context.createDataset(REGRESSION_FS, FileSet.class.getName(),
					FileSetProperties.builder().setBasePath(REGRESSION_FS_BASE).build());
		}

		if (context.datasetExists(REGRESSION_META) == false) {
			/*
			 * This is the first time, that we train a regression model; therefore, the
			 * associated metadata dataset has to be created
			 */
			Schema metaSchema = createRegressionSchema();
			/*
			 * Create a CDAP table with the schema provided
			 */
			TableProperties.Builder builder = TableProperties.builder();
			builder.setDescription(
					"This table contains a timeseries of metadata information about Predictive Works regression models.");
			builder.setSchema(metaSchema);

			context.createDataset(REGRESSION_META, Table.class.getName(), builder.build());

		}
		;

	}

	/***** TEXT ANALYSIS *****/

	public static FileSet getTextanalysisFS(SparkPluginContext context) throws DatasetManagementException, Exception {

		if (context.datasetExists(TEXTANALYSIS_FS) == false)
			throw new Exception("Fileset to store text analysis model components does not exist.");

		FileSet fs = context.getDataset(TEXTANALYSIS_FS);
		return fs;

	}

	public static FileSet getTextanalysisFS(SparkExecutionPluginContext context)
			throws DatasetManagementException, Exception {

		FileSet fs = context.getDataset(TEXTANALYSIS_FS);
		return fs;

	}

	public static Table getTextanalysisMeta(SparkPluginContext context) throws DatasetManagementException, Exception {

		if (context.datasetExists(TEXTANALYSIS_META) == false)
			throw new Exception("Table to store text analysis model metadata does not exist.");

		Table table = context.getDataset(TEXTANALYSIS_META);
		return table;

	}

	public static Table getTextanalysisMeta(SparkExecutionPluginContext context)
			throws DatasetManagementException, Exception {

		Table table = context.getDataset(TEXTANALYSIS_META);
		return table;

	}

	public static void createTextanalysisIfNotExists(SparkPluginContext context) throws DatasetManagementException {

		if (context.datasetExists(TEXTANALYSIS_FS) == false) {
			/*
			 * The text analysis fileset does not exist yet; this path is relative to the data
			 * directory of the CDAP namespace in which the FileSet is created.
			 * 
			 * Note, we do NOT specify an absolute base path (begins with /) as this would
			 * be interpreted as an absolute path in the file system
			 */
			context.createDataset(TEXTANALYSIS_FS, FileSet.class.getName(),
					FileSetProperties.builder().setBasePath(TEXTANALYSIS_FS_BASE).build());
		}

		if (context.datasetExists(TEXTANALYSIS_META) == false) {
			/*
			 * This is the first time, that we train a text analysis model; therefore, the
			 * associated metadata dataset has to be created
			 */
			Schema metaSchema = createMetaSchema("textanalysisSchema");
			/*
			 * Create a CDAP table with the schema provided
			 */
			TableProperties.Builder builder = TableProperties.builder();
			builder.setDescription(
					"This table contains a timeseries of metadata information about Predictive Works text analysis models.");
			builder.setSchema(metaSchema);

			context.createDataset(TEXTANALYSIS_META, Table.class.getName(), builder.build());

		}
		;

	}
	
	/***** TIMESERIES *****/

	public static FileSet getTimeseriesFS(SparkPluginContext context) throws DatasetManagementException, Exception {

		if (context.datasetExists(TIMESERIES_FS) == false)
			throw new Exception("Fileset to store timeseries model components does not exist.");

		FileSet fs = context.getDataset(TIMESERIES_FS);
		return fs;

	}

	public static FileSet getTimeseriesFS(SparkExecutionPluginContext context)
			throws DatasetManagementException, Exception {

		FileSet fs = context.getDataset(TIMESERIES_FS);
		return fs;

	}

	public static Table getTimeseriesMeta(SparkPluginContext context) throws DatasetManagementException, Exception {

		if (context.datasetExists(TIMESERIES_META) == false)
			throw new Exception("Table to store timeseries model metadata does not exist.");

		Table table = context.getDataset(TIMESERIES_META);
		return table;

	}

	public static Table getTimeseriesMeta(SparkExecutionPluginContext context)
			throws DatasetManagementException, Exception {

		Table table = context.getDataset(TIMESERIES_META);
		return table;

	}

	public static void createTimeseriesIfNotExists(SparkPluginContext context) throws DatasetManagementException {

		if (context.datasetExists(TIMESERIES_FS) == false) {
			/*
			 * The timeseries fileset does not exist yet; this path is relative to the data
			 * directory of the CDAP namespace in which the FileSet is created.
			 * 
			 * Note, we do NOT specify an absolute base path (begins with /) as this would
			 * be interpreted as an absolute path in the file system
			 */
			context.createDataset(TIMESERIES_FS, FileSet.class.getName(),
					FileSetProperties.builder().setBasePath(TIMESERIES_FS_BASE).build());
		}

		if (context.datasetExists(TIMESERIES_META) == false) {
			/*
			 * This is the first time, that we train a timeseries model; therefore, the
			 * associated metadata dataset has to be created
			 */
			Schema metaSchema = createMetaSchema("timeseriesSchema");
			/*
			 * Create a CDAP table with the schema provided
			 */
			TableProperties.Builder builder = TableProperties.builder();
			builder.setDescription(
					"This table contains a timeseries of metadata information about Predictive Works timeseries models.");
			builder.setSchema(metaSchema);

			context.createDataset(TIMESERIES_META, Table.class.getName(), builder.build());

		}
		;

	}

	private static Schema createMetaSchema(String name) {

		List<Schema.Field> fields = new ArrayList<>();
		/*
		 * The timestamp this model has been created
		 */
		fields.add(Schema.Field.of("timestamp", Schema.of(Schema.Type.LONG)));
		/*
		 * The name of a certain ML model
		 */
		fields.add(Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
		/*
		 * The version of a certain ML model
		 */
		fields.add(Schema.Field.of("version", Schema.of(Schema.Type.STRING)));
		/*
		 * The algorithm of a certain ML model
		 */
		fields.add(Schema.Field.of("algorithm", Schema.of(Schema.Type.STRING)));
		/*
		 * The parameters of a certain ML model; this is a JSON object that contains the
		 * parameter set that has been used to train a certain model instance
		 */
		fields.add(Schema.Field.of("params", Schema.of(Schema.Type.STRING)));
		/*
		 * The metrics of a certain ML model; this is a JSON object that contains metric
		 * data of a certain model instance
		 */
		fields.add(Schema.Field.of("metrics", Schema.of(Schema.Type.STRING)));
		/*
		 * The fileset name of a certain ML model; the model itself is persisted
		 * leveraging Apache Spark's internal mechanism backed by CDAP's fileset API
		 */
		fields.add(Schema.Field.of("fsName", Schema.of(Schema.Type.STRING)));
		fields.add(Schema.Field.of("fsPath", Schema.of(Schema.Type.STRING)));

		Schema schema = Schema.recordOf(name, fields);
		return schema;

	}

	private static Schema createClassificationSchema() {

		String schemaName = "classificationSchema";
		List<Schema.Field> fields = new ArrayList<>();
		/*
		 * The timestamp this classification model has been created
		 */
		fields.add(Schema.Field.of("timestamp", Schema.of(Schema.Type.LONG)));
		/*
		 * The name of this classification model
		 */
		fields.add(Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
		/*
		 * The version of this classification model
		 */
		fields.add(Schema.Field.of("version", Schema.of(Schema.Type.STRING)));
		/*
		 * The algorithm of this classification model
		 */
		fields.add(Schema.Field.of("algorithm", Schema.of(Schema.Type.STRING)));
		/*
		 * The parameters of this classification model; this is a JSON object 
		 * that contains the parameter set that has been used to train a certain 
		 * model instance
		 */
		fields.add(Schema.Field.of("params", Schema.of(Schema.Type.STRING)));
		/*
		 * The calculcated metric values for this classification model; 
		 * currently the following metrics are supported:
		 * 
	     * - accuracy
		 * - f1
		 * - hammingLoss
		 * - weightedFMeasure
		 * - weightedPrecision
		 * - weightedRecall
		 * - weightedFalsePositiveRate
		 * - weightedTruePositiveRate
 		 */
		fields.add(Schema.Field.of("accuracy", Schema.of(Schema.Type.DOUBLE)));
		fields.add(Schema.Field.of("f1", Schema.of(Schema.Type.DOUBLE)));
		fields.add(Schema.Field.of("hammingLoss", Schema.of(Schema.Type.DOUBLE)));
		fields.add(Schema.Field.of("weightedFMeasure", Schema.of(Schema.Type.DOUBLE)));
		fields.add(Schema.Field.of("weightedPrecision", Schema.of(Schema.Type.DOUBLE)));
		fields.add(Schema.Field.of("weightedRecall", Schema.of(Schema.Type.DOUBLE)));
		fields.add(Schema.Field.of("weightedFalsePositiveRate", Schema.of(Schema.Type.DOUBLE)));
		fields.add(Schema.Field.of("weightedTruePositiveRate", Schema.of(Schema.Type.DOUBLE)));		
		/*
		 * The fileset name of this classification model; the model itself is persisted
		 * leveraging Apache Spark's internal mechanism backed by CDAP's fileset API
		 */
		fields.add(Schema.Field.of("fsName", Schema.of(Schema.Type.STRING)));
		fields.add(Schema.Field.of("fsPath", Schema.of(Schema.Type.STRING)));

		Schema schema = Schema.recordOf(schemaName, fields);
		return schema;

	}

	private static Schema createRegressionSchema() {

		String schemaName = "regressionSchema";
		List<Schema.Field> fields = new ArrayList<>();
		/*
		 * The timestamp this regression model has been created
		 */
		fields.add(Schema.Field.of("timestamp", Schema.of(Schema.Type.LONG)));
		/*
		 * The name of this regression model
		 */
		fields.add(Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
		/*
		 * The version of this regression model
		 */
		fields.add(Schema.Field.of("version", Schema.of(Schema.Type.STRING)));
		/*
		 * The algorithm of this regression model
		 */
		fields.add(Schema.Field.of("algorithm", Schema.of(Schema.Type.STRING)));
		/*
		 * The parameters of this regression model; this is a JSON object that contains
		 * the parameter set that has been used to train a certain model instance
		 */
		fields.add(Schema.Field.of("params", Schema.of(Schema.Type.STRING)));
		/*
		 * The calculcated metric values for this regression model; currently the
		 * following metrics are supported:
		 *  
		 * - root mean squared error (rsme) 
		 * - mean squared error (mse) 
		 * - mean absolute error (mae) 
		 * - r^2 metric (r2)
		 * 
		 */
		fields.add(Schema.Field.of("rsme", Schema.of(Schema.Type.DOUBLE)));
		fields.add(Schema.Field.of("mse", Schema.of(Schema.Type.DOUBLE)));
		fields.add(Schema.Field.of("mae", Schema.of(Schema.Type.DOUBLE)));
		fields.add(Schema.Field.of("r2", Schema.of(Schema.Type.DOUBLE)));
		/*
		 * The fileset name for this regression model; the model itself is persisted
		 * leveraging Apache Spark's internal mechanism backed by CDAP's fileset API
		 */
		fields.add(Schema.Field.of("fsName", Schema.of(Schema.Type.STRING)));
		fields.add(Schema.Field.of("fsPath", Schema.of(Schema.Type.STRING)));

		Schema schema = Schema.recordOf(schemaName, fields);
		return schema;

	}

}
