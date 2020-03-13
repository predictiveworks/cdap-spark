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
import de.kp.works.core.Names;

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
	 * The name of the internal dataset that is used to persist metadata for
	 * all classifier models in a time series; this dataset is publically
	 * visible from the CDAP user interface
	 */
	public static String CLASSIFICATION_TABLE = "Classifiers";
	/*
	 * The fileset name of the internal fileset that is used to store 
	 * classification model artifacts; it is a convention that the last
	 * part of the base path is equal to the fs name
	 */
	public static String CLASSIFICATION_FS = "classification";
	/*
	 * Note, we do NOT specify an absolute base path (begins with /) as this would
	 * be interpreted as an absolute path in the file system
	 */
	public static String CLASSIFICATION_FS_BASE = "models/classification/";

	/*
	 * The name of the internal dataset that is used to persist metadata for
	 * all cluster models
	 */
	public static String CLUSTERING_TABLE = "Clusters";
	/*
	 * The fileset name of the internal fileset that is used to store clustering
	 * model artifacts; it is a convention that the last part of the base path is 
	 * equal to the fs name
	 */
	public static String CLUSTERING_FS = "clustering";
	/*
	 * Note, we do NOT specify an absolute base path (begins with /) as this would
	 * be interpreted as an absolute path in the file system
	 */
	public static String CLUSTERING_FS_BASE = "models/clustering/";
	/*
	 * The name of the internal dataset that is used to persist metadata for
	 * all feature models.
	 */
	public static String FEATURE_TABLE = "Features";
	/*
	 * The fileset name of the internal fileset that is used to store feature model
	 * (e.g. count vectorizer or word2vec models) artifacts; it is a convention that 
	 * the last part of the base path is equal to the fs name
	 */
	public static String FEATURE_FS = "feature";
	/*
	 * Note, we do NOT specify an absolute base path (begins with /) as this would
	 * be interpreted as an absolute path in the file system
	 */
	public static String FEATURE_FS_BASE = "models/feature/";
	/*
	 * The model of the internal dataset that is used to persist metadata for
	 * recommendation models.
	 */
	public static String RECOMMENDATION_TABLE = "Recommenders";
	/*
	 * The fileset name of the internal fileset that is used to store recommendation
	 * model artifacts; it is a convention that the last part of the base path is 
	 * equal to the fs name
	 */
	public static String RECOMMENDATION_FS = "recommendation";
	/*
	 * Note, we do NOT specify an absolute base path (begins with /) as this would
	 * be interpreted as an absolute path in the file system
	 */
	public static String RECOMMENDATION_FS_BASE = "models/recommendation/";
	/*
	 * The model of the internal dataset that is used to persist metadata for
	 * all regression models.
	 */
	public static String REGRESSION_TABLE = "Regressors";
	/*
	 * The fileset name of the internal fileset that is used to store regression
	 * model artifacts; it is a convention that the last part of the base path is 
	 * equal to the fs name
	 */
	public static String REGRESSION_FS = "regression";
	/*
	 * Note, we do NOT specify an absolute base path (begins with /) as this would
	 * be interpreted as an absolute path in the file system
	 */
	public static String REGRESSION_FS_BASE = "models/regression/";
	
	/*
	 * The model of the internal dataset that is used to persist metadata for
	 * all text analysis models.
	 */
	public static String TEXTANALYSIS_TABLE = "TextModels";
	/*
	 * The fileset name of the internal fileset that is used to store text analysis
	 * model artifacts; it is a convention that the last part of the base path is 
	 * equal to the fs name
	 */
	public static String TEXTANALYSIS_FS = "textanalysis";
	/*
	 * Note, we do NOT specify an absolute base path (begins with /) as this would
	 * be interpreted as an absolute path in the file system
	 */
	public static String TEXTANALYSIS_FS_BASE = "models/textanalysis/";
	/*
	 * The model of the internal dataset that is used to persist metadata with
	 * respect to time series models.
	 */
	public static String TIMESERIES_TABLE = "TimeModels";
	/*
	 * The fileset name of the internal fileset that is used to store time series
	 * model artifacts; it is a convention that the last part of the base path is 
	 * equal to the fs name
	 */
	public static String TIMESERIES_FS = "timeseries";
	/*
	 * Note, we do NOT specify an absolute base path (begins with /) as this would
	 * be interpreted as an absolute path in the file system
	 */
	public static String TIMESERIES_FS_BASE = "models/timeseries/";

	/***** CLASSIFICATION *****/

	public static FileSet getClassificationFS(SparkPluginContext context) throws DatasetManagementException, Exception {

		if (context.datasetExists(CLASSIFICATION_FS) == false)
			throw new Exception("Fileset to store classification model artifacts does not exist.");

		FileSet fs = context.getDataset(CLASSIFICATION_FS);
		return fs;

	}

	public static FileSet getClassificationFS(SparkExecutionPluginContext context)
			throws DatasetManagementException, Exception {

		FileSet fs = context.getDataset(CLASSIFICATION_FS);
		return fs;

	}

	public static Table getClassificationTable(SparkPluginContext context) throws DatasetManagementException, Exception {

		if (context.datasetExists(CLASSIFICATION_TABLE) == false)
			throw new Exception("Table to store classification model metadata does not exist.");

		Table table = context.getDataset(CLASSIFICATION_TABLE);
		return table;

	}

	public static Table getClassificationTable(SparkExecutionPluginContext context)
			throws DatasetManagementException, Exception {

		Table table = context.getDataset(CLASSIFICATION_TABLE);
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

		if (context.datasetExists(CLASSIFICATION_TABLE) == false) {
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
					"This table contains a timeseries of metadata for ML classification models.");
			builder.setSchema(metaSchema);

			context.createDataset(CLASSIFICATION_TABLE, Table.class.getName(), builder.build());

		}
		

	}

	/***** CLUSTERING *****/

	public static FileSet getClusteringFS(SparkPluginContext context) throws DatasetManagementException, Exception {

		if (context.datasetExists(CLUSTERING_FS) == false)
			throw new Exception("Fileset to store clustering model artifacts does not exist.");

		FileSet fs = context.getDataset(CLUSTERING_FS);
		return fs;

	}

	public static FileSet getClusteringFS(SparkExecutionPluginContext context)
			throws DatasetManagementException, Exception {

		FileSet fs = context.getDataset(CLUSTERING_FS);
		return fs;

	}

	public static Table getClusteringTable(SparkPluginContext context) throws DatasetManagementException, Exception {

		if (context.datasetExists(CLUSTERING_TABLE) == false)
			throw new Exception("Table to store clustering model metadata does not exist.");

		Table table = context.getDataset(CLUSTERING_TABLE);
		return table;

	}

	public static Table getClusteringTable(SparkExecutionPluginContext context)
			throws DatasetManagementException, Exception {

		Table table = context.getDataset(CLUSTERING_TABLE);
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

		if (context.datasetExists(CLUSTERING_TABLE) == false) {
			/*
			 * This is the first time, that we train a clustering model; therefore, the
			 * associated metadata dataset has to be created
			 */
			Schema metaSchema = createClusteringSchema();
			/*
			 * Create a CDAP table with the schema provided
			 */
			TableProperties.Builder builder = TableProperties.builder();
			builder.setDescription(
					"This table contains a timeseries of metadata for ML clustering models.");
			builder.setSchema(metaSchema);

			context.createDataset(CLUSTERING_TABLE, Table.class.getName(), builder.build());

		}
		

	}

	/***** FEATURES *****/

	public static FileSet getFeatureFS(SparkPluginContext context) throws DatasetManagementException, Exception {

		if (context.datasetExists(FEATURE_FS) == false)
			throw new Exception("Fileset to store feature model artifacts does not exist.");

		FileSet fs = context.getDataset(FEATURE_FS);
		return fs;

	}

	public static FileSet getFeatureFS(SparkExecutionPluginContext context)
			throws DatasetManagementException, Exception {

		FileSet fs = context.getDataset(FEATURE_FS);
		return fs;

	}

	public static Table getFeatureTable(SparkPluginContext context) throws DatasetManagementException, Exception {

		if (context.datasetExists(FEATURE_TABLE) == false)
			throw new Exception("Table to store feature model metadata does not exist.");

		Table table = context.getDataset(FEATURE_TABLE);
		return table;

	}

	public static Table getFeatureTable(SparkExecutionPluginContext context)
			throws DatasetManagementException, Exception {

		Table table = context.getDataset(FEATURE_TABLE);
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

		if (context.datasetExists(FEATURE_TABLE) == false) {
			/*
			 * This is the first time, that we train a feature model; therefore, the
			 * associated metadata dataset has to be created
			 */
			Schema metaSchema = createCommonSchema("featureSchema");
			/*
			 * Create a CDAP table with the schema provided
			 */
			TableProperties.Builder builder = TableProperties.builder();
			builder.setDescription(
					"This table contains a timeseries of metadata for ML feature models.");
			builder.setSchema(metaSchema);

			context.createDataset(FEATURE_TABLE, Table.class.getName(), builder.build());

		}
		

	}

	/***** RECOMMENDATION *****/

	public static FileSet getRecommendationFS(SparkPluginContext context) throws DatasetManagementException, Exception {

		if (context.datasetExists(RECOMMENDATION_FS) == false)
			throw new Exception("Fileset to store recommendation model artifacts does not exist.");

		FileSet fs = context.getDataset(RECOMMENDATION_FS);
		return fs;

	}

	public static FileSet getRecommendationFS(SparkExecutionPluginContext context)
			throws DatasetManagementException, Exception {

		FileSet fs = context.getDataset(RECOMMENDATION_FS);
		return fs;

	}

	public static Table getRecommendationTable(SparkPluginContext context) throws DatasetManagementException, Exception {

		if (context.datasetExists(RECOMMENDATION_TABLE) == false)
			throw new Exception("Table to store recommendation model metadata does not exist.");

		Table table = context.getDataset(RECOMMENDATION_TABLE);
		return table;

	}

	public static Table getRecommendationTable(SparkExecutionPluginContext context)
			throws DatasetManagementException, Exception {

		Table table = context.getDataset(RECOMMENDATION_TABLE);
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

		if (context.datasetExists(RECOMMENDATION_TABLE) == false) {
			/*
			 * This is the first time, that we train a recommendation model; therefore, the
			 * associated metadata dataset has to be created: note, the (current) ALS model
			 * is evaluated by leveraging regression evaluation
			 */
			Schema metaSchema = createRegressionSchema();
			/*
			 * Create a CDAP table with the schema provided
			 */
			TableProperties.Builder builder = TableProperties.builder();
			builder.setDescription(
					"This table contains a timeseries of metadata for ML recommendation models.");
			builder.setSchema(metaSchema);

			context.createDataset(RECOMMENDATION_TABLE, Table.class.getName(), builder.build());

		}
		

	}

	/***** REGRESSION *****/

	public static FileSet getRegressionFS(SparkPluginContext context) throws DatasetManagementException, Exception {

		if (context.datasetExists(REGRESSION_FS) == false)
			throw new Exception("Fileset to store regression model artifacts does not exist.");

		FileSet fs = context.getDataset(REGRESSION_FS);
		return fs;

	}

	public static FileSet getRegressionFS(SparkExecutionPluginContext context)
			throws DatasetManagementException, Exception {

		FileSet fs = context.getDataset(REGRESSION_FS);
		return fs;

	}

	public static Table getRegressionTabl(SparkPluginContext context) throws DatasetManagementException, Exception {

		if (context.datasetExists(REGRESSION_TABLE) == false)
			throw new Exception("Table to store regression model metadata does not exist.");

		Table table = context.getDataset(REGRESSION_TABLE);
		return table;

	}

	public static Table getRegressionTable(SparkExecutionPluginContext context)
			throws DatasetManagementException, Exception {

		Table table = context.getDataset(REGRESSION_TABLE);
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

		if (context.datasetExists(REGRESSION_TABLE) == false) {
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
					"This table contains a timeseries of metadata for ML regression models.");
			builder.setSchema(metaSchema);

			context.createDataset(REGRESSION_TABLE, Table.class.getName(), builder.build());

		}
		

	}

	/***** TEXT ANALYSIS *****/

	public static FileSet getTextFS(SparkPluginContext context) throws DatasetManagementException, Exception {

		if (context.datasetExists(TEXTANALYSIS_FS) == false)
			throw new Exception("Fileset to store text analysis model artifacts does not exist.");

		FileSet fs = context.getDataset(TEXTANALYSIS_FS);
		return fs;

	}

	public static FileSet getTextFS(SparkExecutionPluginContext context)
			throws DatasetManagementException, Exception {

		FileSet fs = context.getDataset(TEXTANALYSIS_FS);
		return fs;

	}

	public static Table getTextTable(SparkPluginContext context) throws DatasetManagementException, Exception {

		if (context.datasetExists(TEXTANALYSIS_TABLE) == false)
			throw new Exception("Table to store text analysis model metadata does not exist.");

		Table table = context.getDataset(TEXTANALYSIS_TABLE);
		return table;

	}

	public static Table getTextTable(SparkExecutionPluginContext context)
			throws DatasetManagementException, Exception {

		Table table = context.getDataset(TEXTANALYSIS_TABLE);
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

		if (context.datasetExists(TEXTANALYSIS_TABLE) == false) {
			/*
			 * This is the first time, that we train a text analysis model; therefore, the
			 * associated metadata dataset has to be created
			 */
			Schema metaSchema = createCommonSchema("textSchema");
			/*
			 * Create a CDAP table with the schema provided
			 */
			TableProperties.Builder builder = TableProperties.builder();
			builder.setDescription(
					"This table contains a timeseries of metadata for text analysis models.");
			builder.setSchema(metaSchema);

			context.createDataset(TEXTANALYSIS_TABLE, Table.class.getName(), builder.build());

		}
		

	}
	
	/***** TIMESERIES *****/

	public static FileSet getTimeFS(SparkPluginContext context) throws DatasetManagementException, Exception {

		if (context.datasetExists(TIMESERIES_FS) == false)
			throw new Exception("Fileset to store time series model artifacts does not exist.");

		FileSet fs = context.getDataset(TIMESERIES_FS);
		return fs;

	}

	public static FileSet getTimeFS(SparkExecutionPluginContext context)
			throws DatasetManagementException, Exception {

		FileSet fs = context.getDataset(TIMESERIES_FS);
		return fs;

	}

	public static Table getTimeTable(SparkPluginContext context) throws DatasetManagementException, Exception {

		if (context.datasetExists(TIMESERIES_TABLE) == false)
			throw new Exception("Table to store time series model metadata does not exist.");

		Table table = context.getDataset(TIMESERIES_TABLE);
		return table;

	}

	public static Table getTimesTable(SparkExecutionPluginContext context)
			throws DatasetManagementException, Exception {

		Table table = context.getDataset(TIMESERIES_TABLE);
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

		if (context.datasetExists(TIMESERIES_TABLE) == false) {
			/*
			 * This is the first time, that we train a timeseries model; therefore, the
			 * associated metadata dataset has to be created
			 */
			Schema metaSchema = createCommonSchema("timeseriesSchema");
			/*
			 * Create a CDAP table with the schema provided
			 */
			TableProperties.Builder builder = TableProperties.builder();
			builder.setDescription(
					"This table contains a timeseries of metadata for time series models.");
			builder.setSchema(metaSchema);

			context.createDataset(TIMESERIES_TABLE, Table.class.getName(), builder.build());

		}

	}
	/****************************************
	 * 
	 * 			SCHEMA DEFINITIONS
	 * 
	 * CDAP tables do not require a fixed row schema; 
	 * however, as we offer the tables via REST interface, 
	 * a fixed schema is defined
	 */
	
	private static List<Schema.Field> getSharedFields() {
		
		List<Schema.Field> fields = new ArrayList<>();
		/*
		 * The timestamp this version of the model has been 
		 * created; the timestamp is used as a foundation for
		 * metric timeseries
		 */
		fields.add(Schema.Field.of("timestamp", Schema.of(Schema.Type.LONG)));
		/*
		 * The unique identifier of a certain machine intelligence
		 * model; this identifier is is used as a model reference
		 * for each computed prediction. 
		 * 
		 * This enables to build a common predictive graph
		 */
		fields.add(Schema.Field.of("id", Schema.of(Schema.Type.STRING)));		
		/*
		 * The name of a certain ML model
		 */
		fields.add(Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
		/*
		 * The version of a certain ML model
		 */
		fields.add(Schema.Field.of("version", Schema.of(Schema.Type.STRING)));
		/*
		 * The fileset name of the artifacts of certain ML model; 
		 * artifacts are persisted leveraging Apache Spark's internal 
		 * mechanism backed by CDAP's fileset API
		 */
		fields.add(Schema.Field.of("fsName", Schema.of(Schema.Type.STRING)));
		fields.add(Schema.Field.of("fsPath", Schema.of(Schema.Type.STRING)));		
		/*
		 * The package of a certain ML model, e.g. WorksML, WorksTs;
		 */
		fields.add(Schema.Field.of("pack", Schema.of(Schema.Type.STRING)));		
		/*
		 * The stage of a certain ML model, e.g. Staging or Production;
		 * this is an indicator describe the life cycle stage of a model
		 */
		fields.add(Schema.Field.of("stage", Schema.of(Schema.Type.STRING)));		
		/*
		 * The algorithm of a certain ML model
		 */
		fields.add(Schema.Field.of("algorithm", Schema.of(Schema.Type.STRING)));
		/*
		 * The parameters of a certain ML model; this is a JSON object that contains the
		 * parameter set that has been used to train a certain model instance
		 */
		fields.add(Schema.Field.of("params", Schema.of(Schema.Type.STRING)));
		
		return fields;
		
	}
	
	private static Schema createCommonSchema(String name) {

		List<Schema.Field> fields = new ArrayList<>();
		/* 
		 * Append shared field to the common schema 
		 */
		fields.addAll(getSharedFields());
		/*
		 * The metrics of a certain ML model; for common models
		 * this is specified as a JSON object 
		 */
		fields.add(Schema.Field.of("metrics", Schema.of(Schema.Type.STRING)));

		Schema schema = Schema.recordOf(name, fields);
		return schema;

	}
	/*
	 * This method defines the metadata schema for classification
	 * models; in contrast to the common schema, classification
	 * specific metrics are explicitly defined as schema fields
	 */
	private static Schema createClassificationSchema() {

		String schemaName = "classificationSchema";
		List<Schema.Field> fields = new ArrayList<>();		
		/* 
		 * Append shared field to the classification schema 
		 */
		fields.addAll(getSharedFields());
		/*
		 * The calculcated metric values for this classification model; 
		 * currently the following metrics are supported:
		 * 
	     * - accuracy
		 * - f1
		 * - weightedFMeasure
		 * - weightedPrecision
		 * - weightedRecall
		 * - weightedFalsePositiveRate
		 * - weightedTruePositiveRate
 		 */
		fields.add(Schema.Field.of(Names.ACCURACY, Schema.of(Schema.Type.DOUBLE)));
		fields.add(Schema.Field.of(Names.F1, Schema.of(Schema.Type.DOUBLE)));
		fields.add(Schema.Field.of(Names.WEIGHTED_FMEASURE, Schema.of(Schema.Type.DOUBLE)));
		fields.add(Schema.Field.of(Names.WEIGHTED_PRECISION, Schema.of(Schema.Type.DOUBLE)));
		fields.add(Schema.Field.of(Names.WEIGHTED_RECALL, Schema.of(Schema.Type.DOUBLE)));
		fields.add(Schema.Field.of(Names.WEIGHTED_FALSE_POSITIVE, Schema.of(Schema.Type.DOUBLE)));
		fields.add(Schema.Field.of(Names.WEIGHTED_TRUE_POSITIVE, Schema.of(Schema.Type.DOUBLE)));		

		Schema schema = Schema.recordOf(schemaName, fields);
		return schema;

	}

	/*
	 * This method defines the metadata schema for clustering
	 * models; in contrast to the common schema, clustering
	 * specific metrics are explicitly defined as schema fields
	 */
	private static Schema createClusteringSchema() {

		String schemaName = "clusteringSchema";
		List<Schema.Field> fields = new ArrayList<>();
		/* 
		 * Append shared field to the clustering schema 
		 */
		fields.addAll(getSharedFields());
		/*
		 * The calculcated metric values for this clustering model; 
		 * currently the following metrics are supported:
		 *  
		 *  - silhouette_euclidean
		 *  - silhouette_cosine
		 *  - perplexity
		 *  - likelihood
		 * 
		 */
		fields.add(Schema.Field.of("silhouette_euclidean", Schema.of(Schema.Type.DOUBLE)));
		fields.add(Schema.Field.of("silhouette_cosine", Schema.of(Schema.Type.DOUBLE)));
		fields.add(Schema.Field.of("perplexity", Schema.of(Schema.Type.DOUBLE)));
		fields.add(Schema.Field.of("likelihood", Schema.of(Schema.Type.DOUBLE)));

		Schema schema = Schema.recordOf(schemaName, fields);
		return schema;

	}

	/*
	 * This method defines the metadata schema for regression
	 * models; in contrast to the common schema, regression
	 * specific metrics are explicitly defined as schema fields
	 */
	private static Schema createRegressionSchema() {

		String schemaName = "regressionSchema";
		List<Schema.Field> fields = new ArrayList<>();
		/* 
		 * Append shared field to the regression schema 
		 */
		fields.addAll(getSharedFields());
		/*
		 * The calculcated metric values for this regression model; 
		 * currently the following metrics are supported:
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

		Schema schema = Schema.recordOf(schemaName, fields);
		return schema;

	}

}
