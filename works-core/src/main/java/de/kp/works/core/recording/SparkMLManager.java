package de.kp.works.core.recording;

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
import java.util.List;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.api.dataset.lib.FileSetProperties;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.api.dataset.table.TableProperties;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkPluginContext;
import de.kp.works.core.Names;

/**
 * [SparkMLManager] defines the entry point for Apache Spark based model
 * management; the current version supports the following Spark native models:
 * 
 * - classification 
 * - clustering 
 * - recommendation 
 * - regression 
 * - text analysis
 * - time series
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

	public static FileSet getClassificationFS(SparkPluginContext context) throws Exception {

		if (!context.datasetExists(CLASSIFICATION_FS))
			throw new Exception("Fileset to store classification model artifacts does not exist.");

		return context.getDataset(CLASSIFICATION_FS);

	}

	public static FileSet getClassificationFS(SparkExecutionPluginContext context) {
		return context.getDataset(CLASSIFICATION_FS);
	}

	public static Table getClassificationTable(SparkPluginContext context) throws Exception {

		if (!context.datasetExists(CLASSIFICATION_TABLE))
			throw new Exception("Table to store classification model metadata does not exist.");

		return context.getDataset(CLASSIFICATION_TABLE);

	}

	public static Table getClassificationTable(SparkExecutionPluginContext context) {
		return context.getDataset(CLASSIFICATION_TABLE);
	}

	public static void createClassificationIfNotExists(SparkPluginContext context) throws DatasetManagementException {

		if (!context.datasetExists(CLASSIFICATION_FS)) {
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

		if (!context.datasetExists(CLASSIFICATION_TABLE)) {
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

	public static FileSet getClusteringFS(SparkPluginContext context) throws Exception {

		if (!context.datasetExists(CLUSTERING_FS))
			throw new Exception("Fileset to store clustering model artifacts does not exist.");

		return context.getDataset(CLUSTERING_FS);

	}

	public static FileSet getClusteringFS(SparkExecutionPluginContext context) {
		return context.getDataset(CLUSTERING_FS);
	}

	public static Table getClusteringTable(SparkPluginContext context) throws Exception {

		if (!context.datasetExists(CLUSTERING_TABLE))
			throw new Exception("Table to store clustering model metadata does not exist.");

		return context.getDataset(CLUSTERING_TABLE);

	}

	public static Table getClusteringTable(SparkExecutionPluginContext context) {
		return context.getDataset(CLUSTERING_TABLE);
	}

	public static void createClusteringIfNotExists(SparkPluginContext context) throws DatasetManagementException {

		if (!context.datasetExists(CLUSTERING_FS)) {
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

		if (!context.datasetExists(CLUSTERING_TABLE)) {
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

	public static FileSet getFeatureFS(SparkPluginContext context) throws Exception {

		if (!context.datasetExists(FEATURE_FS))
			throw new Exception("Fileset to store feature model artifacts does not exist.");

		return context.getDataset(FEATURE_FS);

	}

	public static FileSet getFeatureFS(SparkExecutionPluginContext context) throws Exception {
		return context.getDataset(FEATURE_FS);
	}

	public static Table getFeatureTable(SparkPluginContext context) throws Exception {

		if (!context.datasetExists(FEATURE_TABLE))
			throw new Exception("Table to store feature model metadata does not exist.");

		return context.getDataset(FEATURE_TABLE);

	}

	public static Table getFeatureTable(SparkExecutionPluginContext context)
			throws Exception {

		return context.getDataset(FEATURE_TABLE);

	}

	public static void createFeatureIfNotExists(SparkPluginContext context) throws DatasetManagementException {

		if (!context.datasetExists(FEATURE_FS)) {
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

		if (!context.datasetExists(FEATURE_TABLE)) {
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

	public static FileSet getRecommendationFS(SparkPluginContext context) throws Exception {

		if (!context.datasetExists(RECOMMENDATION_FS))
			throw new Exception("Fileset to store recommendation model artifacts does not exist.");

		return context.getDataset(RECOMMENDATION_FS);

	}

	public static FileSet getRecommendationFS(SparkExecutionPluginContext context) {
		return context.getDataset(RECOMMENDATION_FS);
	}

	public static Table getRecommendationTable(SparkPluginContext context) throws Exception {

		if (!context.datasetExists(RECOMMENDATION_TABLE))
			throw new Exception("Table to store recommendation model metadata does not exist.");

		return context.getDataset(RECOMMENDATION_TABLE);

	}

	public static Table getRecommendationTable(SparkExecutionPluginContext context) {
		return context.getDataset(RECOMMENDATION_TABLE);
	}

	public static void createRecommendationIfNotExists(SparkPluginContext context) throws DatasetManagementException {

		if (!context.datasetExists(RECOMMENDATION_FS)) {
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

		if (!context.datasetExists(RECOMMENDATION_TABLE)) {
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

	public static FileSet getRegressionFS(SparkPluginContext context) throws Exception {

		if (!context.datasetExists(REGRESSION_FS))
			throw new Exception("Fileset to store regression model artifacts does not exist.");

		return context.getDataset(REGRESSION_FS);

	}

	public static FileSet getRegressionFS(SparkExecutionPluginContext context) {
		return context.getDataset(REGRESSION_FS);
	}

	public static Table getRegressionTable(SparkPluginContext context) throws Exception {

		if (!context.datasetExists(REGRESSION_TABLE))
			throw new Exception("Table to store regression model metadata does not exist.");

		return context.getDataset(REGRESSION_TABLE);

	}

	public static Table getRegressionTable(SparkExecutionPluginContext context) {
		return context.getDataset(REGRESSION_TABLE);
	}

	public static void createRegressionIfNotExists(SparkPluginContext context) throws DatasetManagementException {

		if (!context.datasetExists(REGRESSION_FS)) {
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

		if (!context.datasetExists(REGRESSION_TABLE)) {
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

	public static FileSet getTextFS(SparkPluginContext context) throws Exception {

		if (!context.datasetExists(TEXTANALYSIS_FS))
			throw new Exception("Fileset to store text analysis model artifacts does not exist.");

		return context.getDataset(TEXTANALYSIS_FS);

	}

	public static FileSet getTextFS(SparkExecutionPluginContext context)
			throws Exception {

		return context.getDataset(TEXTANALYSIS_FS);

	}

	public static Table getTextTable(SparkPluginContext context) throws Exception {

		if (!context.datasetExists(TEXTANALYSIS_TABLE))
			throw new Exception("Table to store text analysis model metadata does not exist.");

		return context.getDataset(TEXTANALYSIS_TABLE);

	}

	public static Table getTextTable(SparkExecutionPluginContext context)throws Exception {
		return context.getDataset(TEXTANALYSIS_TABLE);
	}

	public static void createTextAnalysisIfNotExists(SparkPluginContext context) throws DatasetManagementException {

		if (!context.datasetExists(TEXTANALYSIS_FS)) {
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

		if (!context.datasetExists(TEXTANALYSIS_TABLE)) {
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

	public static FileSet getTimeFS(SparkPluginContext context) throws Exception {

		if (!context.datasetExists(TIMESERIES_FS))
			throw new Exception("Fileset to store time series model artifacts does not exist.");

		return context.getDataset(TIMESERIES_FS);

	}

	public static FileSet getTimeFS(SparkExecutionPluginContext context) {
		return context.getDataset(TIMESERIES_FS);
	}

	public static Table getTimeTable(SparkPluginContext context) throws Exception {

		if (!context.datasetExists(TIMESERIES_TABLE))
			throw new Exception("Table to store time series model metadata does not exist.");

		return context.getDataset(TIMESERIES_TABLE);

	}

	public static Table getTimesTable(SparkExecutionPluginContext context) {
		return context.getDataset(TIMESERIES_TABLE);
	}

	public static void createTimeseriesIfNotExists(SparkPluginContext context) throws DatasetManagementException {

		if (!context.datasetExists(TIMESERIES_FS)) {
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

		if (!context.datasetExists(TIMESERIES_TABLE)) {
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
		 * This is the namespace a certain ML model refers to
		 */
		fields.add(Schema.Field.of("namespace", Schema.of(Schema.Type.STRING)));		
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

		/*
		 * Append shared field to the common schema 
		 */
		List<Schema.Field> fields = new ArrayList<>(getSharedFields());
		/*
		 * The metrics of a certain ML model; for common models
		 * this is specified as a JSON object 
		 */
		fields.add(Schema.Field.of("metrics", Schema.of(Schema.Type.STRING)));
		return Schema.recordOf(name, fields);

	}
	/*
	 * This method defines the metadata schema for classification
	 * models; in contrast to the common schema, classification
	 * specific metrics are explicitly defined as schema fields
	 */
	private static Schema createClassificationSchema() {

		String schemaName = "classificationSchema";
		/*
		 * Append shared field to the classification schema 
		 */
		List<Schema.Field> fields = new ArrayList<>(getSharedFields());
		/*
		 * The calculated metric values for this classification model;
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

		return Schema.recordOf(schemaName, fields);

	}

	/*
	 * This method defines the metadata schema for clustering
	 * models; in contrast to the common schema, clustering
	 * specific metrics are explicitly defined as schema fields
	 */
	private static Schema createClusteringSchema() {

		String schemaName = "clusteringSchema";
		/*
		 * Append shared field to the clustering schema 
		 */
		List<Schema.Field> fields = new ArrayList<>(getSharedFields());
		/*
		 * The calculated metric values for this clustering model;
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

		return Schema.recordOf(schemaName, fields);

	}

	/*
	 * This method defines the metadata schema for regression
	 * models; in contrast to the common schema, regression
	 * specific metrics are explicitly defined as schema fields
	 */
	private static Schema createRegressionSchema() {

		String schemaName = "regressionSchema";
		/*
		 * Append shared field to the regression schema 
		 */
		List<Schema.Field> fields = new ArrayList<>(getSharedFields());
		/*
		 * The calculated metric values for this regression model;
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

		return Schema.recordOf(schemaName, fields);

	}

}
