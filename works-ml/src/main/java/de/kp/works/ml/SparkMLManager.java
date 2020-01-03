package de.kp.works.ml;

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
import co.cask.cdap.etl.api.batch.SparkPluginContext;

/**
 * [SparkMLManager] defines the entry point for Apache Spark
 * based model management; the current version supports the
 * following Spark native models:
 * 
 * - classification
 * - clustering
 * - recommendation
 * - regression
 *
 */
public class SparkMLManager {

	/*
	 * The model of the internal dataset that is used to 
	 * persist metadata with respect to classification models.
	 */
	public static String CLASSIFICATION_META = "classificationMeta";
	/*
	 * The fileset name of the internal fileset that is
	 * used to store classification models
	 */
	public static String CLASSIFICATION_FS = "classificationFs";
	/*
	 * Note, we do NOT specify an absolute base path (begins with /) as 
	 * this would be interpreted as an absolute path in the file system
	 */
	public static String CLASSIFICATION_FS_BASE = "models/classification/";

	/*
	 * The model of the internal dataset that is used to 
	 * persist metadata with respect to clustering models.
	 */
	public static String CLUSTERING_META = "clusteringMeta";
	/*
	 * The fileset name of the internal fileset that is
	 * used to store clustering models
	 */
	public static String CLUSTERING_FS = "clusteringFs";
	/*
	 * Note, we do NOT specify an absolute base path (begins with /) as 
	 * this would be interpreted as an absolute path in the file system
	 */
	public static String CLUSTERING_FS_BASE = "models/clustering/";
	/*
	 * The model of the internal dataset that is used to 
	 * persist metadata with respect to recommendation models.
	 */
	public static String RECOMMENDATION_META = "recommendationMeta";
	/*
	 * The fileset name of the internal fileset that is
	 * used to store recommendation models
	 */
	public static String RECOMMENDATION_FS = "recommendationFs";
	/*
	 * Note, we do NOT specify an absolute base path (begins with /) as 
	 * this would be interpreted as an absolute path in the file system
	 */
	public static String RECOMMENDATION_FS_BASE = "models/recommendation/";
	/*
	 * The model of the internal dataset that is used to 
	 * persist metadata with respect to regression models.
	 */
	public static String REGRESSION_META = "regressionMeta";
	/*
	 * The fileset name of the internal fileset that is
	 * used to store regression models
	 */
	public static String REGRESSION_FS = "regressionFs";
	/*
	 * Note, we do NOT specify an absolute base path (begins with /) as 
	 * this would be interpreted as an absolute path in the file system
	 */
	public static String REGRESSION_FS_BASE = "models/regression/";

	/***** CLASSIFICATION *****/
	
	public static FileSet getClassificationFS(SparkPluginContext context) throws DatasetManagementException, Exception {
		
		if (context.datasetExists(CLASSIFICATION_FS) == false)
			throw new Exception("Fileset to store classification model components does not exist.");
		
		FileSet fs = context.getDataset(CLASSIFICATION_FS);
		return fs;
		
	}

	public static Table getClassificationMeta(SparkPluginContext context) throws DatasetManagementException, Exception {
		
		if (context.datasetExists(CLASSIFICATION_META) == false)
			throw new Exception("Table to store classification model metadata does not exist.");
		
		Table table = context.getDataset(CLASSIFICATION_META);
		return table;
		
	}

	public static void createClassificationIfNotExists(SparkPluginContext context) throws DatasetManagementException {
		
		if (context.datasetExists(CLASSIFICATION_FS) == false) {
			/*
			 * The classification fileset does not exist yet; this path is relative to the
			 * data directory of the CDAP namespace in which the FileSet is created. 
			 * 
			 * Note, we do NOT specify an absolute base path (begins with /) as this
			 * would be interpreted as an absolute path in the file system
			 */
			context.createDataset(CLASSIFICATION_FS, FileSet.class.getName(), FileSetProperties.builder().setBasePath(CLASSIFICATION_FS_BASE).build());			
		}
		
		if (context.datasetExists(CLASSIFICATION_META) == false) {
			/*
			 * This is the first time, that we train a classification model;
			 * therefore, the associated metadata dataset has to be created
			 */
			Schema metaSchema = createMetaSchema("classificationSchema");
			/*
			 * Create a CDAP table with the schema provided
			 */
			TableProperties.Builder builder = TableProperties.builder();
			builder.setDescription("This table contains a timeseries of metadata information about Predictive Works classification models.");
			builder.setSchema(metaSchema);
			
			context.createDataset(CLASSIFICATION_META, Table.class.getName(), builder.build());
			
		};
		
	}

	/***** CLUSTERING *****/

	public static FileSet getClusteringFS(SparkPluginContext context) throws DatasetManagementException, Exception {
		
		if (context.datasetExists(CLUSTERING_FS) == false)
			throw new Exception("Fileset to store clustering model components does not exist.");
		
		FileSet fs = context.getDataset(CLUSTERING_FS);
		return fs;
		
	}

	public static Table getClusteringMeta(SparkPluginContext context) throws DatasetManagementException, Exception {
		
		if (context.datasetExists(CLUSTERING_META) == false)
			throw new Exception("Table to store clustering model metadata does not exist.");
		
		Table table = context.getDataset(CLUSTERING_META);
		return table;
		
	}

	public static void createClusteringIfNotExists(SparkPluginContext context) throws DatasetManagementException {
		
		if (context.datasetExists(CLUSTERING_FS) == false) {
			/*
			 * The clustering fileset does not exist yet; this path is relative to the
			 * data directory of the CDAP namespace in which the FileSet is created. 
			 * 
			 * Note, we do NOT specify an absolute base path (begins with /) as this
			 * would be interpreted as an absolute path in the file system
			 */
			context.createDataset(CLUSTERING_FS, FileSet.class.getName(), FileSetProperties.builder().setBasePath(CLUSTERING_FS_BASE).build());			
		}
		
		if (context.datasetExists(CLUSTERING_META) == false) {
			/*
			 * This is the first time, that we train a clustering model;
			 * therefore, the associated metadata dataset has to be created
			 */
			Schema metaSchema = createMetaSchema("clusteringSchema");
			/*
			 * Create a CDAP table with the schema provided
			 */
			TableProperties.Builder builder = TableProperties.builder();
			builder.setDescription("This table contains a timeseries of metadata information about Predictive Works clustering models.");
			builder.setSchema(metaSchema);
			
			context.createDataset(CLUSTERING_META, Table.class.getName(), builder.build());
			
		};
		
	}

	/***** RECOMMENDATION *****/

	public static FileSet getRecommendationFS(SparkPluginContext context) throws DatasetManagementException, Exception {
		
		if (context.datasetExists(RECOMMENDATION_FS) == false)
			throw new Exception("Fileset to store recommendation model components does not exist.");
		
		FileSet fs = context.getDataset(RECOMMENDATION_FS);
		return fs;
		
	}

	public static Table getRecommendationMeta(SparkPluginContext context) throws DatasetManagementException, Exception {
		
		if (context.datasetExists(RECOMMENDATION_META) == false)
			throw new Exception("Table to store recommendation model metadata does not exist.");
		
		Table table = context.getDataset(RECOMMENDATION_META);
		return table;
		
	}

	public static void createRecommendationIfNotExists(SparkPluginContext context) throws DatasetManagementException {
		
		if (context.datasetExists(RECOMMENDATION_FS) == false) {
			/*
			 * The recommendation fileset does not exist yet; this path is relative to the
			 * data directory of the CDAP namespace in which the FileSet is created. 
			 * 
			 * Note, we do NOT specify an absolute base path (begins with /) as this
			 * would be interpreted as an absolute path in the file system
			 */
			context.createDataset(RECOMMENDATION_FS, FileSet.class.getName(), FileSetProperties.builder().setBasePath(RECOMMENDATION_FS_BASE).build());			
		}
		
		if (context.datasetExists(RECOMMENDATION_META) == false) {
			/*
			 * This is the first time, that we train a recommendation model;
			 * therefore, the associated metadata dataset has to be created
			 */
			Schema metaSchema = createMetaSchema("recommendationSchema");
			/*
			 * Create a CDAP table with the schema provided
			 */
			TableProperties.Builder builder = TableProperties.builder();
			builder.setDescription("This table contains a timeseries of metadata information about Predictive Works recommendation models.");
			builder.setSchema(metaSchema);
			
			context.createDataset(RECOMMENDATION_META, Table.class.getName(), builder.build());
			
		};
		
	}

	/***** REGRESSION *****/

	public static FileSet getRegressionFS(SparkPluginContext context) throws DatasetManagementException, Exception {
		
		if (context.datasetExists(REGRESSION_FS) == false)
			throw new Exception("Fileset to store regression model components does not exist.");
		
		FileSet fs = context.getDataset(REGRESSION_FS);
		return fs;
		
	}

	public static Table getRegressionMeta(SparkPluginContext context) throws DatasetManagementException, Exception {
		
		if (context.datasetExists(REGRESSION_META) == false)
			throw new Exception("Table to store regression model metadata does not exist.");
		
		Table table = context.getDataset(REGRESSION_META);
		return table;
		
	}

	public static void createRegressionIfNotExists(SparkPluginContext context) throws DatasetManagementException {
		
		if (context.datasetExists(REGRESSION_FS) == false) {
			/*
			 * The regression fileset does not exist yet; this path is relative to the
			 * data directory of the CDAP namespace in which the FileSet is created. 
			 * 
			 * Note, we do NOT specify an absolute base path (begins with /) as this
			 * would be interpreted as an absolute path in the file system
			 */
			context.createDataset(REGRESSION_FS, FileSet.class.getName(), FileSetProperties.builder().setBasePath(REGRESSION_FS_BASE).build());			
		}
		
		if (context.datasetExists(REGRESSION_META) == false) {
			/*
			 * This is the first time, that we train a regression model;
			 * therefore, the associated metadata dataset has to be created
			 */
			Schema metaSchema = createMetaSchema("regressionSchema");
			/*
			 * Create a CDAP table with the schema provided
			 */
			TableProperties.Builder builder = TableProperties.builder();
			builder.setDescription("This table contains a timeseries of metadata information about Predictive Works regression models.");
			builder.setSchema(metaSchema);
			
			context.createDataset(REGRESSION_META, Table.class.getName(), builder.build());
			
		};
		
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
		 * The metrics of a certain ML model; this is a JSON object
		 * that contains metric data of a certain model instance
		 */
		fields.add(Schema.Field.of("metrics", Schema.of(Schema.Type.STRING)));
		/*
		 * The parameters of a certain ML model; this is a JSON object
		 * that contains the parameter set that has been used to train
		 * a certain model instance
		 */
		fields.add(Schema.Field.of("params", Schema.of(Schema.Type.STRING)));
		/*
		 * The fileset name of a certain ML model; the model itself is
		 * persisted leveraging Apache Spark's internal mechanism backed
		 * by CDAP's fileset API
		 */
		fields.add(Schema.Field.of("fsName", Schema.of(Schema.Type.STRING)));
		fields.add(Schema.Field.of("fsPath", Schema.of(Schema.Type.STRING)));
		
		Schema schema = Schema.recordOf(name, fields);
		return schema;
		
	}
	
}
