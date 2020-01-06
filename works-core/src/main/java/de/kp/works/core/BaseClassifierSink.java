package de.kp.works.core;

import org.apache.spark.api.java.JavaRDD;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.api.batch.SparkPluginContext;
import de.kp.works.core.ml.SparkMLManager;

public class BaseClassifierSink extends BaseSink {
	
	private static final long serialVersionUID = -5552264323513756802L;

	protected FileSet modelFs;
	protected Table modelMeta;
	
	@Override
	public void prepareRun(SparkPluginContext context) throws Exception {
		/*
		 *Classification model components and metadata are persisted in a CDAP FileSet
		 * as well as a Table; at this stage, we have to make sure that these internal
		 * metadata structures are present
		 */
		SparkMLManager.createClassificationIfNotExists(context);
		/*
		 * Retrieve classification specified dataset for later use incompute
		 */
		modelFs = SparkMLManager.getClassificationFS(context);
		modelMeta = SparkMLManager.getClassificationMeta(context);
		
	}

	@Override
	public void run(SparkExecutionPluginContext context, JavaRDD<StructuredRecord> input) throws Exception {
		// TODO Auto-generated method stub
		
	}

}
