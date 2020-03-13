package de.kp.works.core.model;
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


import co.cask.cdap.api.dataset.table.Row;
import de.kp.works.core.Names;

public class ClassifierMetric {
	/*
	 * The target variables of the model scan to 
	 * determine the best classifier model
	 */
	public String fsPath;
	public String id;
	
	public Long ts;
	public Double accuracy;
	public Double f1;
	public Double weightedFMeasure;
	public Double weightedPrecision;
	public Double weightedRecall;
	public Double weightedFalsePositiveRate;
	public Double weightedTruePositiveRate;

	public void fromRow(Row row) {
		
		fsPath = row.getString(Names.FS_PATH);
		id = row.getString(Names.ID);

		ts = row.getLong(Names.TIMESTAMP);

		accuracy = row.getDouble(Names.ACCURACY);
		f1 = row.getDouble(Names.F1);
		
		weightedFMeasure = row.getDouble(Names.WEIGHTED_FMEASURE);
		
		weightedPrecision = row.getDouble(Names.WEIGHTED_PRECISION);
		weightedRecall = row.getDouble(Names.WEIGHTED_RECALL);
		
		weightedFalsePositiveRate = row.getDouble(Names.WEIGHTED_FALSE_POSITIVE);
		weightedTruePositiveRate = row.getDouble(Names.WEIGHTED_TRUE_POSITIVE);		
		
	}
}
