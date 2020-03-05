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

public class RegressorMetric {
	/*
	 * The target variable of the model scan to 
	 * determine the best classifier model
	 */
	public String fsPath;
	
	public Long ts;

	public Double rsme;
	public Double mse;
	public Double mae;
	public Double r2;

	public RegressorMetric() {
		
	}

	public void fromRow(Row row) {
		
		fsPath = row.getString(Names.FS_PATH);
		ts = row.getLong(Names.TIMESTAMP);

		rsme = row.getDouble(Names.RSME);
		mse = row.getDouble(Names.MSE);
		mae = row.getDouble(Names.MAE);
		r2 = row.getDouble(Names.R2);

	}
	
}
