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

public class ClusterMetric {
	/*
	 * The target variables of the model scan to 
	 * determine the best classifier model
	 */
	public String fsPath;
	public String id;
	
	public Long ts;

	public Double silhouette_euclidean;
	public Double silhouette_cosine;
	public Double perplexity;
	public Double likelihood;

	public ClusterMetric() {
		
	}

	public void fromRow(Row row) {
		
		fsPath = row.getString(Names.FS_PATH);
		id = row.getString(Names.ID);
		
		ts = row.getLong(Names.TIMESTAMP);

		silhouette_euclidean = row.getDouble(Names.SILHOUETTE_EUCLDIAN);
		silhouette_cosine = row.getDouble(Names.SILHOUETTE_COSINE);
		
		perplexity = row.getDouble(Names.PERPLEXITY);
		likelihood = row.getDouble(Names.LIKELIHOOD);
		
	}
	
}
