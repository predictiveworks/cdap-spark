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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import de.kp.works.core.model.ModelScanner;

public class TextRecorder extends AbstractRecorder {

	protected String getBestModelFsPath(Table table, String algorithmName, String modelName, String modelStage) {
		
		ModelScanner scanner = new ModelScanner();
		String fsPath = scanner.bestText(table, algorithmName, modelName, modelStage);
		if (fsPath == null)
			fsPath = getLatestModelFsPath(table, algorithmName, modelName, modelStage);
		
		return fsPath;

	}

	protected void setMetadata(long ts, Table table, String algorithmName, String modelName, String modelPack,
			String modelStage, String modelParams, String modelMetrics, String fsPath) {

		String fsName = SparkMLManager.TEXTANALYSIS_FS;
		String modelVersion = getLatestModelVersion(table, algorithmName, modelName, modelStage);

		byte[] key = Bytes.toBytes(ts);
		Put row = buildRow(key, ts, modelName, modelVersion, fsName, fsPath, modelPack, modelStage, algorithmName,
				modelParams);

		table.put(row.add("metrics", modelMetrics));

	}
}
