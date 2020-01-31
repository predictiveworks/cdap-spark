package de.kp.works.ts.ar;
/*
 * Copyright (c) 2019 Dr. Krusche & Partner PartG. All rights reserved.
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

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import de.kp.works.core.TimeCompute;

public class ARCompute extends TimeCompute {

	private static final long serialVersionUID = -1934576305579477480L;

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {

		((ARComputeConfig)config).validate();

		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
		/*
		 * Try to determine input and output schema; if these schemas are not explicitly
		 * specified, they will be inferred from the provided data records
		 */
		inputSchema = stageConfigurer.getInputSchema();
		if (inputSchema != null) {
			validateSchema(inputSchema, config);
			/*
			 * In cases where the input schema is explicitly provided, we determine the
			 * output schema by explicitly adding the prediction column
			 */
			outputSchema = getOutputSchema(config.timeCol, config.valueCol);
			stageConfigurer.setOutputSchema(outputSchema);

		}

	}

	protected Schema getOutputSchema(String timeField, String valueField) {

		List<Schema.Field> fields = new ArrayList<>();
		
		fields.add(Schema.Field.of(timeField, Schema.of(Schema.Type.LONG)));
		fields.add(Schema.Field.of(valueField, Schema.of(Schema.Type.DOUBLE)));
		
		return Schema.recordOf("timeseries.forecast", fields);

	}

}
