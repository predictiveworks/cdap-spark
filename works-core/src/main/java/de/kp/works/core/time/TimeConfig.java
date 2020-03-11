package de.kp.works.core.time;
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

import com.google.common.base.Strings;
import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.data.schema.Schema;
import de.kp.works.core.BaseConfig;
import de.kp.works.core.SchemaUtil;

public class TimeConfig extends BaseConfig {

	private static final long serialVersionUID = 3721883166765717447L;

	public static final String GROUP_COL_DESC = "The name of the field in the input schema that specifies data groups.";
	
	@Description("The name of the field in the input schema that contains the time value.")
	@Macro
	public String timeCol;

	@Description("The name of the field in the input schema that contains the value.")
	@Macro
	public String valueCol;

	public void validate() {
		super.validate();

		if (Strings.isNullOrEmpty(timeCol)) {
			throw new IllegalArgumentException(
					String.format("[%s] The name of the field that contains the time value must not be empty.",
							this.getClass().getName()));
		}
		if (Strings.isNullOrEmpty(valueCol)) {
			throw new IllegalArgumentException(
					String.format("[%s] The name of the field that contains the value must not be empty.",
							this.getClass().getName()));
		}

	}

	public void validateSchema(Schema inputSchema) {

		/** TIME COLUMN **/

		Schema.Field timeField = inputSchema.getField(timeCol);
		if (timeField == null) {
			throw new IllegalArgumentException(String.format(
					"[%s] The input schema must contain the field that defines the time value.", this.getClass().getName()));
		}

		Schema.Type timeType = getNonNullIfNullable(timeField.getSchema()).getType();
		if (SchemaUtil.isTimeType(timeType) == false) {
			throw new IllegalArgumentException("The data type of the time value field must be LONG.");
		}

		/** VALUE COLUMN **/

		Schema.Field valueField = inputSchema.getField(valueCol);
		if (valueField == null) {
			throw new IllegalArgumentException(String
					.format("[%s] The input schema must contain the field that defines the value.", this.getClass().getName()));
		}

		Schema.Type valueType = getNonNullIfNullable(valueField.getSchema()).getType();
		/*
		 * The value must be a numeric data type (double, float, int, long), which then
		 * is casted to Double (see classification trainer)
		 */
		if (SchemaUtil.isNumericType(valueType) == false) {
			throw new IllegalArgumentException("The data type of the value field must be NUMERIC.");
		}
	}
	
}
