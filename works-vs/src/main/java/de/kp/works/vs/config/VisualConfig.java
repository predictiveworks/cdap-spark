package de.kp.works.vs.config;
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

import com.google.common.base.Strings;
import de.kp.works.core.Params;
import de.kp.works.core.SchemaUtil;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;

import java.util.HashMap;
import java.util.Map;

public class VisualConfig extends PluginConfig {

    @Description("The unique name of the ML model.")
    @Macro
    public String modelName;

    @Description("The stage of the ML model. Supported values are 'experiment', 'staging', 'production' and 'archived'. Default is 'experiment'.")
    @Macro
    public String modelStage;

    @Description(Params.MODEL_OPTION)
    @Macro
    public String modelOption;

    public void validate() {

        if (Strings.isNullOrEmpty(modelName)) {
            throw new IllegalArgumentException(
                    String.format("[%s] The model name must not be empty.", this.getClass().getName()));
        }

    }

    public void validateSchema(Schema inputSchema) {
    }
    /**
     * This method transforms the entire configuration
     * into a Map to reduce dependencies between Java &
     * Scala of the project
     */
    public Map<String, Object> getParamsAsMap() {

        Map<String, Object> params = new HashMap<>();
        return params;

    }

}
