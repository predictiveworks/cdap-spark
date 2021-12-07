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
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.plugin.common.Constants;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
/**
 * The configuration defined for the visualization
 * stage is very similar to the predictor one.
 */
public class VisualConfig extends PluginConfig {

    protected static final String MODEL_NAME_DESC = "The unique name of the ML model.";
    protected static final String MODEL_STAGE_DESC = "The stage of the ML model. Supported values are 'experiment'," +
            " 'staging', 'production' and 'archived'. Default is 'experiment'.";

    protected static final String FEATURES_COL_DESC = "The name of the field in the input schema that contains the" +
            " feature vector.";

    protected static final String PREDICTION_COL_DESC = "The name of the field in the output schema that contains the" +
            "predicted label.";

    protected static final String FOLDER_PATH_DESC = "The path to the local or distributed file system folder, that"
            + " contains parquet files for visualization.";

    protected static final String SERVER_URL_DESC = "The URL of the Works. visualization server.";

    protected static final String KEYSTORE_PATH_DESC = "A path to a file which contains the client SSL keystore.";

    protected static final String KEYSTORE_TYPE_DESC = "The format of the client SSL keystore. Supported values are 'JKS', "
            + "'JCEKS' and 'PKCS12'. Default is 'JKS'.";

    protected static final String KEYSTORE_PASS_DESC = "The password of the client SSL keystore.";

    protected static final String KEYSTORE_ALGO_DESC = "The algorithm used for the client SSL keystore. Default is 'SunX509'.";

    protected static final String TRUSTSTORE_PATH_DESC = "A path to a file which contains the client SSL truststore.";

    protected static final String TRUSTSTORE_TYPE_DESC = "The format of the client SSL truststore. Supported values are 'JKS', "
            + "'JCEKS' and 'PKCS12'. Default is 'JKS'.";

    protected static final String TRUSTSTORE_PASS_DESC = "The password of the client SSL truststore.";

    protected static final String TRUSTSTORE_ALGO_DESC = "The algorithm used for the client SSL truststore. Default is 'SunX509'.";

    protected static final String SSL_VERIFY_DESC = "An indicator to determine whether certificates have to be verified. "
            + "Supported values are 'true' and 'false'. If 'false', untrusted trust "
            + "certificates (e.g. self signed), will not lead to an error.";

    @Name(Constants.Reference.REFERENCE_NAME)
    @Description(Constants.Reference.REFERENCE_NAME_DESCRIPTION)
    public String referenceName;

    @Description(MODEL_NAME_DESC)
    @Macro
    public String modelName;

    @Description(MODEL_STAGE_DESC)
    @Macro
    public String modelStage;

    @Description(Params.MODEL_OPTION)
    @Macro
    public String modelOption;

    @Description(FEATURES_COL_DESC)
    @Macro
    public String featuresCol;

    @Description(PREDICTION_COL_DESC)
    @Macro
    public String predictionCol;

    @Description(FOLDER_PATH_DESC)
    @Macro
    public String folderPath;

    /*
     * The configuration part for connecting to
     * Works visualization server
     */
    @Description(SERVER_URL_DESC)
    @Macro
    @Nullable
    public String serverUrl;
    /*
     * Support for SSL client-server authentication, based on a set of files
     * that contain certificates and private key; this information is used to
     * create respective key and truststores for authentication
     */
    @Description(KEYSTORE_PATH_DESC)
    @Macro
    @Nullable
    public String sslKeyStorePath;

    @Description(KEYSTORE_TYPE_DESC)
    @Macro
    @Nullable
    public String sslKeyStoreType;

    @Description(KEYSTORE_PASS_DESC)
    @Macro
    @Nullable
    public String sslKeyStorePass;

    @Description(KEYSTORE_ALGO_DESC)
    @Macro
    @Nullable
    public String sslKeyStoreAlgo;

    @Description(TRUSTSTORE_PATH_DESC)
    @Macro
    @Nullable
    public String sslTrustStorePath;

    @Description(TRUSTSTORE_TYPE_DESC)
    @Macro
    @Nullable
    public String sslTrustStoreType;

    @Description(TRUSTSTORE_PASS_DESC)
    @Macro
    @Nullable
    public String sslTrustStorePass;

    @Description(TRUSTSTORE_ALGO_DESC)
    @Macro
    @Nullable
    public String sslTrustStoreAlgo;

    @Description(SSL_VERIFY_DESC)
    @Macro
    @Nullable
    public String sslVerify;

    public Boolean isSsl() {
        return !Strings.isNullOrEmpty(sslKeyStorePath);
    }

    public VisualConfig() {

        if (isSsl()) {

            sslVerify = "true";

            sslKeyStoreType = "JKS";
            sslKeyStoreAlgo = "SunX509";

            sslTrustStoreType = "JKS";
            sslTrustStoreAlgo = "SunX509";

        }

    }

    public void validate() {

        if (Strings.isNullOrEmpty(referenceName)) {
            throw new IllegalArgumentException(
                    String.format("[%s] The reference name must not be empty.", this.getClass().getName()));
        }

        if (Strings.isNullOrEmpty(modelName)) {
            throw new IllegalArgumentException(
                    String.format("[%s] The model name must not be empty.", this.getClass().getName()));
        }

        if (Strings.isNullOrEmpty(featuresCol)) {
            throw new IllegalArgumentException(
                    String.format("[%s] The name of the field that contains the feature vector must not be empty.",
                            this.getClass().getName()));
        }

        if (Strings.isNullOrEmpty(predictionCol)) {
            throw new IllegalArgumentException(String.format(
                    "[%s] The name of the field that contains the predicted label value must not be empty.",
                    this.getClass().getName()));
        }

        if (Strings.isNullOrEmpty(folderPath)) {
            throw new IllegalArgumentException(
                    String.format("[%s] The path to the parquet file folder must not be empty.", this.getClass().getName()));
        }

    }

    public void validateSchema(Schema inputSchema) {

        /* FEATURES COLUMN */

        Schema.Field featuresField = inputSchema.getField(featuresCol);
        if (featuresField == null) {
            throw new IllegalArgumentException(
                    String.format("[%s] The input schema must contain the field that defines the features.",
                            this.getClass().getName()));
        }

        SchemaUtil.isArrayOfNumeric(inputSchema, featuresCol);

    }
    /**
     * This method transforms the entire configuration
     * into a Map to reduce dependencies between Java &
     * Scala of the project
     */
    public Map<String, Object> getParamsAsMap() {
        return new HashMap<>();

    }

}
