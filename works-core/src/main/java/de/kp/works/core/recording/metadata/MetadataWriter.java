package de.kp.works.core.recording.metadata;
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

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import de.kp.works.core.Algorithms;
import de.kp.works.core.Names;
import de.kp.works.core.model.ModelProfile;
import de.kp.works.core.model.ModelSpec;
import de.kp.works.core.recording.SparkMLManager;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkPluginContext;

import java.lang.reflect.Type;
import java.util.Map;

public abstract class MetadataWriter {

    protected Type metricsType = new TypeToken<Map<String, Object>>() {}.getType();

    public String[] classifierMetrics = new String[] {
        Names.ACCURACY,
        Names.F1,
        Names.WEIGHTED_FMEASURE,
        Names.WEIGHTED_PRECISION,
        Names.WEIGHTED_RECALL,
        Names.WEIGHTED_FALSE_POSITIVE,
        Names.WEIGHTED_TRUE_POSITIVE
    };

    public String[] clusterMetrics = new String[] {
        Names.SILHOUETTE_EUCLIDEAN,
        Names.SILHOUETTE_COSINE,
        Names.PERPLEXITY,
        Names.LIKELIHOOD
    };

    public String[] regressorMetrics = new String[] {
            Names.RSME,
            Names.MSE,
            Names.MAE,
            Names.R2
    };

    protected ModelProfile profile;

    public void addMetrics(String algoType, String algoName, String modelMetrics, Put row) throws Exception {

        switch (algoType) {
            case SparkMLManager.CLASSIFIER: {

                Map<String, Object> metrics = new Gson().fromJson(modelMetrics, metricsType);
                for (String metricName: classifierMetrics) {
                    Double metricValue = (Double) metrics.get(metricName);
                    row.add(metricName, metricValue);
                }

                break;
            }
            case SparkMLManager.CLUSTER: {

                Map<String, Object> metrics = new Gson().fromJson(modelMetrics, metricsType);
                for (String metricName: clusterMetrics) {
                    Double metricValue = (Double) metrics.get(metricName);
                    row.add(metricName, metricValue);
                }

                break;
            }
            case SparkMLManager.FEATURE: {
                row.add(Names.METRICS, modelMetrics);
                break;
            }
            case SparkMLManager.RECOMMENDER: {
                if (algoName.equals(Algorithms.ALS)) {

                    Map<String, Object> metrics = new Gson().fromJson(modelMetrics, metricsType);
                    for (String metricName: regressorMetrics) {
                        Double metricValue = (Double) metrics.get(metricName);
                        row.add(metricName, metricValue);
                    }

                }
                break;
            }
            case SparkMLManager.REGRESSOR: {

                Map<String, Object> metrics = new Gson().fromJson(modelMetrics, metricsType);
                for (String metricName: regressorMetrics) {
                    Double metricValue = (Double) metrics.get(metricName);
                    row.add(metricName, metricValue);
                }

            }
            case SparkMLManager.TEXT: {

                if (algoName.equals(Algorithms.VIVEKN_SENTIMENT)) {

                    Map<String, Object> metrics = new Gson().fromJson(modelMetrics, metricsType);
                    for (String metricName: regressorMetrics) {
                        Double metricValue = (Double) metrics.get(metricName);
                        row.add(metricName, metricValue);
                    }

                } else {
                    row.add(Names.METRICS, modelMetrics);
                }

            }
            case SparkMLManager.TIME: {

                if (algoName.equals(Algorithms.ACF)) {
                    row.add(Names.METRICS, modelMetrics);

                } else {

                    Map<String, Object> metrics = new Gson().fromJson(modelMetrics, metricsType);
                    for (String metricName: regressorMetrics) {
                        Double metricValue = (Double) metrics.get(metricName);
                        row.add(metricName, metricValue);
                    }

                }

            }
            default:
                throw new Exception(String.format("The algorithm type '%s' is not supported.", algoType));
        }

    }

    public String getFileName(String algoType) throws Exception {

        String fileName;
        switch (algoType) {
            case SparkMLManager.CLASSIFIER: {
                fileName = SparkMLManager.CLASSIFICATION_FS;
                break;
            }
            case SparkMLManager.CLUSTER: {
                fileName = SparkMLManager.CLUSTERING_FS;
                break;
            }
            case SparkMLManager.FEATURE: {
                fileName = SparkMLManager.FEATURE_FS;
                break;
            }
            case SparkMLManager.RECOMMENDER: {
                fileName = SparkMLManager.RECOMMENDATION_FS;
                break;
            }
            case SparkMLManager.REGRESSOR: {
                fileName = SparkMLManager.REGRESSION_FS;
                break;
            }
            case SparkMLManager.TEXT: {
                fileName = SparkMLManager.TEXTANALYSIS_FS;
                break;
            }
            case SparkMLManager.TIME: {
                fileName = SparkMLManager.TIMESERIES_FS;
                break;
            }
            default:
                throw new Exception(String.format("The algorithm type '%s' is not supported.", algoType));

        }

        return fileName;

    }

    /**
     * The retrieval of the model path is independent
     * of the respective implementation, i.e. either
     * 'cdap' or 'remote'.
     */
    public String getModelPath(SparkExecutionPluginContext context, String algoName,
                               String modelName, String modelStage, String modelOption) throws Exception {

        profile = getProfile(context, algoName, modelName, modelStage, modelOption);
        if (profile.fsPath == null) return null;

        return buildModelPath(context, profile.fsPath);

    }
    /**
     * The retrieval of the model path is independent
     * of the respective implementation, i.e. either
     * 'cdap' or 'remote'.
     */
    public String getModelPath(SparkPluginContext context, String algoName,
                               String modelName, String modelStage, String modelOption) throws Exception {

        profile = getProfile(context, algoName, modelName, modelStage, modelOption);
        if (profile.fsPath == null) return null;

        return buildModelPath(context, profile.fsPath);

    }

    public ModelProfile getProfile() {
        return profile;
    }
    /**
     * This method either retrieves the profile of the
     * best or the latest model; this profile is used
     * to extract the associated artifact path.
     */
    public ModelProfile getProfile(SparkExecutionPluginContext context, String algoName,
                                   String modelName, String modelStage, String modelOption) throws Exception {

        switch (modelOption) {
            case "best" : {
                profile = getBestProfile(context, algoName, modelName, modelStage);
                break;
            }
            case "latest" : {
                profile = getLatestProfile(context, algoName, modelName, modelStage);
                break;
            }
            default:
                throw new Exception(String.format("Model option '%s' is not supported yet.", modelOption));
        }

        return profile;

    }
    /**
     * This method either retrieves the profile of the
     * best or the latest model; this profile is used
     * to extract the associated artifact path.
     */
    public ModelProfile getProfile(SparkPluginContext context, String algoName,
                                   String modelName, String modelStage, String modelOption) throws Exception {

        switch (modelOption) {
            case "best" : {
                profile = getBestProfile(context, algoName, modelName, modelStage);
                break;
            }
            case "latest" : {
                profile = getLatestProfile(context, algoName, modelName, modelStage);
                break;
            }
            default:
                throw new Exception(String.format("Model option '%s' is not supported yet.", modelOption));
        }

        return profile;

    }

    public abstract String buildModelPath(SparkExecutionPluginContext context,
                                          String fsPath) throws Exception;

    public abstract String buildModelPath(SparkPluginContext context,
                                          String fsPath) throws Exception;

    public abstract ModelProfile getBestProfile(SparkExecutionPluginContext context, String algoName,
                                                String modelName, String modelStage) throws Exception;

    public abstract ModelProfile getBestProfile(SparkPluginContext context, String algoName, String modelName,
                                                String modelStage) throws Exception;

    public abstract ModelProfile getLatestProfile(SparkExecutionPluginContext context,
                                                  String algoName, String modelName, String modelStage) throws Exception;

    public abstract ModelProfile getLatestProfile(SparkPluginContext context,
                                                  String algoName, String modelName, String modelStage) throws Exception;

    public abstract void setMetadata(SparkExecutionPluginContext context, ModelSpec modelSpec) throws Exception;

}
