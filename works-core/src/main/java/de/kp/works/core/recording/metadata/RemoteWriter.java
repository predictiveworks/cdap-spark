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

import de.kp.works.core.Algorithms;
import de.kp.works.core.configuration.ConfigReader;
import de.kp.works.core.configuration.FsConf;
import de.kp.works.core.configuration.JdbcConf;
import de.kp.works.core.model.*;
import de.kp.works.core.recording.SparkMLManager;
import de.kp.works.core.timescale.TimeScale;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkPluginContext;

import java.util.List;

public class RemoteWriter extends MetadataWriter {

    private final String algoType;
    private final FsConf fsConf;
    /*
     * The [TimeScale] database is used to store and
     * manage the metadata of a certain machine learning
     * model and its runs.
     */
    private final TimeScale timescale;
    public RemoteWriter(String algoType) {

        this.algoType = algoType;
        this.fsConf = new FsConf();

        this.timescale = TimeScale.getInstance(new JdbcConf());

    }
    /**
     * This method retrieves the artifact (file system)
     * path from time scale request either for the latest
     * model run or the mots accurate (best) run.
     */
    @Override
    public String buildModelPath(SparkExecutionPluginContext context, String fsPath) throws Exception {
        return buildAbsolutePath(fsPath);
    }

    @Override
    public String buildModelPath(SparkPluginContext context, String fsPath) throws Exception {
        return buildAbsolutePath(fsPath);
    }

    private String buildAbsolutePath(String fsPath) throws Exception {

        String relativePath = buildRelativePath(fsPath);

        String folder = fsConf.getFolder();
        String option = fsConf.getOption();

        if (option.equals(ConfigReader.FS_OPTION)) {
            throw new Exception("The CDAP FileSet abstraction cannot be used with a remote artifact handling.");
        }

        if (option.equals(ConfigReader.HDFS_OPTION)) {
            if (!folder.startsWith("hdfs://"))
                throw new Exception("The configured HDFS base path is not valid.");
        }

        if (option.equals(ConfigReader.S3_OPTION)) {
            if (!folder.startsWith("s3a://"))
                throw new Exception("The configured S3 base path is not valid.");
        }

        if (folder.endsWith("/"))
            return folder + relativePath;

        else
            return folder + "/" + relativePath;

    }

    private String buildRelativePath(String fsPath) throws Exception {
        String modelPath;
        switch (algoType) {
            case SparkMLManager.CLASSIFIER: {
                /* models/classification/ */
                String basePath = SparkMLManager.CLASSIFICATION_FS_BASE;
                modelPath = basePath + fsPath;
                break;
            }
            case SparkMLManager.CLUSTER: {
                /* models/clustering/ */
                String basePath = SparkMLManager.CLUSTERING_FS_BASE;
                modelPath = basePath + fsPath;
                break;
            }
            case SparkMLManager.FEATURE: {
                /* models/feature/ */
                String basePath = SparkMLManager.FEATURE_FS_BASE;
                modelPath = basePath + fsPath;
                break;
            }
            case SparkMLManager.RECOMMENDER: {
                /* models/recommendation/ */
                String basePath = SparkMLManager.RECOMMENDATION_FS_BASE;
                modelPath = basePath + fsPath;
                break;
            }
            case SparkMLManager.REGRESSOR: {
                /* models/regression/ */
                String basePath = SparkMLManager.REGRESSION_FS_BASE;
                modelPath = basePath + fsPath;
                break;
            }
            case SparkMLManager.TEXT: {
                /* models/textanalysis/ */
                String basePath = SparkMLManager.TEXTANALYSIS_FS_BASE;
                modelPath = basePath + fsPath;
                break;
            }
            case SparkMLManager.TIME: {
                /* models/timeseries/ */
                String basePath = SparkMLManager.TIMESERIES_FS_BASE;
                modelPath = basePath + fsPath;
                break;
            }
            default:
                throw new Exception(String.format("The algorithm type '%s' is not supported.", algoType));

        }

        return modelPath;
    }

    @Override
    public ModelProfile getBestProfile(SparkExecutionPluginContext context, String algoName,
                                       String modelName, String modelStage) throws Exception {
        return getBestProfile(algoName, modelName, modelStage);
    }

    @Override
    public ModelProfile getBestProfile(SparkPluginContext context, String algoName,
                                       String modelName, String modelStage) throws Exception {
        return getBestProfile(algoName, modelName, modelStage);
    }
    /**
     * A helper method to retrieve the different metrics
     * of the provided algorithm, model name & stage.
     *
     * The result is sent to the [ModelFinder] to find the
     * model with the most accurate training result.
     */
    private ModelProfile getBestProfile(String algoName, String modelName, String modelStage) throws Exception {

        ModelProfile profile = null;
        switch (algoType) {
            case SparkMLManager.CLASSIFIER: {
                /*
                 * Retrieve the classifier metrics, including model
                 * identifier and file system path that refer to the
                 * algoName, modelName and modelStage
                 */
                List<ClassifierMetric> metrics = timescale
                        .getClassifierMetrics(algoName, modelName, modelStage);

                profile = ModelFinder.findClassifier(algoName, metrics);
                break;
            }
            case SparkMLManager.CLUSTER: {
                /*
                 * Retrieve the cluster metrics, including model
                 * identifier and file system path that refer to the
                 * algoName, modelName and modelStage
                 */
                List<ClusterMetric> metrics = timescale
                        .getClusterMetrics(algoName, modelName, modelStage);

                profile = ModelFinder.findCluster(algoName, metrics);
                break;
            }
            case SparkMLManager.FEATURE: {
                break;
            }
            case SparkMLManager.RECOMMENDER: {

                if (Algorithms.ALS.equals(algoName)) {
                    /*
                     * Retrieve the regressor metrics, including model
                     * identifier and file system path that refer to the
                     * algoName, modelName and modelStage
                     */
                    List<RegressorMetric> metrics = timescale
                            .getRegressorMetrics(algoName, modelName, modelStage);

                    profile = ModelFinder.findRegressor(algoName, metrics);

                }

                break;
            }
            case SparkMLManager.REGRESSOR: {
                /*
                 * Retrieve the regressor metrics, including model
                 * identifier and file system path that refer to the
                 * algoName, modelName and modelStage
                 */
                List<RegressorMetric> metrics = timescale
                        .getRegressorMetrics(algoName, modelName, modelStage);

                profile = ModelFinder.findRegressor(algoName, metrics);
                break;
            }
            case SparkMLManager.TEXT: {

                if (Algorithms.VIVEKN_SENTIMENT.equals(algoName)) {
                    /*
                     * Retrieve the regressor metrics, including model
                     * identifier and file system path that refer to the
                     * algoName, modelName and modelStage
                     */
                    List<RegressorMetric> metrics = timescale
                            .getRegressorMetrics(algoName, modelName, modelStage);

                    profile = ModelFinder.findRegressor(algoName, metrics);

                }

                break;
            }
            case SparkMLManager.TIME: {

                if 	(!Algorithms.ACF.equals(algoName)) {
                    /*
                     * Retrieve the regressor metrics, including model
                     * identifier and file system path that refer to the
                     * algoName, modelName and modelStage
                     */
                    List<RegressorMetric> metrics = timescale
                            .getRegressorMetrics(algoName, modelName, modelStage);

                    profile = ModelFinder.findRegressor(algoName, metrics);

                }

                break;
            }
            default:
                throw new Exception(String.format("The algorithm type '%s' is not supported.", algoType));

        }

        return profile;
    }

    @Override
    public ModelProfile getLatestProfile(SparkExecutionPluginContext context, String algoName, String modelName, String modelStage) {
        return timescale.getLastProfile(algoName, modelName, modelStage);
    }

    @Override
    public ModelProfile getLatestProfile(SparkPluginContext context, String algoName, String modelName, String modelStage) {
        return timescale.getLastProfile(algoName, modelName, modelStage);
    }

    @Override
    public void setMetadata(SparkExecutionPluginContext context, ModelSpec modelSpec) throws Exception {

        String fsName = getFileName(algoType);
        modelSpec.setFsName(fsName);

        String modelNS = context.getNamespace();
        modelSpec.setModelNS(modelNS);

        String modelVersion = timescale.getLastVersion(
                modelSpec.getAlgoName(),
                modelSpec.getModelNS(),
                modelSpec.getModelName(),
                modelSpec.getModelStage());

        modelSpec.setModelVersion(modelVersion);

        Put row = modelSpec.buildRow();
        addMetrics(algoType, modelSpec.getAlgoName(), modelSpec.getModelMetrics(), row);

        String algoName = modelSpec.getAlgoName();
        switch (algoType) {
            case SparkMLManager.CLASSIFIER: {
                timescale.insertClassifierRow(algoName, row);
                break;
            }
            case SparkMLManager.CLUSTER: {
                timescale.insertClusterRow(algoName, row);
                break;
            }
            case SparkMLManager.FEATURE: {
                timescale.insertPlainRow(algoName, row);
                break;
            }
            case SparkMLManager.RECOMMENDER: {

                if (Algorithms.ALS.equals(algoName))
                    timescale.insertRegressorRow(algoName, row);

                else
                    timescale.insertPlainRow(algoName, row);

                break;
            }
            case SparkMLManager.REGRESSOR: {
                timescale.insertRegressorRow(algoName, row);
                break;
            }
            case SparkMLManager.TEXT: {

                if (Algorithms.VIVEKN_SENTIMENT.equals(algoName))
                    timescale.insertRegressorRow(algoName, row);

                else
                    timescale.insertPlainRow(algoName, row);

                break;
            }
            case SparkMLManager.TIME: {

                if 	(Algorithms.ACF.equals(algoName))
                    timescale.insertPlainRow(algoName, row);

                else
                    timescale.insertRegressorRow(algoName, row);

                break;
            }
            default:
                throw new Exception(String.format("The algorithm type '%s' is not supported.", algoType));

        }

    }

}
