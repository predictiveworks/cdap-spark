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

import de.kp.works.core.Names;
import de.kp.works.core.model.ModelProfile;
import de.kp.works.core.model.ModelScanner;
import de.kp.works.core.model.ModelSpec;
import de.kp.works.core.recording.SparkMLManager;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.api.dataset.table.Row;
import io.cdap.cdap.api.dataset.table.Scanner;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkPluginContext;

public class CDAPWriter extends MetadataWriter {

    private final String algoType;
    public CDAPWriter(String algoType) {
        this.algoType = algoType;
    }

    @Override
    public String buildModelPath(SparkExecutionPluginContext context, String fsPath) throws Exception {
       FileSet fs = getFileSet(context);
       return fs.getBaseLocation().append(fsPath).toURI().getPath();
    }

    @Override
    public String buildModelPath(SparkPluginContext context, String fsPath) throws Exception {
        FileSet fs = getFileSet(context);
        return fs.getBaseLocation().append(fsPath).toURI().getPath();
    }

    @Override
    public ModelProfile getBestProfile(SparkExecutionPluginContext context, String algoName,
                                       String modelName, String modelStage) throws Exception {

        ModelScanner scanner = new ModelScanner();

        ModelProfile profile = scanner.bestProfile(context, algoType, algoName, modelName, modelStage);
        if (profile == null)
            profile = getLatestProfile(context, algoName, modelName, modelStage);

        return profile;

    }

    @Override
    public ModelProfile getBestProfile(SparkPluginContext context, String algoName,
                                       String modelName, String modelStage) throws Exception {

        ModelScanner scanner = new ModelScanner();

        ModelProfile profile = scanner.bestProfile(context, algoType, algoName, modelName, modelStage);
        if (profile == null)
            profile = getLatestProfile(context, algoName, modelName, modelStage);

        return profile;

    }

    @Override
    public ModelProfile getLatestProfile(SparkExecutionPluginContext context,
                                         String algoName, String modelName, String modelStage) throws Exception {

        Table table = getTable(context);
        return getLatestProfile(table, algoName, modelName, modelStage);

    }

    @Override
    public ModelProfile getLatestProfile(SparkPluginContext context,
                                         String algoName, String modelName, String modelStage) throws Exception {

        Table table = getTable(context);
        return getLatestProfile(table, algoName, modelName, modelStage);

    }

    private ModelProfile getLatestProfile(Table table, String algoName, String modelName, String modelStage) {

        ModelProfile profile = null;

        Row row;
        /*
         * Scan through all baseline models and determine the latest fileset path
         */
        Scanner rows = table.scan(null, null);
        while ((row = rows.next()) != null) {

            String algorithm = row.getString(Names.ALGORITHM);

            assert algorithm != null;
            if (algorithm.equals(algoName)) {

                String name = row.getString(Names.NAME);

                assert name != null;
                if (name.equals(modelName)) {

                    String stage = row.getString(Names.STAGE);

                    assert stage != null;
                    if (stage.equals(modelStage))
                        profile = new ModelProfile()
                                .setId(row.getString(Names.ID))
                                .setPath(row.getString(Names.FS_PATH));

                }
            }

        }

        return profile;

    }

    @Override
    public void setMetadata(SparkExecutionPluginContext context, ModelSpec modelSpec) throws Exception {

        String fsName = getFileName(algoType);
        modelSpec.setFsName(fsName);

        String modelNS = context.getNamespace();
        modelSpec.setModelNS(modelNS);

        Table table = getTable(context);
        String modelVersion = getLatestVersion(
                table,
                modelSpec.getAlgoName(),
                modelSpec.getModelNS(),
                modelSpec.getModelName(),
                modelSpec.getModelStage());

        modelSpec.setModelVersion(modelVersion);

        Put row = modelSpec.buildRow();
        addMetrics(algoType, modelSpec.getAlgoName(), modelSpec.getModelMetrics(), row);

        table.put(row);

    }

    public String getLatestVersion(Table table, String algoName, String modelNS, String modelName, String modelStage) {

        String strVersion = null;

        Row row;
        /*
         * Scan through all baseline models and determine the latest version
         * of the model with the same name
         */
        Scanner rows = table.scan(null, null);
        while ((row = rows.next()) != null) {

            String algorithm = row.getString(Names.ALGORITHM);
            assert algorithm != null;
            if (algorithm.equals(algoName)) {

                String name = row.getString(Names.NAME);

                assert name != null;
                if (name.equals(modelName)) {

                    String namespace = row.getString(Names.NAMESPACE);

                    assert namespace != null;
                    if (namespace.equals(modelNS)) {

                        String stage = row.getString(Names.STAGE);

                        assert stage != null;
                        if (stage.equals(modelStage))
                            strVersion = row.getString(Names.VERSION);

                    }
                }
            }
        }

        if (strVersion == null) {
            return "M-1";

        } else {

            String[] tokens = strVersion.split("-");
            int numVersion = Integer.parseInt(tokens[1]) + 1;

            return Integer.toString(numVersion);
        }

    }

    private FileSet getFileSet(SparkExecutionPluginContext context) throws Exception {

        FileSet fs;
        switch (algoType) {
            case SparkMLManager.CLASSIFIER: {
                fs = SparkMLManager.getClassificationFS(context);
                break;
            }
            case SparkMLManager.CLUSTER: {
                fs = SparkMLManager.getClusteringFS(context);
                break;
            }
            case SparkMLManager.FEATURE: {
                fs = SparkMLManager.getFeatureFS(context);
                break;
            }
            case SparkMLManager.RECOMMENDER: {
                fs = SparkMLManager.getRecommendationFS(context);
                break;
            }
            case SparkMLManager.REGRESSOR: {
                fs = SparkMLManager.getRegressionFS(context);
                break;
            }
            case SparkMLManager.TEXT: {
                fs = SparkMLManager.getTextFS(context);
                break;
            }
            case SparkMLManager.TIME: {
                fs = SparkMLManager.getTimeFS(context);
                break;
            }
            default:
                throw new Exception(String.format("The algorithm type '%s' is not supported.", algoType));

        }

        return fs;

    }

    private FileSet getFileSet(SparkPluginContext context) throws Exception {

        FileSet fs;
        switch (algoType) {
            case SparkMLManager.CLASSIFIER: {
                fs = SparkMLManager.getClassificationFS(context);
                break;
            }
            case SparkMLManager.CLUSTER: {
                fs = SparkMLManager.getClusteringFS(context);
                break;
            }
            case SparkMLManager.FEATURE: {
                fs = SparkMLManager.getFeatureFS(context);
                break;
            }
            case SparkMLManager.RECOMMENDER: {
                fs = SparkMLManager.getRecommendationFS(context);
                break;
            }
            case SparkMLManager.REGRESSOR: {
                fs = SparkMLManager.getRegressionFS(context);
                break;
            }
            case SparkMLManager.TEXT: {
                fs = SparkMLManager.getTextFS(context);
                break;
            }
            case SparkMLManager.TIME: {
                fs = SparkMLManager.getTimeFS(context);
                break;
            }
            default:
                throw new Exception(String.format("The algorithm type '%s' is not supported.", algoType));

        }

        return fs;

    }

    private Table getTable(SparkExecutionPluginContext context) throws Exception {

        Table table;
        switch (algoType) {
            case SparkMLManager.CLASSIFIER: {
                table = context.getDataset(SparkMLManager.CLASSIFICATION_TABLE);
                break;
            }
            case SparkMLManager.CLUSTER: {
                table = context.getDataset(SparkMLManager.CLUSTERING_TABLE);
                break;
            }
            case SparkMLManager.FEATURE: {
                table = context.getDataset(SparkMLManager.FEATURE_TABLE);
                break;
            }
            case SparkMLManager.RECOMMENDER: {
                table = context.getDataset(SparkMLManager.RECOMMENDATION_TABLE);
                break;
            }
            case SparkMLManager.REGRESSOR: {
                table = context.getDataset(SparkMLManager.REGRESSION_TABLE);
                break;
            }
            case SparkMLManager.TEXT: {
                table = context.getDataset(SparkMLManager.TEXTANALYSIS_TABLE);
                break;
            }
            case SparkMLManager.TIME: {
                table = context.getDataset(SparkMLManager.TIMESERIES_TABLE);
                break;
            }
            default:
                throw new Exception(String.format("The algorithm type '%s' is not supported.", algoType));

        }

        return table;

    }

    private Table getTable(SparkPluginContext context) throws Exception {

        Table table;
        switch (algoType) {
            case SparkMLManager.CLASSIFIER: {
                table = context.getDataset(SparkMLManager.CLASSIFICATION_TABLE);
                break;
            }
            case SparkMLManager.CLUSTER: {
                table = context.getDataset(SparkMLManager.CLUSTERING_TABLE);
                break;
            }
            case SparkMLManager.FEATURE: {
                table = context.getDataset(SparkMLManager.FEATURE_TABLE);
                break;
            }
            case SparkMLManager.RECOMMENDER: {
                table = context.getDataset(SparkMLManager.RECOMMENDATION_TABLE);
                break;
            }
            case SparkMLManager.REGRESSOR: {
                table = context.getDataset(SparkMLManager.REGRESSION_TABLE);
                break;
            }
            case SparkMLManager.TEXT: {
                table = context.getDataset(SparkMLManager.TEXTANALYSIS_TABLE);
                break;
            }
            case SparkMLManager.TIME: {
                table = context.getDataset(SparkMLManager.TIMESERIES_TABLE);
                break;
            }
            default:
                throw new Exception(String.format("The algorithm type '%s' is not supported.", algoType));

        }

        return table;

    }

}
