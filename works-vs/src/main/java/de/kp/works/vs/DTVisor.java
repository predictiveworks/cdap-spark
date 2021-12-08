package de.kp.works.vs;
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
import de.kp.works.core.recording.classification.DTCRecorder;
import de.kp.works.core.recording.regression.DTRRecorder;
import de.kp.works.vs.config.CRConfig;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkSink;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.regression.DecisionTreeRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.List;

@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name("DTVisor")
@Description("An Apache Spark ML based visualization plugin for Decision Tree based classification or regression.")
public class DTVisor extends VisualSink {

    private DecisionTreeClassificationModel classifier;
    private DecisionTreeRegressionModel regressor;

    public DTVisor(CRConfig config) {
        super(config);
        this.algoName = Algorithms.DECISION_TREE;
    }

    @Override
    public void compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

        if (classifier == null || regressor == null) {

            String modelType = ((CRConfig)config).modelType;

            assert modelType != null;
            if (modelType.equals("classifier")) {

                DTCRecorder recorder = new DTCRecorder();
                /*
                 * Retrieve the trained classification model
                 * that refers to the provide name, stage and option
                 */
                classifier = recorder.read(context, config.modelName, config.modelStage, config.modelOption);
                if (classifier == null)
                    throw new IllegalArgumentException(String
                            .format("[%s] A classifier model with name '%s' does not exist.", this.getClass().getName(), config.modelName));

             } else if (modelType.equals("regressor")) {

                DTRRecorder recorder = new DTRRecorder();
                /*
                 * Retrieve the trained regression model
                 * that refers to the provide name, stage and option
                 */
                regressor = recorder.read(context, config.modelName, config.modelStage, config.modelOption);
                if (regressor == null)
                    throw new IllegalArgumentException(String
                            .format("[%s] A regressor model with name '%s' does not exist.", this.getClass().getName(), config.modelName));

            } else
                throw new IllegalArgumentException(
                        String.format("[%s] The model type '%s' is not supported.", this.getClass().getName(), modelType));

        }
        /*
         * This stage is intended to run after the Decision Tree Predictor
         * stage. This ensures that the data sources contains features
         * and a prediction column.
         *
         * The following visualization use cases are supported:
         *
         * - The cartesian (2D) visualization of the features is
         *   supported, and the respective cluster (number) is used
         *   to assign different colors.
         */
        String featuresCol = config.featuresCol;
        String predictionCol = config.predictionCol;

        List<String> otherCols = new ArrayList<>();
        otherCols.add(predictionCol);

        projectAndPublish(source, featuresCol, otherCols);

    }
}
