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
import de.kp.works.core.recording.clustering.LDARecorder;
import de.kp.works.vs.config.VisualConfig;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkSink;
import org.apache.spark.ml.clustering.LDAModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.List;

@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name("LDAVisor")
@Description("An Apache Spark ML based visualization plugin for Latent Dirichlet clustering.")
public class LDAVisor extends VisualSink {

    private LDAModel model;

    public LDAVisor(VisualConfig config) {
        super(config);
        this.algoName = Algorithms.LATENT_DIRICHLET_ALLOCATION;
    }

    @Override
    public void compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

        if (model == null) {

            LDARecorder recorder = new LDARecorder(configReader);
            model = recorder.read(context, config.modelName,
                    config.modelStage, config.modelOption);

            if (model == null)
                throw new IllegalArgumentException(
                        String.format("[%s] A clustering model with name '%s' does not exist.",
                        this.getClass().getName(), config.modelName));

        }
        /*
         * This stage is intended to run after the Latent Dirichlet
         * Predictor stage. This ensures that the data sources contains
         * features and a prediction column.
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
