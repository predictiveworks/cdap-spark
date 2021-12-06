package de.kp.works.vs.clustering;
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

import de.kp.works.core.ml.clustering.GaussianMixtureRecorder;
import de.kp.works.vs.VisualSink;
import de.kp.works.vs.config.VisualConfig;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkSink;
import org.apache.spark.ml.clustering.GaussianMixtureModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name("KMeansVisor")
@Description("An Apache Spark ML based visualization plugin for Gaussian Mixture clustering.")
public class GaussianMixtureVisor extends VisualSink {

    private GaussianMixtureModel model;
    private GaussianMixtureRecorder recorder;

    public GaussianMixtureVisor(VisualConfig config) {
        super(config);
    }

    @Override
    public void compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

        if (model == null || recorder == null) {

            recorder = new GaussianMixtureRecorder();
            model = recorder.read(context, config.modelName,
                    config.modelStage, config.modelOption);

            if (model == null)
                throw new IllegalArgumentException(
                        String.format("[%s] A clustering model with name '%s' does not exist.",
                                this.getClass().getName(), config.modelName));

        }
    }

}

