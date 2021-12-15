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

import com.google.common.base.Strings;
import de.kp.works.core.Algorithms;
import de.kp.works.core.SessionHelper;
import de.kp.works.core.configuration.ConfigReader;
import de.kp.works.vs.config.VisualConfig;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.spark.sql.DataFrames;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkPluginContext;
import io.cdap.cdap.etl.api.batch.SparkSink;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.Date;
import java.util.List;

abstract public class VisualSink extends SparkSink<StructuredRecord> {

    protected String algoName;
    protected Schema inputSchema;

    protected String reducer = "PCA";
    protected final VisualConfig config;
    /*
     * The config reader is introduced to enable
     * access to static side wide configurations
     */
    protected ConfigReader configReader = ConfigReader.getInstance();
    /*
     * This class is a CDAP wrapper for the Scala [Visualizer]
     * that performs the entire visualization of the dataset
     */
    public VisualSink(VisualConfig config) {
        this.config = config;
    }

    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
        super.configurePipeline(pipelineConfigurer);

        /* Validate configuration */
        config.validate();

        /* Validate schema */
        StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
        inputSchema = stageConfigurer.getInputSchema();
        if (inputSchema != null)
            config.validateSchema(inputSchema);

    }

    @Override
    public void run(SparkExecutionPluginContext context, JavaRDD<StructuredRecord> input) throws Exception {

        if (input.isEmpty())
            return;

        if (inputSchema == null) {
            inputSchema = input.first().getSchema();
            config.validateSchema(inputSchema);
        }

        JavaSparkContext jsc = context.getSparkContext();
        SparkSession session = new SparkSession(jsc.sc());
        /*
         * Transform JavaRDD<StructuredRecord> into Dataset<Row>
         */
        StructType structType = DataFrames.toDataType(inputSchema);
        Dataset<Row> rows = SessionHelper.toDataset(input, structType, session);
        /*
         * Apply visualization functionality and thereby
         * continue with the Works visualization library
         */
        compute(context, rows);

    }

    @Override
    public void prepareRun(SparkPluginContext context) {
    }

    public void compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {
    }

    protected void projectAndPublish(Dataset<Row> source, String featuresCol, List<String> otherCols) throws IOException {

        Dataset<Row> projected = Projector.execute(source, featuresCol, otherCols);
        /*
         * The projected dataset is a 3-column dataset, x, y, prediction.
         * This dataset is saved as *.parquet file in a local or distributed
         * file system.
         *
         * This file system is shared with PredictiveWorks. visualization
         * server that transforms the parquet file into an image.
         */
        String filePath = buildFilePath();
        projected.write().mode(SaveMode.Overwrite).parquet(filePath);

        if (!Strings.isNullOrEmpty(config.serverUrl)) {

            Publisher publisher = new Publisher(config);
            publisher.publish(algoName, reducer, filePath);

        }

    }

    protected String buildFilePath() {

        long ts = new Date().getTime();
        String fsPath = algoName + "/" + ts + "/" + config.modelName + ".parquet";

        return config.folderPath + "/" + fsPath;

    }

}
