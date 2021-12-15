package de.kp.works.core.model;
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
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.table.Put;

import java.security.MessageDigest;
import java.util.Arrays;

public class ModelSpec {

    private Long ts;
    private String algoName;

    private String modelNS;
    private String modelName;
    private String modelPack;
    private String modelStage;
    private String modelVersion;
    private String modelParams;
    private String modelMetrics;

    private String fsName;
    private String fsPath;

    public String getFsName() {
        return fsName;
    }

    public void setFsName(String fsName) {
        this.fsName = fsName;
    }

    public String getFsPath() {
        return fsPath;
    }

    public void setFsPath(String fsPath) {
        this.fsPath = fsPath;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public String getAlgoName() {
        return algoName;
    }

    public void setAlgoName(String algoName) {
        this.algoName = algoName;
    }

    public String getModelNS() {
        return modelNS;
    }

    public void setModelNS(String modelNS) {
        this.modelNS = modelNS;
    }

    public String getModelName() {
        return modelName;
    }

    public void setModelName(String modelName) {
        this.modelName = modelName;
    }

    public String getModelPack() {
        return modelPack;
    }

    public void setModelPack(String modelPack) {
        this.modelPack = modelPack;
    }

    public String getModelStage() {
        return modelStage;
    }

    public void setModelStage(String modelStage) {
        this.modelStage = modelStage;
    }

    public String getModelVersion() {
        return modelVersion;
    }

    public void setModelVersion(String modelVersion) {
        this.modelVersion = modelVersion;
    }

    public String getModelParams() {
        return modelParams;
    }

    public void setModelParams(String modelParams) {
        this.modelParams = modelParams;
    }

    public String getModelMetrics() {
        return modelMetrics;
    }

    public void setModelMetrics(String modelMetrics) {
        this.modelMetrics = modelMetrics;
    }

    public Put buildRow() {

        byte[] key = Bytes.toBytes(ts);
        /*
         * Build unique model identifier from all information
         * that is available for a certain model
         */
        String mid;
        try {
            String[] parts = {
                    String.valueOf(ts),
                    modelName,
                    modelVersion,
                    fsName,
                    fsPath,
                    modelPack,
                    modelStage,
                    algoName};

            String serialized = String.join("|", parts);
            mid = Arrays.toString(MessageDigest.getInstance("MD5").digest(serialized.getBytes()));

        } catch (Exception e) {
            mid = String.valueOf(ts);

        }

        return new Put(key)
                .add(Names.TIMESTAMP, ts)
                .add(Names.ID,        mid)
                .add(Names.NAMESPACE, modelNS)
                .add(Names.NAME,      modelName)
                .add(Names.VERSION,   modelVersion)
                .add(Names.FS_NAME,   fsName)
                .add(Names.FS_PATH,   fsPath)
                .add(Names.PACK,      modelPack)
                .add(Names.STAGE,     modelStage)
                .add(Names.ALGORITHM, algoName)
                .add(Names.PARAMS,    modelParams);

    }
}
