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

import com.google.gson.JsonObject;
import de.kp.works.vs.config.VisualConfig;
import de.kp.works.vs.http.HttpClient;

import java.io.IOException;

public class Publisher {

    private final VisualConfig config;
    private final HttpClient httpClient;

    public Publisher(VisualConfig config) {
        this.config = config;
        this.httpClient = new HttpClient(config);
    }

    public void publish(String algoName, String reducer, String filePath) throws IOException {

        JsonObject request = new JsonObject();
        request.addProperty("algoName", algoName);
        request.addProperty("modelName", config.modelName);

        request.addProperty("modelStage", config.modelStage);
        request.addProperty("modelOption", config.modelOption);

        request.addProperty("reducer", reducer);
        request.addProperty("filePath", filePath);

        httpClient.execute(config.serverUrl, request.toString());

    }

}
