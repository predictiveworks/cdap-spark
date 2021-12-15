package de.kp.works.core.configuration;
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

public class S3Conf {

    private final String endpoint;

    private final String accessKey;
    private final String secretKey;

    public S3Conf() {
        ConfigReader reader = ConfigReader.getInstance();

        this.endpoint = reader.getS3Endpoint();
        this.accessKey = reader.getS3AccessKey();
        this.secretKey = reader.getS3SecretKey();
    }

    public String getEndpoint() {
        return this.endpoint;
    }

    public String getAccessKey() {
        return this.accessKey;
    }

    public String getSecretKey() {
        return this.secretKey;
    }

    public Boolean isEmpty() {
        return this.endpoint == null || this.accessKey == null || this.secretKey == null;
    }

    public Boolean nonEmpty() {
        return !this.isEmpty();
    }
}
