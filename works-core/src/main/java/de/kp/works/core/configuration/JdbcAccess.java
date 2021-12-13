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

public class JdbcAccess {

    private final String url;
    private final String user;
    private final String password;

    public JdbcAccess() {
        ConfigReader reader = new ConfigReader();

        this.url = reader.getJdbcUrl();
        this.user = reader.getS3AccessKey();
        this.password = reader.getJdbcPassword();
    }

    public String getUrl() {
        return this.url;
    }

    public String getUser() {
        return this.user;
    }

    public String getPassword() {
        return this.password;
    }

    public Boolean isEmpty() {
        return this.url == null || this.user == null || this.password == null;
    }

    public Boolean nonEmpty() {
        return !this.isEmpty();
    }


}
