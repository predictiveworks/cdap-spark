package de.kp.works.core.configuration;
/*
 * Copyright (c) 2019- 2021 Dr. Krusche & Partner PartG. All rights reserved.
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

import io.cdap.cdap.common.conf.CConfiguration;

import java.util.Arrays;
import java.util.List;

public class ConfigReader {
     /*
     * Create a new configuration instance and
     * thereby read the content of 'cdap-default.xml'
     * and 'cdap-site.xml'.
     *
     * This configuration is used to retrieve the
     * global AWS (S3) configuration and also the
     * JDBC access to the metadata Postgres instance
     */
    private final CConfiguration config;
    /*
     * The property name to determine where to store
     * machine learning artifacts; the supported values
     * are
     *
     * - fs:    The CDAP FileSet abstraction
     * - hdfs:  Hadoop distributed file system
     * - lfs:   Local file system (for CDAP sandbox)
     * - s3:    AWS S3 storage system
     */

    public static final String FS_OPTION   = "fs";
    public static final String HDFS_OPTION = "hdfs";
    public static final String LFS_OPTION  = "lfs";
    public static final String S3_OPTION   = "s3";

    private final List<String> artifactOptions = Arrays.asList(FS_OPTION, HDFS_OPTION, LFS_OPTION, S3_OPTION);

    public static String MODEL_ARTIFACT_OPTION = "model.artifact.option";
    public static String MODEL_ARTIFACT_FOLDER = "model.artifact.folder";
    /*
     * The property names used to specify the AWS S3
     * endpoint and credentials
     */
    public static String S3A_ENDPOINT   = "fs.s3a.endpoint";
    public static String S3A_ACCESS_KEY = "fs.s3a.access.key";
    public static String S3A_SECRET_KEY = "fs.s3a.secret.key";
    /*
     * The current implementation supports the local storage
     * of the metadata assigned to machine learning runs:
     *
     * - remote: An external Postgres database is used to
     *   store and manage the model (run) specific metadata
     *
     * - cdap: A CDAP Table abstraction used to store and
     *   manage model (run) specific metadata
     */
    public static final String CDAP_OPTION   = "cdap";
    public static final String REMOTE_OPTION = "remote";

    private final List<String> metadataOptions = Arrays.asList(REMOTE_OPTION, CDAP_OPTION);
    public static String MODEL_METADATA_OPTION = "model.metadata.option";

    public static final String POSTGRES_JDBC_URL      = "postgres.jdbc.url";
    public static final String POSTGRES_JDBC_USER     = "postgres.jdbc.user";
    public static final String POSTGRES_JDBC_PASSWORD = "postgres.jdbc.password";

    private static final ConfigReader instance;

    static {

        try {
            instance = new ConfigReader();
        } catch (Exception e) {
            throw new RuntimeException("[ConfigReader] could not be created.");
        }

    }

    public static ConfigReader getInstance() {
        return instance;
    }

    private ConfigReader() {
        config = CConfiguration.create();
    }
    /**
     * Model artifacts are stored in a local or remote
     * file system or AWS S3
     */
    public String getArtifactFolder() {
        try {
            return config.get(MODEL_ARTIFACT_FOLDER);
        } catch (Exception e) {
            return null;
        }
    }

    public String getArtifactOption() {
        try {
            String option = config.get(MODEL_ARTIFACT_OPTION);
            return artifactOptions
                    .contains(option.toLowerCase()) ? option : null;
        } catch (Exception e) {
            return null;
        }
    }

    public String getJdbcUrl() {
        try {
            return config.get(POSTGRES_JDBC_URL);
        } catch (Exception e) {
            return null;
        }
    }

    public String getJdbcUser() {
        try {
            return config.get(POSTGRES_JDBC_USER);
        } catch (Exception e) {
            return null;
        }
    }

    public String getJdbcPassword() {
        try {
            return config.get(POSTGRES_JDBC_PASSWORD);
        } catch (Exception e) {
            return null;
        }
    }

    public String getMetadataOption() {
        try {
            String option = config.get(MODEL_METADATA_OPTION);
            return metadataOptions
                    .contains(option.toLowerCase()) ? option : null;
        } catch (Exception e) {
            return null;
        }
    }
    /**
     * AWS S3 Access parameters & credentials
     */
    public String getS3Endpoint() {
        try {
            return config.get(S3A_ENDPOINT);
        } catch (Exception e) {
            return null;
        }
    }

    public String getS3AccessKey() {
        try {
            return config.get(S3A_ACCESS_KEY);
        } catch (Exception e) {
            return null;
        }
    }

    public String getS3SecretKey() {
        try {
            return config.get(S3A_SECRET_KEY);
        } catch (Exception e) {
            return null;
        }
    }
}
