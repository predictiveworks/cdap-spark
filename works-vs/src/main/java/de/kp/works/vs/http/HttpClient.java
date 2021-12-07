package de.kp.works.vs.http;
/*
 * Copyright (c) 2019 Dr. Krusche & Partner PartG. All rights reserved.
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

import de.kp.works.vs.config.VisualConfig;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.spark_project.guava.net.HttpHeaders;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class HttpClient implements Closeable {

    /*
     * Connection timeout in seconds
     */
    private final long CONNECTION_TIMEOUT_SEC = 30;
    private final long READ_TIMEOUT_SEC       = 30;

    private final VisualConfig config;
    private CloseableHttpClient httpClient;

    public HttpClient(VisualConfig config) {
        this.config = config;
    }

    /**
     * Executes HTTP request with parameters configured and returns
     * response. Is called to load every page by pagination iterator.
     */
    public CloseableHttpResponse execute(String uri, String body) throws IOException {

        if (httpClient == null) {
            httpClient = createHttpClient();
        }

        HttpPost post = new HttpPost(uri);

        post.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        post.setEntity(new StringEntity(body));

        return httpClient.execute(post);
    }

    @Override
    public void close() throws IOException {

        if (httpClient != null) {
            httpClient.close();
        }
    }

    private CloseableHttpClient createHttpClient() {

        HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
        httpClientBuilder.setSSLSocketFactory(new SslFactoryCreator(config).create());

        long connectTimeoutMillis = TimeUnit.SECONDS.toMillis(CONNECTION_TIMEOUT_SEC);
        long readTimeoutMillis = TimeUnit.SECONDS.toMillis(READ_TIMEOUT_SEC);

        RequestConfig.Builder requestBuilder = RequestConfig.custom();

        requestBuilder.setSocketTimeout((int) readTimeoutMillis);
        requestBuilder.setConnectTimeout((int) connectTimeoutMillis);
        requestBuilder.setConnectionRequestTimeout((int) connectTimeoutMillis);

        httpClientBuilder.setDefaultRequestConfig(requestBuilder.build());
        return httpClientBuilder.build();

    }

}

