package de.kp.works.vs.http;
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
import de.kp.works.vs.config.VisualConfig;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

public class SslFactoryCreator {

    private final VisualConfig config;

    public SslFactoryCreator(VisualConfig config) {
        this.config = config;
    }

    public SSLConnectionSocketFactory create() {

        try {

            /* "TLS" means rely system properties */
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(getKeyManagers(), getTrustManagers(), null);

            return new SSLConnectionSocketFactory(sslContext);

        } catch (KeyManagementException | CertificateException | NoSuchAlgorithmException | KeyStoreException
                | IOException | UnrecoverableKeyException e) {
            throw new IllegalStateException("Failed to create an SSL connection factory.", e);
        }
    }

    private KeyManager[] getKeyManagers() throws CertificateException, NoSuchAlgorithmException, KeyStoreException,
            IOException, UnrecoverableKeyException {

        String keystoreFile = config.sslKeyStorePath;
        String keystoreType = config.sslKeyStoreType;

        String keystorePassword = config.sslKeyStorePass;
        String keystoreAlgorithm = config.sslKeyStoreAlgo;


        return SslUtil.getKeyManagers(keystoreFile, keystoreType, keystorePassword, keystoreAlgorithm);

    }

    private TrustManager[] getTrustManagers()
            throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException {

        String truststoreFile = config.sslTrustStorePath;
        String truststoreType = config.sslTrustStoreType;

        String truststorePassword = config.sslTrustStorePass;
        String truststoreAlgorithm = config.sslTrustStoreAlgo;

        boolean verifySsl;
        if (Strings.isNullOrEmpty(config.sslVerify)) {
            verifySsl = false;
        }
        else {
            verifySsl = config.sslVerify.equals("true");
        }

        return SslUtil.getTrustManagers(truststoreFile, truststoreType, truststorePassword, truststoreAlgorithm, verifySsl);

    }
}
