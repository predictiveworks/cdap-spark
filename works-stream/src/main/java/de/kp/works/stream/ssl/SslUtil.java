package de.kp.works.stream.ssl;
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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import com.google.common.base.Strings;

public class SslUtil {

	public static KeyManagerFactory getKeyManagerFactory(String keystoreFile, String keystoreType, String keystorePassword,
			String keystoreAlgorithm) throws CertificateException, NoSuchAlgorithmException, KeyStoreException,
			IOException, UnrecoverableKeyException {

		KeyStore keystore = loadKeystore(keystoreFile, keystoreType, keystorePassword);
		/*
		 * We have to manually fall back to default keystore. SSLContext won't provide
		 * such a functionality.
		 */
		if (keystore == null) {

			keystoreFile = System.getProperty("javax.net.ssl.keyStore");
			keystoreType = System.getProperty("javax.net.ssl.keyStoreType", KeyStore.getDefaultType());
			keystorePassword = System.getProperty("javax.net.ssl.keyStorePassword", "");

			keystore = loadKeystore(keystoreFile, keystoreType, keystorePassword);

		}

		keystoreAlgorithm = (Strings.isNullOrEmpty(keystoreAlgorithm)) ? KeyManagerFactory.getDefaultAlgorithm()
				: keystoreAlgorithm;

		KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(keystoreAlgorithm);

		char[] passwordArr = (keystorePassword == null) ? null : keystorePassword.toCharArray();
		keyManagerFactory.init(keystore, passwordArr);

		return keyManagerFactory;

	}

	public static TrustManagerFactory getTrustManagerFactory(String truststoreFile, String truststoreType,
			String truststorePassword, String truststoreAlgorithm, Boolean verifyHttps)
			throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException {

		TrustManagerFactory factory = null;
		if (!verifyHttps) {
			return factory;
		}

		KeyStore trustStore = SslUtil.loadKeystore(truststoreFile, truststoreType, truststorePassword);

		if (trustStore != null) {

			String trustStoreAlgorithm = (Strings.isNullOrEmpty(truststoreAlgorithm))
					? TrustManagerFactory.getDefaultAlgorithm()
					: truststoreAlgorithm;

			factory = TrustManagerFactory.getInstance(trustStoreAlgorithm);
			factory.init(trustStore);

		}

		return factory;

	}

	/**
	 * Load a Java KeyStore loacted at keystoreFile of keystoreType and
	 * keystorePassword
	 * 
	 * @param keystoreFile
	 * @param keystoreType
	 * @param keystorePassword
	 * @return
	 * @throws IOException
	 * @throws CertificateException
	 * @throws NoSuchAlgorithmException
	 * @throws KeyStoreException
	 */
	public static KeyStore loadKeystore(String keystoreFile, String keystoreType, String keystorePassword)
			throws IOException, CertificateException, NoSuchAlgorithmException, KeyStoreException {

		KeyStore keystore = null;
		if (keystoreFile != null) {
			keystore = KeyStore.getInstance(keystoreType);
			char[] passwordArr = (keystorePassword == null) ? null : keystorePassword.toCharArray();
			try (InputStream is = Files.newInputStream(Paths.get(keystoreFile))) {
				keystore.load(is, passwordArr);
			}
		}
		return keystore;
	}

}
