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

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import com.google.common.base.Strings;

public class SslUtil {

	private static final String TLS_VERSION = "TLSv1.2";

	/**
	 * Build a SSLSocketFactory without Key & Trust Managers;
	 * the TLS version is set to the default version
	 */
	public static SSLSocketFactory getPlainSslSocketFactory() throws Exception {
		return getPlainSslSocketFactory(TLS_VERSION);
	}

	public static SSLSocketFactory getPlainSslSocketFactory(String tlsVersion) throws Exception {

		SSLContext sslContext = SSLContext.getInstance(tlsVersion);
		sslContext.init(null, null, null);

		return sslContext.getSocketFactory();

	}

	public static SSLSocketFactory getSslSocketFactory(String keystoreFile, String keystoreType,
			String keystorePassword, String keystoreAlgorithm) throws Exception {

		return getSslSocketFactory(keystoreFile, keystoreType, keystorePassword, keystoreAlgorithm, TLS_VERSION);

	}

	public static SSLSocketFactory getSslSocketFactory(String keystoreFile, String keystoreType,
			String keystorePassword, String keystoreAlgorithm, String tlsVersion) throws Exception {

		SSLContext sslContext = SSLContext.getInstance(tlsVersion);

		/* Build Key Managers */
		KeyManager[] keyManagers = null;

		keyManagers = getKeyManagerFactory(keystoreFile, keystoreType, keystorePassword, keystoreAlgorithm)
				.getKeyManagers();

		sslContext.init(keyManagers, null, null);
		return sslContext.getSocketFactory();

	}
	
	public static SSLSocketFactory getSslSocketFactory(String keystoreFile, String keystoreType,
			String keystorePassword, String keystoreAlgorithm, String truststoreFile, String truststoreType,
			String truststorePassword, String truststoreAlgorithm, String tlsVersion) throws Exception {

		SSLContext sslContext = SSLContext.getInstance(tlsVersion);
		
		/* Build Key Managers */
		KeyManager[] keyManagers = null;

		KeyManagerFactory keyManagerFactory = getKeyManagerFactory(keystoreFile, keystoreType, keystorePassword, keystoreAlgorithm);
		if (keyManagerFactory != null)
			keyManagers = keyManagerFactory.getKeyManagers();

		/* Build Trust Managers */
		TrustManager[] trustManagers = null;
		
		TrustManagerFactory trustManagerFactory = getTrustManagerFactory(truststoreFile, truststoreType, truststorePassword, truststoreAlgorithm);
		if (trustManagerFactory != null)
			trustManagers = trustManagerFactory.getTrustManagers();
		
		
		sslContext.init(keyManagers, trustManagers, null);
		return sslContext.getSocketFactory();

	}

	/***** KEY MANAGER FACTORY *****/

	public static KeyManagerFactory getKeyManagerFactory(String keystoreFile, String keystoreType,
			String keystorePassword, String keystoreAlgorithm) throws CertificateException, NoSuchAlgorithmException,
			KeyStoreException, IOException, UnrecoverableKeyException {

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

	/***** TRUST MANAGER FACTORY *****/

	public static TrustManagerFactory getTrustManagerFactory(String truststoreFile, String truststoreType,
			String truststorePassword, String truststoreAlgorithm)
			throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException {

		TrustManagerFactory factory = null;
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
