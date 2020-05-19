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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Security;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMDecryptorProvider;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcePEMDecryptorProviderBuilder;

import com.google.common.base.Strings;

public class SslUtil {

	public static final String TLS_VERSION = "TLS";

	public static final String CA_CERT_ALIAS = "caCert-cert";
	public static final String CERT_ALIAS = "cert";

	public static final String PRIVATE_KEY_ALIAS = "private-key";

	/**
	 * Build a SSLSocketFactory without Key & Trust Managers; the TLS version is set
	 * to the default version
	 */
	public static SSLSocketFactory getPlainSslSocketFactory() throws Exception {
		return getPlainSslSocketFactory(TLS_VERSION);
	}

	public static SSLSocketFactory getPlainSslSocketFactory(String tlsVersion) throws Exception {

		SSLContext sslContext = SSLContext.getInstance(tlsVersion);
		sslContext.init(null, null, null);

		return sslContext.getSocketFactory();

	}

	/*** KEY STORE ONLY ***/

	public static SSLSocketFactory getStoreSslSocketFactory(String keystoreFile, String keystoreType,
			String keystorePassword, String keystoreAlgorithm) throws Exception {

		return getStoreSslSocketFactory(keystoreFile, keystoreType, keystorePassword, keystoreAlgorithm, TLS_VERSION);

	}

	public static SSLSocketFactory getStoreSslSocketFactory(String keystoreFile, String keystoreType,
			String keystorePassword, String keystoreAlgorithm, String tlsVersion) throws Exception {

		SSLContext sslContext = SSLContext.getInstance(tlsVersion);

		/* Build Key Managers */
		KeyManager[] keyManagers = null;

		keyManagers = getStoreKeyManagerFactory(keystoreFile, keystoreType, keystorePassword, keystoreAlgorithm)
				.getKeyManagers();

		sslContext.init(keyManagers, null, null);
		return sslContext.getSocketFactory();

	}

	/*** KEY & TRUST STORE ***/

	public static SSLSocketFactory getStoreSslSocketFactory(String keystoreFile, String keystoreType,
			String keystorePassword, String keystoreAlgorithm, String truststoreFile, String truststoreType,
			String truststorePassword, String truststoreAlgorithm, String tlsVersion) throws Exception {

		SSLContext sslContext = SSLContext.getInstance(tlsVersion);

		/* Build Key Managers */
		KeyManager[] keyManagers = null;

		KeyManagerFactory keyManagerFactory = getStoreKeyManagerFactory(keystoreFile, keystoreType, keystorePassword,
				keystoreAlgorithm);
		if (keyManagerFactory != null)
			keyManagers = keyManagerFactory.getKeyManagers();

		/* Build Trust Managers */
		TrustManager[] trustManagers = null;

		TrustManagerFactory trustManagerFactory = getStoreTrustManagerFactory(truststoreFile, truststoreType,
				truststorePassword, truststoreAlgorithm);
		if (trustManagerFactory != null)
			trustManagers = trustManagerFactory.getTrustManagers();

		sslContext.init(keyManagers, trustManagers, null);
		return sslContext.getSocketFactory();

	}

	/*** CERTIFICATE FILES **/

	public static SSLSocketFactory getCertFileSslSocketFactory(String caCrtFile, String crtFile, String keyFile,
			String password) throws Exception {
		return getCertFileSslSocketFactory(caCrtFile, crtFile, keyFile, password, TLS_VERSION);
	}

	public static SSLSocketFactory getCertFileSslSocketFactory(String caCrtFile, String crtFile, String keyFile,
			String password, String tlsVersion) throws Exception {

		Security.addProvider(new BouncyCastleProvider());

		TrustManagerFactory trustManagerFactory = getCertFileTrustManagerFactory(caCrtFile);
		KeyManagerFactory keyManagerFactory = getCertFileKeyManagerFactory(crtFile, keyFile, password);

		SSLContext sslContext = SSLContext.getInstance(tlsVersion);
		sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), null);

		/* Set connect options to use the TLS enabled socket factory */
		return sslContext.getSocketFactory();

	}

	/*** CERTIFICATES ***/

	public static SSLSocketFactory getCertSslSocketFactory(X509Certificate caCert, X509Certificate cert,
			PrivateKey privateKey, String password) throws KeyStoreException, NoSuchAlgorithmException,
			CertificateException, IOException, UnrecoverableKeyException, KeyManagementException {

		return getCertSslSocketFactory(caCert, cert, privateKey, password, TLS_VERSION);
	}

	public static SSLSocketFactory getCertSslSocketFactory(X509Certificate caCert, X509Certificate cert,
			PrivateKey privateKey, String password, String tlsVersion)
			throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException,
			UnrecoverableKeyException, KeyManagementException {

		Security.addProvider(new BouncyCastleProvider());

		TrustManagerFactory trustManagerFactory = getCertTrustManagerFactory(caCert);
		KeyManagerFactory keyManagerFactory = getCertKeyManagerFactory(cert, privateKey, password);

		SSLContext sslContext = SSLContext.getInstance(tlsVersion);
		sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), null);

		/* Set connect options to use the TLS enabled socket factory */
		return sslContext.getSocketFactory();

	}

	/***** KEY MANAGER FACTORY *****/

	public static KeyManagerFactory getStoreKeyManagerFactory(String keystoreFile, String keystoreType,
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

	public static KeyManagerFactory getCertFileKeyManagerFactory(String crtFile, String keyFile, String password)
			throws Exception {

		X509Certificate cert = getX509CertFromPEM(crtFile);
		PrivateKey privateKey = getPrivateKeyFromPEM(keyFile, password);

		return getCertKeyManagerFactory(cert, privateKey, password);

	}

	public static KeyManagerFactory getCertKeyManagerFactory(X509Certificate cert, 
			PrivateKey privateKey, String password) throws KeyStoreException, NoSuchAlgorithmException,
			CertificateException, IOException, UnrecoverableKeyException {

		KeyStore ks = createKeystore();
		/*
		 * Add client certificate to key store, the client certificate alias is
		 * 'certificate' (see IBM Watson IoT platform)
		 */
		String certAlias = CERT_ALIAS;
		ks.setCertificateEntry(certAlias, cert);

		/*
		 * Add private key to keystore and distinguish between use case with and without
		 * password
		 */
		X509Certificate[] chain = new X509Certificate[] { cert };

		char[] passwordArray = (password != null) ? password.toCharArray() : new char[0];

		String keyAlias = PRIVATE_KEY_ALIAS;
		ks.setKeyEntry(keyAlias, privateKey, passwordArray, chain);

		/*
		 * Initialize key manager from the key store; note, the default algorithm also
		 * supported by IBM Watson IoT platform is PKIX
		 */
		KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
		keyManagerFactory.init(ks, passwordArray);

		return keyManagerFactory;

	}

	/***** TRUST MANAGER FACTORY *****/

	public static TrustManagerFactory getStoreTrustManagerFactory(String truststoreFile, String truststoreType,
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

	public static TrustManagerFactory getCertTrustManagerFactory(X509Certificate caCert)
			throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException {

		KeyStore ks = createKeystore();
		/*
		 * Add CA certificate to keystore; note, the CA certificate alias is set to
		 * 'ca-certificate' (see IBM Watson IoT platform)
		 */
		String caCertAlias = CA_CERT_ALIAS;
		ks.setCertificateEntry(caCertAlias, caCert);

		/*
		 * Establish certificate trust chain; note, the default algorithm also supported
		 * by IBM Watson IoT platform is PKIX
		 */
		TrustManagerFactory trustManagerFactory = TrustManagerFactory
				.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		trustManagerFactory.init(ks);

		return trustManagerFactory;

	}

	public static TrustManagerFactory getCertFileTrustManagerFactory(String caCrtFile) throws Exception {

		X509Certificate caCert = getX509CertFromPEM(caCrtFile);
		return getCertTrustManagerFactory(caCert);

	}

	/***** X509 CERTIFICATE *****/

	public static X509Certificate getX509CertFromPEM(String crtFile) throws IOException {
		/*
		 * Since Java cannot read PEM formatted certificates, this method is using
		 * bouncy castle (http://www.bouncycastle.org/) to load the necessary files.
		 * 
		 * IMPORTANT: Bouncycastle Provider must be added before this method can be
		 * called
		 * 
		 */
		byte[] bytes = Files.readAllBytes(Paths.get(crtFile));
		ByteArrayInputStream bais = new ByteArrayInputStream(bytes);

		PEMParser reader = new PEMParser(new InputStreamReader(bais));
		X509Certificate cert = (X509Certificate) reader.readObject();

		reader.close();
		return cert;

	}

	/***** PRIVATE KEY *****/

	public static PrivateKey getPrivateKeyFromPEM(String keyFile, String password) throws Exception {

		byte[] bytes = Files.readAllBytes(Paths.get(keyFile));
		ByteArrayInputStream bais = new ByteArrayInputStream(bytes);

		PEMParser reader = new PEMParser(new InputStreamReader(bais));
		Object keyObject = reader.readObject();

		reader.close();

		PEMKeyPair keyPair = null;

		if (keyObject instanceof PEMEncryptedKeyPair) {

			if (password == null)
				throw new Exception("[ERROR] Reading private key from file without password is not supported.");

			char[] passwordArray = password.toCharArray();
			PEMDecryptorProvider provider = new JcePEMDecryptorProviderBuilder().build(passwordArray);

			keyPair = ((PEMEncryptedKeyPair) keyObject).decryptKeyPair(provider);

		} else {
			keyPair = (PEMKeyPair) keyObject;
		}

		KeyFactory factory = KeyFactory.getInstance("RSA", "BC");
		PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(keyPair.getPrivateKeyInfo().getEncoded());

		return factory.generatePrivate(keySpec);

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

	public static KeyStore createKeystore()
			throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException {
		/*
		 * Create a default (JKS) keystore without any password. Method load(null, null)
		 * indicates to create a new one
		 */
		KeyStore keystore = KeyStore.getInstance(KeyStore.getDefaultType());
		keystore.load(null, null);

		return keystore;

	}

}
