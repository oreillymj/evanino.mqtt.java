/*******************************************************************************
 * Copyright (c) 2026 Michael OReilly
 *
 * This program and the accompanying materials are made available under
 * the terms of the Apache License, Version 2.0, which accompanies this
 * distribution (see LICENSE).
 *
 * The Apache License is available at
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Contributors:
 *    Michael OReilly - initial API and implementation
 *******************************************************************************/

package io.github.oreillymj.evanino.mqtt;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.ClassRule;
import org.junit.Test;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.util.Properties;
import java.util.UUID;

/**
 * Checks the recipe in PahoMigration.md for replacing Paho's {@code setSSLProperties} and its
 * {@code com.ibm.ssl.*} keys, which this client does not implement:
 * {@link #fromPahoProperties(Properties)} is the guide's code, unchanged. Requires a broker with
 * TLS listeners; see {@link MosquittoBroker}.
 */
public class PahoSslPropertiesRecipeTest {

	/** The broker used by every test in this class. */
	@ClassRule
	public static final MosquittoBroker broker = new MosquittoBroker();

	/**
	 * Paho-style properties naming the test keystore, truststore, protocol and a cipher suite
	 * give a context that connects to the broker, which requires a client certificate.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 10_000)
	public void testRecipeConnects() throws Exception {
		Properties sslProperties = testProperties();
		sslProperties.setProperty("com.ibm.ssl.protocol", "TLSv1.2");
		sslProperties.setProperty("com.ibm.ssl.enabledCipherSuites",
				"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256, TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384");

		MqttConnectOptions options = TestClients.options(broker.getSslServerURI(), "PahoSslPropertiesRecipeTest-" + UUID.randomUUID());
		options.setSslContext(fromPahoProperties(sslProperties));
		String ciphers = sslProperties.getProperty("com.ibm.ssl.enabledCipherSuites");
		if (ciphers != null) {
			options.setSslEngineConsumer(engine -> engine.setEnabledCipherSuites(ciphers.split("\\s*,\\s*")));
		}

		MqttClient client = new MqttClient(options);
		client.setTimeToWait(5000);
		try {
			client.connect(options);
			Assert.assertTrue(client.isConnected());
			client.disconnect();
		} finally {
			TestClients.closeQuietly(client);
		}
	}

	/**
	 * Without the keystore the broker, which requires a client certificate, refuses the
	 * connection: the keystore keys really are used.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 10_000)
	public void testRecipeWithoutKeyStoreIsRefused() throws Exception {
		Properties sslProperties = testProperties();
		sslProperties.remove("com.ibm.ssl.keyStore");

		MqttConnectOptions options = TestClients.options(broker.getSslServerURI(), "PahoSslPropertiesRecipeTest-" + UUID.randomUUID());
		options.setSslContext(fromPahoProperties(sslProperties));
		MqttClient client = new MqttClient(options);
		client.setTimeToWait(5000);
		try {
			client.connect(options);
			Assert.fail("The broker requires a client certificate");
		} catch (MqttException expected) {
			Assert.assertFalse(client.isConnected());
		} finally {
			TestClients.closeQuietly(client);
		}
	}

	/**
	 * Returns Paho-style properties for the test keystore and truststore.
	 *
	 * @return the properties
	 */
	private static Properties testProperties() {
		File keystore = TestTls.file("client-keystore.jks");
		File truststore = TestTls.file("client-truststore.jks");
		Assume.assumeTrue("test TLS keystores not on the classpath", keystore != null && truststore != null);
		Properties p = new Properties();
		p.setProperty("com.ibm.ssl.keyStore", keystore.getAbsolutePath());
		p.setProperty("com.ibm.ssl.keyStorePassword", TestTls.PASSWORD);
		p.setProperty("com.ibm.ssl.keyStoreType", "JKS");
		p.setProperty("com.ibm.ssl.trustStore", truststore.getAbsolutePath());
		p.setProperty("com.ibm.ssl.trustStorePassword", TestTls.PASSWORD);
		p.setProperty("com.ibm.ssl.trustStoreType", "JKS");
		return p;
	}

	/**
	 * Builds a TLS context from Paho's {@code com.ibm.ssl.*} properties; the code given in
	 * PahoMigration.md.
	 *
	 * @param p the properties
	 * @return the TLS context
	 * @throws Exception if a keystore cannot be loaded or an algorithm is unknown
	 */
	static SSLContext fromPahoProperties(Properties p) throws Exception {
		KeyManager[] keyManagers = null;
		String keyStoreFile = p.getProperty("com.ibm.ssl.keyStore");
		if (keyStoreFile != null) {
			char[] password = p.getProperty("com.ibm.ssl.keyStorePassword", "").toCharArray();  // decode {xor} first
			KeyStore keyStore = KeyStore.getInstance(p.getProperty("com.ibm.ssl.keyStoreType", KeyStore.getDefaultType()));
			try (InputStream in = new FileInputStream(keyStoreFile)) {
				keyStore.load(in, password);
			}
			KeyManagerFactory kmf = KeyManagerFactory.getInstance(
					p.getProperty("com.ibm.ssl.keyManager", KeyManagerFactory.getDefaultAlgorithm()));
			kmf.init(keyStore, password);
			keyManagers = kmf.getKeyManagers();
		}

		TrustManager[] trustManagers = null;   // null: the JVM's default trust store
		String trustStoreFile = p.getProperty("com.ibm.ssl.trustStore");
		if (trustStoreFile != null) {
			KeyStore trustStore = KeyStore.getInstance(p.getProperty("com.ibm.ssl.trustStoreType", KeyStore.getDefaultType()));
			try (InputStream in = new FileInputStream(trustStoreFile)) {
				trustStore.load(in, p.getProperty("com.ibm.ssl.trustStorePassword", "").toCharArray());
			}
			TrustManagerFactory tmf = TrustManagerFactory.getInstance(
					p.getProperty("com.ibm.ssl.trustManager", TrustManagerFactory.getDefaultAlgorithm()));
			tmf.init(trustStore);
			trustManagers = tmf.getTrustManagers();
		}

		SSLContext context = SSLContext.getInstance(p.getProperty("com.ibm.ssl.protocol", "TLS"));
		context.init(keyManagers, trustManagers, null);
		return context;
	}
}
