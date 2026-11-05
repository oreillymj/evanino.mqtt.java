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

import org.junit.Assume;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.security.KeyStore;
import java.util.function.Consumer;

/**
 * Test TLS material, standing in for Paho's {@code TestProperties} keystore settings.
 * <p>
 * The certificates in {@code src/test/resources/tls} are for tests only: a CA, a server
 * certificate valid for {@code localhost} only (no IP address, so hostname verification against
 * {@code 127.0.0.1} fails), and a client certificate signed by the CA. The JKS keystore and
 * truststore passwords are {@code password}.
 */
final class TestTls {

	/** Password of the client keystore and truststore. */
	static final String PASSWORD = "password";

	/**
	 * Prevents instantiation; this class only holds static helpers.
	 */
	private TestTls() {
	}

	/**
	 * Locates a file under {@code tls/} on the test classpath.
	 *
	 * @param name the file name
	 * @return the file, or {@code null} if it is not on the classpath
	 */
	static File file(String name) {
		URL url = TestTls.class.getClassLoader().getResource("tls/" + name);
		if (url == null) {
			return null;
		}
		try {
			return new File(url.toURI());
		} catch (Exception e) {
			return null;
		}
	}

	/**
	 * Returns the mosquitto settings for a TLS listener using the test certificates.
	 *
	 * @return the {@code cafile}, {@code certfile} and {@code keyfile} lines, or {@code null} if
	 *         the certificates are not on the classpath
	 */
	static String mosquittoTlsSettings() {
		File ca = file("ca.crt");
		File cert = file("server.crt");
		File key = file("server.key");
		if (ca == null || cert == null || key == null) {
			return null;
		}
		return "cafile " + slashes(ca) + "\n"
				+ "certfile " + slashes(cert) + "\n"
				+ "keyfile " + slashes(key) + "\n";
	}

	/**
	 * Creates a client TLS context that trusts the test CA and presents the test client
	 * certificate, the equivalent of Paho's {@code javax.net.ssl.keyStore} /
	 * {@code javax.net.ssl.trustStore} system properties.
	 *
	 * @return a new TLS context
	 * @throws Exception if the keystores cannot be loaded
	 */
	static SSLContext clientContext() throws Exception {
		File keystoreFile = file("client-keystore.jks");
		File truststoreFile = file("client-truststore.jks");
		Assume.assumeTrue("test TLS keystores not on the classpath", keystoreFile != null && truststoreFile != null);

		KeyStore keyStore = KeyStore.getInstance("JKS");
		try (InputStream in = new FileInputStream(keystoreFile)) {
			keyStore.load(in, PASSWORD.toCharArray());
		}
		KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
		kmf.init(keyStore, PASSWORD.toCharArray());

		KeyStore trustStore = KeyStore.getInstance("JKS");
		try (InputStream in = new FileInputStream(truststoreFile)) {
			trustStore.load(in, PASSWORD.toCharArray());
		}
		TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		tmf.init(trustStore);

		SSLContext context = SSLContext.getInstance("TLS");
		context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
		return context;
	}

	/**
	 * Returns an {@link SSLEngine} hook that turns on HTTPS-style hostname verification, the
	 * equivalent of Paho's {@code setHttpsHostnameVerificationEnabled(true)}.
	 *
	 * @return the engine consumer to pass to {@link MqttConnectOptions#setSslEngineConsumer(Consumer)}
	 */
	static Consumer<SSLEngine> httpsHostnameVerification() {
		return engine -> {
			SSLParameters params = engine.getSSLParameters();
			params.setEndpointIdentificationAlgorithm("HTTPS");
			engine.setSSLParameters(params);
		};
	}

	/**
	 * Returns a file's absolute path with forward slashes, as mosquitto expects on Windows.
	 *
	 * @param f the file
	 * @return the path
	 */
	private static String slashes(File f) {
		return f.getAbsolutePath().replace('\\', '/');
	}
}
