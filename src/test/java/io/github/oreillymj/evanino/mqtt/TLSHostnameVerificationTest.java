/*******************************************************************************
 * Ported from Eclipse Paho's Java client tests,
 * org/eclipse/paho/client/mqttv3/test/TLSHostnameVerificationTest.java,
 * https://github.com/eclipse-paho/paho.mqtt.java. This project uses the Paho code under
 * the Eclipse Distribution License v1.0 (see licenses/EDL-1.0.txt).
 *
 * The original notice:
 *
 *   Copyright (c) 2011, 2017 IBM
 *
 *   All rights reserved. This program and the accompanying materials
 *   are made available under the terms of the Eclipse Public License v2.0
 *   and Eclipse Distribution License v1.0 which accompany this distribution.
 *
 *   The Eclipse Public License is available at
 *      https://www.eclipse.org/legal/epl-2.0
 *   and the Eclipse Distribution License is available at
 *     https://www.eclipse.org/org/documents/edl-v10.php
 *
 *   Contributors:
 *     James Sutton - IBM Adding HTTPS Hostname verification tests.
 *
 * Changes copyright (c) 2026 Michael OReilly, under the Apache License 2.0 (see LICENSE).
 *******************************************************************************/

package io.github.oreillymj.evanino.mqtt;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLParameters;
import java.net.URI;
import java.util.UUID;

/**
 * Port of Paho's {@code TLSHostnameVerificationTest}: with HTTPS-style hostname verification
 * on, a TLS connection succeeds when the host name matches the server certificate and fails
 * when it does not, over plain TLS and over secure WebSockets. Requires a broker with TLS
 * listeners; see {@link MosquittoBroker}. The test server certificate is valid for
 * {@code localhost} only, so connecting to {@code 127.0.0.1} must fail verification.
 * <p>
 * Differences from Paho:
 * <ul>
 * <li>Hostname verification is off by default, so the tests turn it on with
 * {@code setHttpsHostnameVerificationEnabled(true)}, and the TLS context is set in the options
 * (see {@link BasicSSLTest}).</li>
 * <li>The broker URI uses {@code mqtts://}, which is an alias of Paho's {@code ssl://}.</li>
 * <li>Paho's "invalid" tests only check the exception if one is thrown, so they pass when the
 * connection succeeds. Here the connection is required to fail with an
 * {@link SSLHandshakeException}.</li>
 * </ul>
 * The tests after Paho's also check the default, and that the {@code SSLEngine} hook set with
 * {@code setSslEngineConsumer} still works alongside the setter.
 */
public class TLSHostnameVerificationTest {

	/** The broker used by every test in this class. */
	@ClassRule
	public static final MosquittoBroker broker = new MosquittoBroker();

	private static final String serverHost = "localhost";
	private static final String serverIP = "127.0.0.1";
	private static int serverPort;
	private static int websocketPort;

	/**
	 * Picks up the TLS and secure WebSocket ports.
	 */
	@BeforeClass
	public static void setUpBeforeClass() {
		serverPort = broker.getSslServerURI().getPort();
		websocketPort = broker.getSecureWebSocketServerURI().getPort();
	}

	/**
	 * Connecting to {@code localhost}, which the server certificate names, succeeds.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 10_000)
	public void testValidHTTPSStyleHostnameVerification() throws Exception {
		MqttClient mqttClient = createVerifyingClient(new URI("mqtts://" + serverHost + ":" + serverPort));
		try {
			mqttClient.connect(mqttClient.getOptions());
			Assert.assertTrue(mqttClient.isConnected());
			mqttClient.disconnect();
		} finally {
			TestClients.closeQuietly(mqttClient);
		}
	}

	/**
	 * Connecting to {@code 127.0.0.1}, which the server certificate does not name, fails the
	 * TLS handshake.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 10_000)
	public void testInvalidHTTPSStyleHostnameVerification() throws Exception {
		MqttClient mqttClient = createVerifyingClient(new URI("mqtts://" + serverIP + ":" + serverPort));
		try {
			mqttClient.connect(mqttClient.getOptions());
			Assert.fail("Hostname verification should have rejected " + serverIP);
		} catch (MqttException ex) {
			// We want to make sure that the returned exception is an SSLHandshakeException
			assertCausedBySslHandshakeException(ex);
		} finally {
			TestClients.closeQuietly(mqttClient);
		}
	}

	/**
	 * Connecting over secure WebSockets to {@code localhost} succeeds.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 10_000)
	public void testValidWebSocketHTTPSStyleHostnameVerification() throws Exception {
		MqttClient mqttClient = createVerifyingClient(new URI("wss://" + serverHost + ":" + websocketPort + "/ws"));
		try {
			mqttClient.connect(mqttClient.getOptions());
			Assert.assertTrue(mqttClient.isConnected());
			mqttClient.disconnect();
		} finally {
			TestClients.closeQuietly(mqttClient);
		}
	}

	/**
	 * Connecting over secure WebSockets to {@code 127.0.0.1} fails the TLS handshake.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 10_000)
	public void testInvalidWebSocketHTTPSStyleHostnameVerification() throws Exception {
		MqttClient mqttClient = createVerifyingClient(new URI("wss://" + serverIP + ":" + websocketPort + "/ws"));
		try {
			mqttClient.connect(mqttClient.getOptions());
			Assert.fail("Hostname verification should have rejected " + serverIP);
		} catch (MqttException ex) {
			// We want to make sure that the returned exception is an SSLHandshakeException
			assertCausedBySslHandshakeException(ex);
		} finally {
			TestClients.closeQuietly(mqttClient);
		}
	}

	/**
	 * Hostname verification is off by default, so connecting to {@code 127.0.0.1}, which the
	 * server certificate does not name, succeeds.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 10_000)
	public void testHostnameVerificationOffByDefault() throws Exception {
		MqttConnectOptions options = TestClients.options(new URI("mqtts://" + serverIP + ":" + serverPort),
				"TLSHostnameVerificationTest-" + UUID.randomUUID());
		Assert.assertFalse(options.isHttpsHostnameVerificationEnabled());
		options.setSslContext(TestTls.clientContext());
		assertConnects(options);
	}

	/**
	 * Turning verification on and off again goes back to connecting to {@code 127.0.0.1}.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 10_000)
	public void testHostnameVerificationDisabledAgain() throws Exception {
		MqttConnectOptions options = TestClients.options(new URI("mqtts://" + serverIP + ":" + serverPort),
				"TLSHostnameVerificationTest-" + UUID.randomUUID());
		options.setSslContext(TestTls.clientContext());
		options.setHttpsHostnameVerificationEnabled(true);
		Assert.assertTrue(options.isHttpsHostnameVerificationEnabled());
		options.setHttpsHostnameVerificationEnabled(false);
		Assert.assertFalse(options.isHttpsHostnameVerificationEnabled());
		assertConnects(options);
	}

	/**
	 * The {@code SSLEngine} hook that turned verification on before the setter existed still
	 * rejects {@code 127.0.0.1}.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 10_000)
	public void testEngineConsumerStillVerifies() throws Exception {
		MqttConnectOptions options = TestClients.options(new URI("mqtts://" + serverIP + ":" + serverPort),
				"TLSHostnameVerificationTest-" + UUID.randomUUID());
		options.setSslContext(TestTls.clientContext());
		options.setSslEngineConsumer(TestTls.httpsHostnameVerification());
		MqttClient mqttClient = new MqttClient(options);
		mqttClient.setTimeToWait(5000);
		try {
			mqttClient.connect(options);
			Assert.fail("Hostname verification should have rejected " + serverIP);
		} catch (MqttException ex) {
			assertCausedBySslHandshakeException(ex);
		} finally {
			TestClients.closeQuietly(mqttClient);
		}
	}

	/**
	 * The {@code SSLEngine} hook runs after the setter has been applied, so it can turn
	 * verification off again for a single connection.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 10_000)
	public void testEngineConsumerOverridesSetter() throws Exception {
		MqttConnectOptions options = TestClients.options(new URI("mqtts://" + serverIP + ":" + serverPort),
				"TLSHostnameVerificationTest-" + UUID.randomUUID());
		options.setSslContext(TestTls.clientContext());
		options.setHttpsHostnameVerificationEnabled(true);
		String[] seen = new String[1];
		options.setSslEngineConsumer(engine -> {
			SSLParameters params = engine.getSSLParameters();
			seen[0] = params.getEndpointIdentificationAlgorithm();
			// "", not null: the JDK keeps the previous algorithm when given null
			params.setEndpointIdentificationAlgorithm("");
			engine.setSSLParameters(params);
		});
		assertConnects(options);
		Assert.assertEquals("HTTPS", seen[0]);
	}

	/**
	 * Connects with the given options, checks the client is connected, then disconnects.
	 *
	 * @param options the connection options
	 * @throws Exception if the connection fails
	 */
	private static void assertConnects(MqttConnectOptions options) throws Exception {
		MqttClient mqttClient = new MqttClient(options);
		mqttClient.setTimeToWait(5000);
		try {
			mqttClient.connect(options);
			Assert.assertTrue(mqttClient.isConnected());
			mqttClient.disconnect();
		} finally {
			TestClients.closeQuietly(mqttClient);
		}
	}

	/**
	 * Creates a TLS client with hostname verification enabled.
	 *
	 * @param serverURI the broker URI
	 * @return a new, unconnected client
	 * @throws Exception if the TLS context cannot be created
	 */
	private static MqttClient createVerifyingClient(URI serverURI) throws Exception {
		MqttConnectOptions options = TestClients.options(serverURI, "TLSHostnameVerificationTest-" + UUID.randomUUID());
		options.setSslContext(TestTls.clientContext());
		options.setHttpsHostnameVerificationEnabled(true);
		MqttClient client = new MqttClient(options);
		client.setTimeToWait(5000);
		return client;
	}

	/**
	 * Checks that an {@link SSLHandshakeException} appears in an exception's cause chain.
	 *
	 * @param ex the exception thrown by connect
	 */
	private static void assertCausedBySslHandshakeException(Throwable ex) {
		for (Throwable t = ex; t != null; t = t.getCause()) {
			if (t instanceof SSLHandshakeException) {
				return;
			}
			if (t.getCause() == t) {
				break;
			}
		}
		Assert.fail("Expected an SSLHandshakeException in the cause chain of " + ex);
	}
}
