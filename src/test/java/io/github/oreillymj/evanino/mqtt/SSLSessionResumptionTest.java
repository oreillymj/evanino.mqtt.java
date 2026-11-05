/*******************************************************************************
 * Ported from Eclipse Paho's Java client tests,
 * org/eclipse/paho/client/mqttv3/test/SSLSessionResumptionTest.java,
 * https://github.com/eclipse-paho/paho.mqtt.java. This project uses the Paho code under
 * the Eclipse Distribution License v1.0 (see licenses/EDL-1.0.txt).
 *
 * The original notice:
 *
 *   Copyright (c) 2011, 2014 Eurotech and/or its affiliates
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
 *     Cristiano De Alti - Eurotech (Initial contribution)
 *     James Sutton - IBM (Fixing Copyright header and adding getSocketFactory)
 *
 * Changes copyright (c) 2026 Michael OReilly, under the Apache License 2.0 (see LICENSE).
 *******************************************************************************/

package io.github.oreillymj.evanino.mqtt;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.net.URI;
import java.util.UUID;

/**
 * Port of Paho's {@code SSLSessionResumptionTest}: a TLS client can disconnect and reconnect
 * after its TLS session is gone, and TLS sessions to the broker are cached. Requires a broker
 * with a TLS listener; see {@link MosquittoBroker}.
 * <p>
 * Paho passes an {@code SSLSocketFactory} with {@code setSocketFactory} and the URI with
 * {@code setServerURIs}; here the TLS context and URI are set in the options. The broker URI
 * uses {@code mqtts://}, which is an alias of Paho's {@code ssl://}.
 */
public class SSLSessionResumptionTest {

	/** The broker used by every test in this class. */
	@ClassRule
	public static final MosquittoBroker broker = new MosquittoBroker();

	private static URI serverURI;
	private static String serverHost;
	private static int serverPort;

	/**
	 * Picks up the TLS broker address.
	 */
	@BeforeClass
	public static void setUpBeforeClass() {
		serverURI = broker.getSslServerURI();
		serverHost = serverURI.getHost();
		serverPort = serverURI.getPort();
	}

	/**
	 * A TLS client can connect, disconnect and connect again with a pause in between.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 60_000)
	public void testSSLSessionInvalidated() throws Exception {
		MqttConnectOptions options = TestClients.options(serverURI, "SSLSessionResumptionTest-" + UUID.randomUUID());
		options.setKeepAliveInterval(60);
		options.setSslContext(TestTls.clientContext());

		MqttClient mqttClient = new MqttClient(options);
		try {
			mqttClient.connect(options);
			Assert.assertTrue(mqttClient.isConnected());
			Thread.sleep(2000);
			mqttClient.disconnect();
			Thread.sleep(2000);

			mqttClient.connect(options);
			Assert.assertTrue(mqttClient.isConnected());
			Thread.sleep(2000);
			mqttClient.disconnect();
		} finally {
			TestClients.closeQuietly(mqttClient);
		}
	}

	/**
	 * Two TLS handshakes with the same socket factory share the client's session cache. This
	 * exercises the JDK and the broker only, as in Paho.
	 *
	 * @throws Exception if the TLS context cannot be created
	 */
	@Test(timeout = 10_000)
	public void testSSLSessionCached() throws Exception {
		SSLContext context = TestTls.clientContext();
		SSLSocketFactory factory = context.getSocketFactory();
		doHandshake(factory, serverHost, serverPort);
		doHandshake(factory, serverHost, serverPort);
		Assert.assertTrue("no TLS session was cached",
				context.getClientSessionContext().getIds().hasMoreElements());
	}

	/**
	 * Opens a TLS socket, completes the handshake and closes it.
	 *
	 * @param factory the socket factory
	 * @param host    the broker host
	 * @param port    the broker TLS port
	 * @throws Exception if the handshake fails
	 */
	private static void doHandshake(SSLSocketFactory factory, String host, int port) throws Exception {
		try (SSLSocket socket = (SSLSocket) factory.createSocket(host, port)) {
			socket.startHandshake();
			Assert.assertNotNull(socket.getSession().getSessionContext());
		}
	}
}
