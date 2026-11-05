/*******************************************************************************
 * Ported from Eclipse Paho's Java client tests,
 * org/eclipse/paho/client/mqttv3/test/automaticReconnect/AutomaticReconnectTest.java,
 * https://github.com/eclipse-paho/paho.mqtt.java. This project uses the Paho code under
 * the Eclipse Distribution License v1.0 (see licenses/EDL-1.0.txt).
 *
 * The original notice:
 *
 *   Copyright (c) 2016, 2018 IBM Corp.
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
 * Changes copyright (c) 2026 Michael OReilly, under the Apache License 2.0 (see LICENSE).
 *******************************************************************************/

package io.github.oreillymj.evanino.mqtt;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.net.URI;
import java.util.concurrent.TimeUnit;

/**
 * Port of Paho's {@code AutomaticReconnectTest}: the client reconnects by itself after the
 * network drops, can be reconnected manually, and does not retry when the very first connect
 * fails. The network is dropped with a {@link ConnectionManipulationProxyServer} between the
 * client and the broker. Requires a broker; see {@link MosquittoBroker}.
 * <p>
 * Paho also gives the client a {@code MemoryPersistence} store; this client has no
 * persistence option, so none is used.
 */
public class AutomaticReconnectTest {

	/** The broker used by every test in this class. */
	@ClassRule
	public static final MosquittoBroker broker = new MosquittoBroker();

	private static ConnectionManipulationProxyServer proxy;
	private static URI proxyURI;
	private final String clientId = "device-client-id";

	/**
	 * Starts the proxy in front of the broker.
	 *
	 * @throws Exception if the proxy cannot be started
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		URI serverURI = broker.getServerURI();
		// Use 0 for the first time.
		proxy = new ConnectionManipulationProxyServer(serverURI.getHost(), serverURI.getPort(), 0);
		proxy.startProxy();
		while (!proxy.isPortSet()) {
			Thread.sleep(0);
		}
		proxyURI = new URI("tcp://localhost:" + proxy.getLocalPort());
	}

	/**
	 * Stops the proxy.
	 */
	@AfterClass
	public static void tearDownAfterClass() {
		if (proxy != null) {
			proxy.stopProxy();
		}
	}

	/**
	 * After the network drops, a client with automatic reconnect enabled reconnects within
	 * four seconds of the network returning.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testAutomaticReconnectAfterDisconnect() throws Exception {
		MqttConnectOptions options = TestClients.options(proxyURI, clientId);
		options.setCleanSession(true);
		options.setAutomaticReconnect(true);
		final MqttClient client = new MqttClient(options);
		try {
			proxy.enableProxy();
			client.connect(options);

			boolean isConnected = client.isConnected();
			Assert.assertTrue(isConnected);

			proxy.disableProxy();
			isConnected = client.isConnected();
			Assert.assertFalse(isConnected);

			proxy.enableProxy();
			// give it some time to reconnect
			long currentTime = System.nanoTime();
			long timeout = TimeUnit.SECONDS.toNanos(4);
			while (!client.isConnected()) {
				long now = System.nanoTime();
				if ((currentTime + timeout) < now) {
					System.err.println("AutomaticReconnectTest: timeout exceeded");
					break;
				}
				Thread.sleep(500);
			}
			isConnected = client.isConnected();
			Assert.assertTrue(isConnected);

			client.disconnect();
			Assert.assertFalse(client.isConnected());
		} finally {
			TestClients.closeQuietly(client);
		}
	}

	/**
	 * After the network drops, {@code reconnect()} reconnects the client once the network
	 * returns.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testManualReconnectAfterDisconnect() throws Exception {
		MqttConnectOptions options = TestClients.options(proxyURI, clientId);
		options.setCleanSession(true);
		options.setAutomaticReconnect(true);
		final MqttClient client = new MqttClient(options);
		try {
			proxy.enableProxy();
			client.connect(options);

			boolean isConnected = client.isConnected();
			Assert.assertTrue(isConnected);

			proxy.disableProxy();
			isConnected = client.isConnected();
			Assert.assertFalse(isConnected);

			proxy.enableProxy();
			client.reconnect();
			// give it some time to reconnect
			Thread.sleep(4000);
			isConnected = client.isConnected();
			Assert.assertTrue(isConnected);

			client.disconnect();
			Assert.assertFalse(client.isConnected());
		} finally {
			TestClients.closeQuietly(client);
		}
	}

	/**
	 * When the very first connect fails, the client does not keep retrying, even with
	 * automatic reconnect enabled.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testNoAutomaticReconnectWithNoInitialConnect() throws Exception {
		MqttConnectOptions options = TestClients.options(proxyURI, clientId);
		options.setCleanSession(true);
		options.setAutomaticReconnect(true);
		options.setConnectionTimeout(15);
		final MqttClient client = new MqttClient(options);
		try {
			// Make sure the proxy is disabled and give it a second to close everything down
			proxy.disableProxy();
			try {
				client.connect(options);
			} catch (MqttException ex) {
				// Exceptions are good in this case!
			}
			boolean isConnected = client.isConnected();
			Assert.assertFalse(isConnected);

			// Enable The Proxy
			proxy.enableProxy();

			// Give it some time to make sure we are still not connected
			long currentTime = System.nanoTime();
			long timeout = TimeUnit.SECONDS.toNanos(4);
			while (!client.isConnected()) {
				long now = System.nanoTime();
				if ((currentTime + timeout) < now) {
					Assert.assertFalse(isConnected);
					break;
				}
				Thread.sleep(500);
			}
			isConnected = client.isConnected();
			Assert.assertFalse(isConnected);
		} finally {
			TestClients.closeQuietly(client);
		}
	}
}
