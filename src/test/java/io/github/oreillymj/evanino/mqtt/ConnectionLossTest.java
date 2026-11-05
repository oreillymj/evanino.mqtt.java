/*******************************************************************************
 * Ported from Eclipse Paho's Java client tests,
 * org/eclipse/paho/client/mqttv3/test/connectionLoss/ConnectionLossTest.java,
 * https://github.com/eclipse-paho/paho.mqtt.java. This project uses the Paho code under
 * the Eclipse Distribution License v1.0 (see licenses/EDL-1.0.txt).
 *
 * The original notice:
 *
 *   Copyright (c) 2009, 2019 IBM Corp.
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

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Port of Paho's {@code ConnectionLossTest}: the client notices when the network drops while
 * publishing or idle, and keep-alive pings hold an idle or busy connection open. The network
 * is dropped with a {@link ConnectionManipulationProxyServer} between the client and the
 * broker. Requires a broker; see {@link MosquittoBroker}.
 * <p>
 * These tests are slow by design: each runs for up to ten keep-alive intervals (150 s).
 * Paho also gives the client a file persistence store; this client has no persistence
 * option, so none is used.
 */
public class ConnectionLossTest {

	/** The broker used by every test in this class. */
	@ClassRule
	public static final MosquittoBroker broker = new MosquittoBroker();

	private static ConnectionManipulationProxyServer proxy;
	private static URI proxyURI;

	private final String username = "username";
	private final char[] password = "password".toCharArray();
	private final String clientId = "device-client-id";
	private final String message = "12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890";

	private volatile boolean connectionLostCalled = false;

	/** Records connection-lost events, in place of Paho's test class implementing MqttCallback. */
	private final MqttCallback callback = new MqttCallbackAdapter() {
		/** {@inheritDoc} */
		@Override
		public void connectionLost(Throwable cause) {
			connectionLostCalled = true;
		}
	};

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
	 * Drops the network after each test.
	 */
	@After
	public void afterTest() {
		proxy.disableProxy();
	}

	/**
	 * The client notices the network dropping while it publishes at QoS 0.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 120_000)
	public void testConnectionLossWhilePublishingQos0() throws Exception {
		connectionLossWhilePublishing(0);
	}

	/**
	 * The client notices the network dropping while it publishes at QoS 1.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 120_000)
	public void testConnectionLossWhilePublishingQos1() throws Exception {
		connectionLossWhilePublishing(1);
	}

	/**
	 * The client notices the network dropping while it publishes at QoS 2.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 120_000)
	public void testConnectionLossWhilePublishingQos2() throws Exception {
		connectionLossWhilePublishing(2);
	}

	/**
	 * The asynchronous client notices the network dropping while it publishes at QoS 1.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 240_000)
	public void testConnectionLossWhilePublishingQos1Async() throws Exception {
		final int keepAlive = 15;
		MqttConnectOptions options = options(keepAlive);
		MqttAsyncClient client = new MqttAsyncClient(options);
		try {
			client.setCallback(callback);
			proxy.enableProxy();
			client.connect(options);
			while (!client.isConnected()) {
				Thread.sleep(1000);
			}

			for (int i = 0; i < 10; i++) {
				client.publish(topic(), message.getBytes(StandardCharsets.UTF_8), 1, false);
				Thread.sleep(1000);
			}

			proxy.disableProxy();
			AtomicInteger res = new AtomicInteger();
			Timer timer = startSecondsCounter(res);
			try {
				boolean stopPublishing = false;
				while (client.isConnected() && res.get() < 10 * keepAlive) {
					if (!stopPublishing) {
						try {
							client.publish(topic(), message.getBytes(StandardCharsets.UTF_8), 1, false);
							Thread.sleep(1000);
						} catch (MqttException e) {
							stopPublishing = true;
						}
					}
				}
			} finally {
				timer.cancel();
			}
			Assert.assertFalse("Disconected", client.isConnected());
		} finally {
			if (client.isConnected()) {
				client.disconnect(0, null, null);
			}
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * Publishing at QoS 0 once a second keeps the connection open for ten keep-alive intervals.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 240_000)
	public void testKeepConnectionOpenWhilePublishingQos0() throws Exception {
		final int keepAlive = 15;
		MqttConnectOptions options = options(keepAlive);
		MqttClient client = new MqttClient(options);
		try {
			client.setCallback(callback);
			proxy.enableProxy();
			client.connect(options);

			AtomicInteger res = new AtomicInteger();
			Timer timer = startSecondsCounter(res);
			try {
				while (client.isConnected() && res.get() < 10 * keepAlive) {
					client.publish(topic(), message.getBytes(StandardCharsets.UTF_8), 0, false);
					Thread.sleep(1000);
				}
			} finally {
				timer.cancel();
			}
			Assert.assertTrue("Connected", client.isConnected());
		} finally {
			if (client.isConnected()) {
				client.disconnect(0);
			}
			client.close();
		}
	}

	/**
	 * Keep-alive pings hold an idle connection open for ten keep-alive intervals.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 240_000)
	public void testKeepConnectionOpenIdle() throws Exception {
		final int keepAlive = 15;
		MqttConnectOptions options = options(keepAlive);
		MqttClient client = new MqttClient(options);
		try {
			client.setCallback(callback);
			proxy.enableProxy();
			client.connect(options);

			AtomicInteger res = new AtomicInteger();
			Timer timer = startSecondsCounter(res);
			try {
				while (client.isConnected() && res.get() < 10 * keepAlive) {
					Thread.sleep(1000);
				}
			} finally {
				timer.cancel();
			}
			Assert.assertTrue("Connected", client.isConnected());
		} finally {
			if (client.isConnected()) {
				client.disconnect(0);
			}
			client.close();
		}
	}

	/**
	 * An idle client notices the network dropping and calls connectionLost.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 120_000)
	public void testConnectionLossUsingPing() throws Exception {
		final int keepAlive = 15;
		MqttConnectOptions options = options(keepAlive);
		connectionLostCalled = false;
		MqttClient client = new MqttClient(options);
		try {
			client.setCallback(callback);
			proxy.enableProxy();
			client.connect(options);
			Thread.sleep(1000);
			proxy.disableProxy();

			int count = 0;
			while (client.isConnected() && ++count < 2 * keepAlive) {
				Thread.sleep(1000);
			}
			// connectionLost is delivered on another thread; allow it a moment to arrive
			for (int i = 0; i < 50 && !connectionLostCalled; i++) {
				Thread.sleep(100);
			}

			Assert.assertTrue("Connection lost was not called", connectionLostCalled);
			Assert.assertFalse("Disconnected", client.isConnected());
		} finally {
			if (client.isConnected()) {
				client.disconnect(0);
			}
			client.close();
		}
	}

	// -------------------------------------------------------------
	// Helper methods
	// -------------------------------------------------------------

	/**
	 * Runs Paho's shared connection-loss scenario: publish ten messages, drop the network,
	 * then keep publishing until the client notices or two keep-alive intervals pass.
	 *
	 * @param qos the QoS to publish at
	 * @throws Exception if the test fails unexpectedly
	 */
	private void connectionLossWhilePublishing(int qos) throws Exception {
		final int keepAlive = 15;
		MqttConnectOptions options = options(keepAlive);
		MqttClient client = new MqttClient(options);
		try {
			client.setCallback(callback);
			proxy.enableProxy();
			client.connect(options);

			for (int i = 0; i < 10; i++) {
				client.publish(topic(), message.getBytes(StandardCharsets.UTF_8), qos, false);
				Thread.sleep(1000);
			}

			proxy.disableProxy();
			AtomicInteger res = new AtomicInteger();
			Timer timer = startSecondsCounter(res);
			try {
				while (client.isConnected() && res.get() < 2 * keepAlive) {
					try {
						client.publish(topic(), message.getBytes(StandardCharsets.UTF_8), qos, false);
						Thread.sleep(1000);
					} catch (MqttException e) {
						// ignore
					}
				}
			} finally {
				timer.cancel();
			}
			Assert.assertFalse("Disconected", client.isConnected());
		} finally {
			if (client.isConnected()) {
				client.disconnect(0);
			}
			client.close();
		}
	}

	/**
	 * Creates the connection options Paho uses in every test of this class.
	 *
	 * @param keepAlive the keep-alive interval in seconds
	 * @return new connection options for the proxy
	 */
	private MqttConnectOptions options(int keepAlive) {
		MqttConnectOptions options = TestClients.options(proxyURI, clientId);
		options.setCleanSession(true);
		options.setUserName(username);
		options.setPassword(password);
		options.setKeepAliveInterval(keepAlive);
		return options;
	}

	/**
	 * Returns the topic Paho publishes to.
	 *
	 * @return the topic name
	 */
	private String topic() {
		return username + "/" + clientId + "/abc";
	}

	/**
	 * Starts a timer that adds one to a counter every second, as Paho's tests do.
	 *
	 * @param counter the counter to advance
	 * @return the timer, which the caller must cancel
	 */
	private static Timer startSecondsCounter(AtomicInteger counter) {
		Timer timer = new Timer("seconds-counter", true);
		timer.schedule(new TimerTask() {
			/** Advances the counter. */
			@Override
			public void run() {
				counter.incrementAndGet();
			}
		}, 0, 1000);
		return timer;
	}
}
