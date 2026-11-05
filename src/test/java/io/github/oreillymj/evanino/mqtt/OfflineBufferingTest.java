/*******************************************************************************
 * Ported from Eclipse Paho's Java client tests,
 * org/eclipse/paho/client/mqttv3/test/automaticReconnect/OfflineBufferingTest.java,
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
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Port of Paho's {@code OfflineBufferingTest}: messages published while the client is offline
 * are delivered once it reconnects. The network is dropped with a
 * {@link ConnectionManipulationProxyServer}. Requires a broker; see {@link MosquittoBroker}.
 * <p>
 * Paho turns buffering on with {@code DisconnectedBufferOptions}, which this client does not
 * have. Instead it always holds QoS 1 and 2 publishes made while disconnected and sends them
 * when the connection is re-established, so the two basic cases are tested against that
 * behaviour. The cases that depend on buffer size limits, deleting the oldest buffered message
 * or persisting the buffer are ignored.
 */
public class OfflineBufferingTest {

	/** The broker used by every test in this class. */
	@ClassRule
	public static final MosquittoBroker broker = new MosquittoBroker();

	/** Gives each test its own client ID and topic. */
	@Rule
	public final TestName name = new TestName();

	private static ConnectionManipulationProxyServer proxy;
	private static URI proxyURI;
	private static String topicPrefix;

	/**
	 * Starts the proxy in front of the broker.
	 *
	 * @throws Exception if the proxy cannot be started
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		URI serverURI = broker.getServerURI();
		topicPrefix = "OfflineBufferingTest-" + UUID.randomUUID() + "-";
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
	 * A message published while offline is held, and completes once the client has
	 * automatically reconnected.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 60_000)
	public void testSingleMessageBufferAndDeliver() throws Exception {
		String methodName = name.getMethodName();
		MqttConnectOptions options = TestClients.options(proxyURI, methodName);
		options.setCleanSession(true);
		options.setAutomaticReconnect(true);
		MqttAsyncClient client = new MqttAsyncClient(options);
		try {
			proxy.enableProxy();
			IMqttToken connectToken = client.connect(options);
			connectToken.waitForCompletion(5000);
			boolean isConnected = client.isConnected();
			Assert.assertTrue(isConnected);

			proxy.disableProxy();
			isConnected = client.isConnected();
			Assert.assertFalse(isConnected);

			IMqttDeliveryToken pubToken = client.publish(topicPrefix + methodName,
					new MqttMessage(methodName.getBytes(StandardCharsets.UTF_8)));
			Assert.assertFalse(pubToken.isComplete());

			proxy.enableProxy();
			pubToken.waitForCompletion(5000);

			long currentTime = System.nanoTime();
			long timeout = TimeUnit.SECONDS.toNanos(4);
			while (!client.isConnected()) {
				long now = System.nanoTime();
				if ((currentTime + timeout) < now) {
					break;
				}
				Thread.sleep(500);
			}
			isConnected = client.isConnected();
			Assert.assertTrue(isConnected);
			Assert.assertTrue(pubToken.isComplete());

			client.disconnect().waitForCompletion(5000);
		} finally {
			TestClients.shutdownQuietly(client);
			proxy.disableProxy();
		}
	}

	/**
	 * A hundred messages published while offline are all delivered, in order, to a subscriber
	 * once the publisher has automatically reconnected.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 120_000)
	public void testManyMessageBufferAndDeliver() throws Exception {
		String methodName = name.getMethodName();
		MqttConnectOptions options = TestClients.options(proxyURI, methodName);
		options.setCleanSession(true);
		options.setAutomaticReconnect(true);
		MqttAsyncClient client = new MqttAsyncClient(options);

		MqttV3Receiver mqttV3Receiver = new MqttV3Receiver();
		MqttAsyncClient subClient = TestClients.createMqttAsyncClient(broker.getServerURI(), methodName + "sub-client");
		try {
			subClient.setCallback(mqttV3Receiver);
			subClient.connect().waitForCompletion(5000);
			subClient.subscribe(topicPrefix + methodName, 1).waitForCompletion(5000);

			proxy.enableProxy();
			client.connect(options).waitForCompletion(5000);
			boolean isConnected = client.isConnected();
			Assert.assertTrue(isConnected);

			proxy.disableProxy();
			isConnected = client.isConnected();
			Assert.assertFalse(isConnected);

			int msgCount = 100;
			for (int x = 0; x < msgCount; x++) {
				client.publish(topicPrefix + methodName, new MqttMessage(Integer.toString(x).getBytes(StandardCharsets.UTF_8)));
			}

			proxy.enableProxy();

			long currentTime = System.nanoTime();
			long timeout = TimeUnit.SECONDS.toNanos(8);
			while (!client.isConnected()) {
				long now = System.nanoTime();
				if ((currentTime + timeout) < now) {
					break;
				}
				Thread.sleep(500);
			}
			isConnected = client.isConnected();
			Assert.assertTrue(isConnected);

			Thread.sleep(5000);

			for (int x = 0; x < msgCount; x++) {
				boolean received = mqttV3Receiver.validateReceipt(topicPrefix + methodName, 1,
						Integer.toString(x).getBytes(StandardCharsets.UTF_8));
				Assert.assertTrue("message " + x + " not received", received);
			}

			client.disconnect().waitForCompletion(5000);
			subClient.disconnect().waitForCompletion(5000);
		} finally {
			TestClients.shutdownQuietly(client);
			TestClients.shutdownQuietly(subClient);
			proxy.disableProxy();
		}
	}

	/**
	 * Paho drops the oldest buffered message when the buffer is full.
	 */
	@Ignore("Needs DisconnectedBufferOptions (buffer size, delete oldest), which this client does not have")
	@Test
	public void testDeleteOldestBufferedMessages() {
	}

	/**
	 * Paho drops the oldest buffered message when a persisted buffer is full.
	 */
	@Ignore("Needs DisconnectedBufferOptions and client persistence, which this client does not have")
	@Test
	public void testDeleteOldestBufferedMessagesWithPersistance() {
	}

	/**
	 * Paho rejects new messages when the buffer is full and deleting the oldest is off.
	 */
	@Ignore("Needs DisconnectedBufferOptions (buffer size, delete oldest), which this client does not have")
	@Test
	public void testNoDeleteOldestBufferedMessages() {
	}

	/**
	 * Paho persists buffered messages and delivers them after the client is recreated.
	 */
	@Ignore("Needs DisconnectedBufferOptions and client persistence, which this client does not have")
	@Test
	public void testPersistBufferedMessages() {
	}

	/**
	 * Paho delivers persisted buffered messages from a new client instance.
	 */
	@Ignore("Needs DisconnectedBufferOptions and client persistence, which this client does not have")
	@Test
	public void testUnPersistBufferedMessagesOnNewClient() {
	}
}
