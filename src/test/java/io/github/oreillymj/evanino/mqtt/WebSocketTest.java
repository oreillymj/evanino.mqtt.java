/*******************************************************************************
 * Ported from Eclipse Paho's Java client tests,
 * org/eclipse/paho/client/mqttv3/test/WebSocketTest.java,
 * https://github.com/eclipse-paho/paho.mqtt.java. This project uses the Paho code under
 * the Eclipse Distribution License v1.0 (see licenses/EDL-1.0.txt).
 *
 * The original notice:
 *
 *   Copyright (c) 2009, 2014 IBM Corp.
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

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;

/**
 * Port of Paho's {@code WebSocketTest}: connect, publish/subscribe and a large binary payload
 * over a WebSocket connection. Requires a broker with a WebSocket listener; see
 * {@link MosquittoBroker}.
 */
public class WebSocketTest {

	/** The broker used by every test in this class. */
	@ClassRule
	public static final MosquittoBroker broker = new MosquittoBroker();

	/** Gives each test its own client ID. */
	@Rule
	public final TestName name = new TestName();

	private static URI serverURI;
	private static String topicPrefix;

	/**
	 * Picks up the WebSocket broker URI and a unique topic prefix for the run.
	 */
	@BeforeClass
	public static void setUpBeforeClass() {
		serverURI = broker.getWebSocketServerURI();
		topicPrefix = "WebSocketTest-" + UUID.randomUUID() + "-";
	}

	/**
	 * A client can connect, disconnect and connect again over WebSockets.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testWebSocketConnect() throws Exception {
		MqttClient client = null;
		try {
			String clientId = name.getMethodName();
			client = TestClients.createMqttClient(serverURI, clientId);
			client.connect(client.getOptions());

			Assert.assertEquals(clientId, client.getClientId());
			Assert.assertTrue(client.isConnected());
			Assert.assertNotNull(client.getServerURI());

			client.disconnect();
			client.connect(client.getOptions());
			client.disconnect();
		} catch (MqttException exception) {
			Assert.fail("Unexpected exception: " + exception);
		} finally {
			closeForcibly(client);
		}
	}

	/**
	 * A message published through an {@link MqttTopic} arrives on a subscription, over
	 * WebSockets.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testWebSocketPubSub() throws Exception {
		MqttClient client = null;
		try {
			String topicStr = topicPrefix + "topic" + "_02";
			String clientId = name.getMethodName();
			client = TestClients.createMqttClient(serverURI, clientId);
			BasicTest.MessageListener listener = new BasicTest.MessageListener();
			client.setCallback(listener);
			client.connect(client.getOptions());
			client.subscribe(topicStr, 1);

			MqttTopic topic = client.getTopic(topicStr);
			MqttMessage message = new MqttMessage("foo".getBytes(StandardCharsets.UTF_8));
			topic.publish(message);

			MqttMessage msg = listener.getNextMessage();
			Assert.assertNotNull(msg);
			Assert.assertEquals("foo", msg.toString());

			String topicName = topic.getName();
			Assert.assertEquals(topicName, topicStr);

			client.disconnect();
		} catch (MqttException exception) {
			Assert.fail("Unexpected exception: " + exception);
		} finally {
			closeForcibly(client);
		}
	}

	/**
	 * A 32,000-byte random binary payload arrives byte-for-byte over WebSockets.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 10_000)
	public void largePayloadTest() throws Exception {
		// Generate large byte array;
		byte[] largeByteArray = new byte[32000];
		new Random().nextBytes(largeByteArray);
		MqttClient client = null;
		try {
			String topicStr = UUID.randomUUID() + "/topic_largeFile_01";
			String clientId = name.getMethodName();
			client = TestClients.createMqttClient(serverURI, clientId);
			BasicTest.MessageListener listener = new BasicTest.MessageListener();
			client.setCallback(listener);
			client.connect(client.getOptions());
			client.subscribe(topicStr, 1);

			MqttTopic topic = client.getTopic(topicStr);
			MqttMessage message = new MqttMessage(largeByteArray);
			topic.publish(message);

			MqttMessage msg = listener.getNextMessage();
			Assert.assertNotNull(msg);
			Assert.assertTrue("payload must arrive unchanged", Arrays.equals(largeByteArray, msg.getPayload()));
			client.disconnect();
		} catch (MqttException exception) {
			Assert.fail("Unexpected exception: " + exception);
		} finally {
			closeForcibly(client);
		}
	}

	/**
	 * A client can connect when the server URI carries user info
	 * ({@code ws://username:password@host:port}).
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 10_000)
	public void testBasicAuth() throws Exception {
		String userInfo = "username:password";
		String clientId = name.getMethodName();
		URI serverURIWithUserInfo = new URI(serverURI.getScheme(),
				userInfo,
				serverURI.getHost(),
				serverURI.getPort(),
				serverURI.getPath(),
				serverURI.getQuery(),
				serverURI.getFragment());
		MqttClient client = null;
		try {
			client = TestClients.createMqttClient(serverURIWithUserInfo, clientId);
			client.connect(client.getOptions());
			client.disconnect();
		} catch (MqttException exception) {
			Assert.fail("Unexpected exception: " + exception);
		} finally {
			closeForcibly(client);
		}
	}

	/**
	 * Forcibly disconnects a client if it is still connected, then closes it.
	 *
	 * @param client the client, or {@code null}
	 */
	private static void closeForcibly(MqttClient client) {
		if (client != null) {
			try {
				if (client.isConnected()) {
					client.disconnectForcibly();
				}
			} catch (Exception ignored) {
			}
			TestClients.closeQuietly(client);
		}
	}
}
