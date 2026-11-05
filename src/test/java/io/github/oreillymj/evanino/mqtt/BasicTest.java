/*******************************************************************************
 * Ported from Eclipse Paho's Java client tests,
 * org/eclipse/paho/client/mqttv3/test/BasicTest.java,
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
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.UUID;

/**
 * Port of Paho's {@code BasicTest}: connect/disconnect, publish/subscribe, message and option
 * defaults, and thread clean-up. Requires a broker; see {@link MosquittoBroker}.
 */
public class BasicTest {

	/** The broker used by every test in this class. */
	@ClassRule
	public static final MosquittoBroker broker = new MosquittoBroker();

	/** Gives each test its own client ID. */
	@Rule
	public final TestName name = new TestName();

	private static URI serverURI;
	private static String topicPrefix;

	/**
	 * Picks up the broker URI and a unique topic prefix for the run.
	 */
	@BeforeClass
	public static void setUpBeforeClass() {
		serverURI = broker.getServerURI();
		topicPrefix = "BasicTest-" + UUID.randomUUID() + "-";
	}

	/**
	 * A client can connect, disconnect, and connect again.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testConnect() throws Exception {
		MqttClient client = null;
		try {
			String clientId = name.getMethodName();
			client = TestClients.createMqttClient(serverURI, clientId);

			client.connect(client.getOptions());

			Assert.assertEquals(clientId, client.getClientId());
			Assert.assertTrue(client.isConnected());
			Assert.assertNotNull(client.getServerURI());

			client.disconnect();
			Assert.assertFalse(client.isConnected());

			client.connect(client.getOptions());
			Assert.assertTrue(client.isConnected());

			client.disconnect();
		} catch (MqttException exception) {
			Assert.fail("Unexpected exception: " + exception);
		} finally {
			if (client != null) {
				client.close();
			}
		}
	}

	/**
	 * The client connects to the first reachable URI in {@code MqttConnectOptions.setServerURIs}:
	 * the client is created with a junk URI, and the first URI in the list has a host name that
	 * does not resolve, so only the second can connect.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testHAConnect() throws Exception {
		MqttClient client = null;
		try {
			String clientId = name.getMethodName();
			client = new MqttClient("tcp://junk:123", clientId);

			String[] urls = new String[]{"tcp://junk", serverURI.toString()};
			MqttConnectOptions options = new MqttConnectOptions();
			options.setClientId(clientId);
			options.setServerURIs(urls);

			client.connect(options);
			Assert.assertTrue(client.isConnected());
			Assert.assertEquals(serverURI.getPort(), URI.create(client.getServerURI()).getPort());

			client.disconnect();
		} finally {
			if (client != null) {
				client.close();
			}
		}
	}

	/**
	 * A message published through an {@link MqttTopic} arrives on a subscription to that topic.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testPubSub() throws Exception {
		MqttClient client = null;
		try {
			String topicStr = topicPrefix + "topic" + "_02";
			String clientId = name.getMethodName();
			client = TestClients.createMqttClient(serverURI, clientId);

			MessageListener listener = new MessageListener();
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
		} finally {
			if (client != null) {
				client.close();
			}
		}
	}

	/**
	 * A new message has the documented defaults, and its properties can be changed.
	 */
	@Test
	public void testMsgProperties() {
		MqttMessage msg = new MqttMessage();
		Assert.assertEquals(1, msg.getQos());
		Assert.assertFalse(msg.isDuplicate());
		Assert.assertFalse(msg.isRetained());
		Assert.assertNotNull(msg.getPayload());
		Assert.assertEquals(0, msg.getPayload().length);
		Assert.assertEquals("", msg.toString());

		msg = new MqttMessage("foo".getBytes(StandardCharsets.UTF_8));
		Assert.assertEquals(1, msg.getQos());
		Assert.assertFalse(msg.isDuplicate());
		Assert.assertFalse(msg.isRetained());
		Assert.assertEquals(3, msg.getPayload().length);
		Assert.assertEquals("foo", msg.toString());

		msg.setQos(0);
		Assert.assertEquals(0, msg.getQos());
		msg.setQos(1);
		Assert.assertEquals(1, msg.getQos());
		msg.setQos(2);
		Assert.assertEquals(2, msg.getQos());

		// Paho throws IllegalArgumentException; this client treats a QoS MQTT does not have as 0.
		msg.setQos(-1);
		Assert.assertEquals(0, msg.getQos());
		msg.setQos(2);
		msg.setQos(3);
		Assert.assertEquals(0, msg.getQos());

		msg.setPayload("foobar".getBytes(StandardCharsets.UTF_8));
		Assert.assertEquals(6, msg.getPayload().length);
		Assert.assertEquals("foobar", msg.toString());

		msg.clearPayload();
		Assert.assertNotNull(msg.getPayload());
		Assert.assertEquals(0, msg.getPayload().length);
		Assert.assertEquals("", msg.toString());

		msg.setRetained(true);
		Assert.assertTrue(msg.isRetained());
		msg.setRetained(false);
		Assert.assertFalse(msg.isRetained());
	}

	/**
	 * New connection options have the documented defaults. Paho also checks the socket
	 * factory, will destination/message and SSL properties; the equivalents here are the last
	 * will and the TLS context.
	 */
	@Test
	public void testConnOptDefaults() {
		MqttConnectOptions connOpts = new MqttConnectOptions();
		Assert.assertEquals(60, connOpts.getKeepAliveInterval());
		Assert.assertNull(connOpts.getPassword());
		Assert.assertNull(connOpts.getUserName());
		Assert.assertTrue(connOpts.isCleanSession());
		Assert.assertNull(connOpts.getLastWill());
		Assert.assertNull(connOpts.getSslContext());
	}

	/**
	 * Disconnecting while automatic reconnect is retrying an unreachable broker must not
	 * deadlock, and closing the client must release its threads (Paho issue 330). Paho uses
	 * {@code tcp://iot.eclipse.org:1882}; a closed local port gives the same unreachable broker
	 * without depending on the network.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 60_000)
	public void test330() throws Exception {
		int beforeThreadCount = Thread.activeCount();
		int deadPort;
		try (ServerSocket s = new ServerSocket(0)) {
			deadPort = s.getLocalPort();
		}
		MqttConnectOptions options = TestClients.options(new URI("tcp://127.0.0.1:" + deadPort), "client-1");
		options.setAutomaticReconnect(true);
		options.setUserName("foo");
		options.setPassword("bar".toCharArray());
		options.setConnectionTimeout(2);
		MqttAsyncClient client = new MqttAsyncClient(options);
		client.connect(options);

		Thread.sleep(1000);

		try {
			// this would deadlock before the Paho fix
			client.disconnect(0, null, null).waitForCompletion();
		} finally {
			client.shutdown();
		}

		Assert.assertEquals(beforeThreadCount, awaitThreadCount(beforeThreadCount, 20_000));
	}

	/**
	 * Connecting and disconnecting must not leak threads once the client is closed (Paho
	 * issue 402). Paho also expects no extra threads after {@code disconnect()}; this client
	 * deliberately keeps its event loop between connections, so the check is made after
	 * {@code close()}.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 60_000)
	public void test402() throws Exception {
		MqttClient client = null;
		int beforeThreadCount = Thread.activeCount();
		try {
			String clientId = name.getMethodName();
			client = TestClients.createMqttClient(serverURI, clientId);

			client.connect(client.getOptions());
			Assert.assertTrue(client.isConnected());
			client.disconnect();

			client.connect(client.getOptions());
			client.disconnect();
		} catch (MqttException exception) {
			Assert.fail("Unexpected exception: " + exception);
		} finally {
			if (client != null) {
				client.close();
			}
		}
		Assert.assertEquals(beforeThreadCount, awaitThreadCount(beforeThreadCount, 20_000));
	}

	/**
	 * Paho constructs the client with a caller-supplied {@code ScheduledExecutorService} and
	 * checks the pool's threads are the only ones left.
	 */
	@Ignore("This client does not accept a caller-supplied executor")
	@Test
	public void test402a() {
	}

	/**
	 * A client can be forcibly disconnected and then connect again.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testDisconnectForcibly() throws Exception {
		MqttClient client = null;
		try {
			String clientId = name.getMethodName();
			client = TestClients.createMqttClient(serverURI, clientId);

			client.connect(client.getOptions());
			Assert.assertTrue(client.isConnected());
			client.disconnectForcibly();

			client.connect(client.getOptions());
			Assert.assertTrue(client.isConnected());
			client.disconnectForcibly(2000, 2000);
		} catch (MqttException exception) {
			Assert.fail("Unexpected exception: " + exception);
		} finally {
			if (client != null) {
				client.close();
			}
		}
	}

	// -------------------------------------------------------------
	// Helper methods/classes
	// -------------------------------------------------------------

	/**
	 * Waits for the live thread count to drop back to a baseline, as shutting down Netty event
	 * loops and executors is asynchronous.
	 *
	 * @param baseline      the expected thread count
	 * @param timeoutMillis the maximum time to wait
	 * @return the thread count when it reached the baseline or the timeout expired
	 * @throws InterruptedException if interrupted while waiting
	 */
	static int awaitThreadCount(int baseline, long timeoutMillis) throws InterruptedException {
		long deadline = System.currentTimeMillis() + timeoutMillis;
		int count = Thread.activeCount();
		while (count > baseline && System.currentTimeMillis() < deadline) {
			Thread.sleep(100);
			count = Thread.activeCount();
		}
		if (count > baseline) {
			Thread[] threads = new Thread[count + 10];
			int n = Thread.enumerate(threads);
			for (int i = 0; i < n; i++) {
				System.err.println("BasicTest: live thread " + threads[i].getName());
			}
		}
		return count;
	}

	/**
	 * Collects incoming messages so a test can wait for the next one.
	 */
	static class MessageListener extends MqttCallbackAdapter {

		final ArrayList<MqttMessage> messages = new ArrayList<>();

		/**
		 * Waits up to five seconds for the next message (Paho waits one second).
		 *
		 * @return the next message, or {@code null} if none arrived
		 */
		public MqttMessage getNextMessage() {
			synchronized (messages) {
				if (messages.isEmpty()) {
					try {
						messages.wait(5000);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
				}
				if (messages.isEmpty()) {
					return null;
				}
				return messages.remove(0);
			}
		}

		/**
		 * Queues an incoming message.
		 *
		 * @param topic   the topic the message was published to
		 * @param message the received message
		 */
		@Override
		public void messageArrived(String topic, MqttMessage message) {
			synchronized (messages) {
				messages.add(message);
				messages.notifyAll();
			}
		}
	}
}
