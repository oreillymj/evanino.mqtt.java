/*******************************************************************************
 * Ported from Eclipse Paho's Java client tests,
 * org/eclipse/paho/client/mqttv3/test/SendReceiveAsyncTest.java,
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

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * Port of Paho's {@code SendReceiveAsyncTest}: publish/subscribe round trips with the
 * asynchronous client. Requires a broker; see {@link MosquittoBroker}.
 * <p>
 * API differences from Paho: {@code connect(null, null)} is {@code connect(null, null, null)}
 * (null options keep the client's current options), {@code disconnect(null, null)} is
 * {@code disconnect(0, null, null)}, {@code close()} is {@link MqttAsyncClient#shutdown()}, and
 * {@code publish(topic, bytes, qos, retained, context, callback)} goes through an
 * {@link MqttMessage}.
 */
public class SendReceiveAsyncTest {

	/** The broker used by every test in this class. */
	@ClassRule
	public static final MosquittoBroker broker = new MosquittoBroker();

	/** Gives each test its own client ID and topics. */
	@Rule
	public final TestName name = new TestName();

	private static final String className = SendReceiveAsyncTest.class.getName();
	private static URI serverURI;
	private static String topicPrefix;

	/**
	 * Picks up the broker URI and a unique topic prefix for the run.
	 */
	@BeforeClass
	public static void setUpBeforeClass() {
		serverURI = broker.getServerURI();
		topicPrefix = "SendReceiveAsyncTest-" + UUID.randomUUID() + "-";
	}

	/**
	 * A client can connect and disconnect twice, waiting on each token.
	 */
	@Test
	public void testConnect() {
		String methodName = name.getMethodName();
		MqttAsyncClient mqttClient = null;
		try {
			mqttClient = TestClients.createMqttAsyncClient(serverURI, methodName);

			IMqttToken connectToken = mqttClient.connect(null, null, null);
			connectToken.waitForCompletion();
			IMqttToken disconnectToken = mqttClient.disconnect(0, null, null);
			disconnectToken.waitForCompletion();

			connectToken = mqttClient.connect(null, null, null);
			connectToken.waitForCompletion();
			disconnectToken = mqttClient.disconnect(0, null, null);
			disconnectToken.waitForCompletion();
		} catch (Exception exception) {
			Assert.fail("Failed:" + methodName + " exception=" + exception);
		} finally {
			TestClients.shutdownQuietly(mqttClient);
		}
	}

	/**
	 * A client can connect and disconnect fifty times in a row.
	 */
	@Test(timeout = 120_000)
	public void testConAndDiscon() {
		MqttAsyncClient client = null;
		int maxLoopCount = 50;
		try {
			client = new MqttAsyncClient(serverURI.toString(), name.getMethodName());
			for (int i = 0; i < maxLoopCount; i++) {
				IMqttToken token = client.connect();
				token.waitForCompletion();
				token = client.disconnect();
				token.waitForCompletion();
			}
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Failed: " + e);
		} finally {
			if (client != null) {
				if (client.isConnected()) {
					client.disconnectForcibly();
				}
				TestClients.shutdownQuietly(client);
			}
		}
	}

	/**
	 * After a reconnect with {@code cleanSession=false}, a QoS 1 publish arrives on a QoS 0
	 * subscription at QoS 0.
	 */
	@Test
	public void testRemoteConnect() {
		String methodName = name.getMethodName();
		MqttAsyncClient mqttClient = null;
		try {
			mqttClient = TestClients.createMqttAsyncClient(serverURI, methodName);
			IMqttToken connectToken = mqttClient.connect(null, null, null);
			connectToken.waitForCompletion();
			IMqttToken disconnectToken = mqttClient.disconnect(0, null, null);
			disconnectToken.waitForCompletion();

			MqttV3Receiver mqttV3Receiver = new MqttV3Receiver();
			mqttClient.setCallback(mqttV3Receiver);
			MqttConnectOptions mqttConnectOptions = TestClients.options(serverURI, methodName);
			mqttConnectOptions.setCleanSession(false);
			connectToken = mqttClient.connect(mqttConnectOptions, null, null);
			connectToken.waitForCompletion();

			String[] topicNames = new String[]{topicPrefix + methodName + "/Topic"};
			int[] topicQos = {0};
			IMqttToken subToken = mqttClient.subscribe(topicNames, topicQos, null, null);
			subToken.waitForCompletion();

			byte[] payload = ("Message payload " + className + "." + methodName).getBytes(StandardCharsets.UTF_8);
			IMqttDeliveryToken pubToken = TestClients.publish(mqttClient, topicNames[0], payload, 1, false);
			pubToken.waitForCompletion();

			boolean ok = mqttV3Receiver.validateReceipt(topicNames[0], 0, payload);
			if (!ok) {
				Assert.fail("Receive failed");
			}

			disconnectToken = mqttClient.disconnect(0, null, null);
			disconnectToken.waitForCompletion();
		} catch (Exception exception) {
			Assert.fail("Failed:" + methodName + " exception=" + exception);
		} finally {
			TestClients.shutdownQuietly(mqttClient);
		}
	}

	/**
	 * A 1,000-byte payload makes the round trip intact, after an unsubscribe and resubscribe.
	 */
	@Test
	public void testLargeMessage() {
		String methodName = name.getMethodName();
		MqttAsyncClient mqttClient = null;
		try {
			mqttClient = TestClients.createMqttAsyncClient(serverURI, methodName);
			MqttV3Receiver mqttV3Receiver = new MqttV3Receiver();
			mqttClient.setCallback(mqttV3Receiver);
			IMqttToken connectToken = mqttClient.connect(null, null, null);
			connectToken.waitForCompletion();

			int largeSize = 1000;
			String[] topicNames = new String[]{topicPrefix + methodName + "/Topic"};
			int[] topicQos = {0};
			byte[] message = new byte[largeSize];
			Arrays.fill(message, (byte) 's');

			IMqttToken subToken = mqttClient.subscribe(topicNames, topicQos, null, null);
			subToken.waitForCompletion();
			IMqttToken unsubToken = mqttClient.unsubscribe(topicNames, null, null);
			unsubToken.waitForCompletion();
			subToken = mqttClient.subscribe(topicNames, topicQos, null, null);
			subToken.waitForCompletion();

			IMqttDeliveryToken pubToken = TestClients.publish(mqttClient, topicNames[0], message, 0, false);
			pubToken.waitForCompletion();

			boolean ok = mqttV3Receiver.validateReceipt(topicNames[0], 0, message);
			if (!ok) {
				Assert.fail("Receive failed");
			}
		} catch (Exception exception) {
			Assert.fail("Failed to instantiate:" + methodName + " exception=" + exception);
		} finally {
			TestClients.shutdownQuietly(mqttClient);
		}
	}

	/**
	 * Every message from two publishers reaches each of ten subscribers.
	 */
	@Test
	public void testMultipleClients() {
		String methodName = name.getMethodName();
		int publishers = 2;
		int subscribers = 10;
		MqttAsyncClient[] mqttPublisher = new MqttAsyncClient[publishers];
		MqttAsyncClient[] mqttSubscriber = new MqttAsyncClient[subscribers];
		try {
			String[] topicNames = new String[]{topicPrefix + methodName + "/Topic"};
			int[] topicQos = {0};

			for (int i = 0; i < mqttPublisher.length; i++) {
				mqttPublisher[i] = TestClients.createMqttAsyncClient(serverURI, "MultiPub" + i);
				mqttPublisher[i].connect(null, null, null).waitForCompletion();
			}

			MqttV3Receiver[] mqttV3Receiver = new MqttV3Receiver[mqttSubscriber.length];
			for (int i = 0; i < mqttSubscriber.length; i++) {
				mqttSubscriber[i] = TestClients.createMqttAsyncClient(serverURI, "MultiSubscriber" + i);
				mqttV3Receiver[i] = new MqttV3Receiver();
				mqttSubscriber[i].setCallback(mqttV3Receiver[i]);
				mqttSubscriber[i].connect(null, null, null).waitForCompletion();
				mqttSubscriber[i].subscribe(topicNames, topicQos, null, null).waitForCompletion();
			}

			for (int iMessage = 0; iMessage < 10; iMessage++) {
				byte[] payload = ("Message " + iMessage).getBytes(StandardCharsets.UTF_8);
				for (MqttAsyncClient publisher : mqttPublisher) {
					TestClients.publish(publisher, topicNames[0], payload, 0, false).waitForCompletion();
				}

				for (MqttV3Receiver receiver : mqttV3Receiver) {
					for (int ii = 0; ii < mqttPublisher.length; ii++) {
						boolean ok = receiver.validateReceipt(topicNames[0], 0, payload);
						if (!ok) {
							Assert.fail("Receive failed");
						}
					}
				}
			}
		} catch (Exception exception) {
			Assert.fail("Failed to instantiate:" + methodName + " exception=" + exception);
		} finally {
			for (MqttAsyncClient publisher : mqttPublisher) {
				TestClients.shutdownQuietly(publisher);
			}
			for (MqttAsyncClient subscriber : mqttSubscriber) {
				TestClients.shutdownQuietly(subscriber);
			}
		}
	}

	/**
	 * A {@code cleanSession=false} subscription receives messages, and reconnecting with
	 * {@code cleanSession=true} discards both the queued messages and the subscription.
	 */
	@Test
	public void testCleanStart() {
		String methodName = name.getMethodName();
		MqttAsyncClient mqttClient = null;
		try {
			mqttClient = TestClients.createMqttAsyncClient(serverURI, methodName);
			MqttV3Receiver mqttV3Receiver = new MqttV3Receiver();
			mqttClient.setCallback(mqttV3Receiver);
			MqttConnectOptions mqttConnectOptions = TestClients.options(serverURI, methodName);
			mqttConnectOptions.setCleanSession(false);
			mqttClient.connect(mqttConnectOptions, null, null).waitForCompletion();

			String[] topicNames = new String[]{topicPrefix + methodName + "/Topic"};
			int[] topicQos = {0};
			mqttClient.subscribe(topicNames, topicQos, null, null).waitForCompletion();

			byte[] payload = ("Message payload " + className + "." + methodName + " First").getBytes(StandardCharsets.UTF_8);
			TestClients.publish(mqttClient, topicNames[0], payload, 1, false).waitForCompletion();
			boolean ok = mqttV3Receiver.validateReceipt(topicNames[0], 0, payload);
			if (!ok) {
				Assert.fail("Receive failed");
			}

			// Disconnect and reconnect to make sure the subscription and all queued messages are cleared.
			mqttClient.disconnect(0, null, null).waitForCompletion();
			mqttClient.shutdown();

			// Send a message from another client, to our durable subscription.
			mqttClient = TestClients.createMqttAsyncClient(serverURI, methodName + "Other");
			mqttV3Receiver = new MqttV3Receiver();
			mqttClient.setCallback(mqttV3Receiver);
			mqttConnectOptions = TestClients.options(serverURI, methodName + "Other");
			mqttConnectOptions.setCleanSession(true);
			mqttClient.connect(mqttConnectOptions, null, null).waitForCompletion();
			// Receive the publication so that we can be sure the first client has also received it.
			// Otherwise the first client may reconnect with its clean session before the message has arrived.
			mqttClient.subscribe(topicNames, topicQos, null, null).waitForCompletion();
			payload = ("Message payload " + className + "." + methodName + " Other client").getBytes(StandardCharsets.UTF_8);
			TestClients.publish(mqttClient, topicNames[0], payload, 1, false).waitForCompletion();
			ok = mqttV3Receiver.validateReceipt(topicNames[0], 0, payload);
			if (!ok) {
				Assert.fail("Receive failed");
			}
			mqttClient.disconnect(0, null, null).waitForCompletion();
			mqttClient.shutdown();

			// Reconnect and check we have no messages.
			mqttClient = TestClients.createMqttAsyncClient(serverURI, methodName);
			mqttV3Receiver = new MqttV3Receiver();
			mqttClient.setCallback(mqttV3Receiver);
			mqttConnectOptions = TestClients.options(serverURI, methodName);
			mqttConnectOptions.setCleanSession(true);
			mqttClient.connect(mqttConnectOptions, null, null).waitForCompletion();
			MqttV3Receiver.ReceivedMessage receivedMessage = mqttV3Receiver.receiveNext(100);
			if (receivedMessage != null) {
				Assert.fail("Receive messaqe:" + new String(receivedMessage.message.getPayload(), StandardCharsets.UTF_8));
			}

			// Also check that subscription is cancelled.
			payload = ("Message payload " + className + "." + methodName + " Cancelled Subscription").getBytes(StandardCharsets.UTF_8);
			TestClients.publish(mqttClient, topicNames[0], payload, 1, false).waitForCompletion();
			receivedMessage = mqttV3Receiver.receiveNext(100);
			if (receivedMessage != null) {
				Assert.fail("Receive messaqe:" + new String(receivedMessage.message.getPayload(), StandardCharsets.UTF_8));
			}
		} catch (Exception exception) {
			Assert.fail("Failed:" + methodName + " exception=" + exception);
		} finally {
			TestClients.shutdownQuietly(mqttClient);
		}
	}

	/**
	 * A 20 MB payload makes the round trip with a 30 second keep-alive. The decoder accepts
	 * 64 KB packets by default, so the limit is raised for this test.
	 */
	@Test(timeout = 120_000)
	public void testVeryLargeMessageWithShortKeepAlive() {
		String methodName = name.getMethodName();
		MqttAsyncClient mqttClient = null;
		try {
			int largeSize = 20 * (1 << 20);
			MqttConnectOptions options = TestClients.options(serverURI, "testVeryLargeMessage");
			options.setKeepAliveInterval(30);
			options.setMaxBytesInMessage(largeSize + 1024);
			mqttClient = new MqttAsyncClient(options);
			MqttV3Receiver mqttV3Receiver = new MqttV3Receiver();
			mqttClient.setCallback(mqttV3Receiver);
			mqttClient.connect(options).waitForCompletion();

			String topic = topicPrefix + "testLargeMsg/Topic";
			byte[] message = new byte[largeSize];
			Arrays.fill(message, (byte) 's');

			mqttClient.subscribe(topic, 0).waitForCompletion();
			TestClients.publish(mqttClient, topic, message, 0, false).waitForCompletion();

			boolean ok = mqttV3Receiver.validateReceipt(topic, 0, message);
			if (!ok) {
				Assert.fail("Receive failed");
			}
		} catch (Exception exception) {
			Assert.fail("Failed to instantiate:" + methodName + " exception=" + exception);
		} finally {
			TestClients.shutdownQuietly(mqttClient);
		}
	}

	/**
	 * Waiting on a connect to a server that accepts TCP but never answers MQTT times out with
	 * an {@link MqttException}, and the client can be forcibly disconnected and retried. Paho
	 * uses {@code tcp://iot.eclipse.org:22} (an SSH port); a local socket that never replies
	 * does the same without the network.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 60_000)
	public void testConnectTimeout() throws Exception {
		List<Socket> accepted = new ArrayList<>();
		try (ServerSocket silent = new ServerSocket(0)) {
			Thread acceptor = new Thread(() -> {
				try {
					while (true) {
						accepted.add(silent.accept());
					}
				} catch (Exception closed) {
					// server socket closed at the end of the test
				}
			}, "silent-server");
			acceptor.setDaemon(true);
			acceptor.start();

			URI uri = new URI("tcp://127.0.0.1:" + silent.getLocalPort());
			MqttAsyncClient mqttClient = null;
			IMqttToken connectToken;
			try {
				mqttClient = TestClients.createMqttAsyncClient(uri, name.getMethodName());
				connectToken = mqttClient.connect(TestClients.options(uri, name.getMethodName()));
				connectToken.waitForCompletion(5000);
				Assert.fail("Should throw an timeout exception.");
			} catch (Exception exception) {
				Assert.assertTrue("Expected MqttException but got " + exception, exception instanceof MqttException);
			} finally {
				if (mqttClient != null) {
					mqttClient.disconnectForcibly(5000, 5000);
				}
			}

			try {
				connectToken = mqttClient.connect(TestClients.options(uri, name.getMethodName()));
				connectToken.waitForCompletion(5000);
			} catch (Exception exception) {
				Assert.assertTrue("Expected MqttException but got " + exception, exception instanceof MqttException);
			} finally {
				mqttClient.disconnectForcibly(5000, 5000);
				Assert.assertFalse(mqttClient.isConnected());
				TestClients.shutdownQuietly(mqttClient);
			}
		} finally {
			for (Socket s : accepted) {
				s.close();
			}
		}
	}

	/**
	 * A thousand QoS 0 publishes each get a token, and all thousand messages arrive.
	 * Paho also calls {@code setMaxInflight(1000)}, which is a no-op in this client.
	 */
	@Test(timeout = 120_000)
	public void testQoS0Tokens() {
		String methodName = name.getMethodName();
		int tokenCount = 1000;  // how many QoS 0 tokens shall we track?
		MqttAsyncClient mqttClient = null;
		try {
			mqttClient = TestClients.createMqttAsyncClient(serverURI, methodName);
			IMqttDeliveryToken[] pubTokens = new IMqttDeliveryToken[tokenCount];
			MqttV3Receiver mqttV3Receiver = new MqttV3Receiver();
			mqttClient.setCallback(mqttV3Receiver);
			MqttConnectOptions opts = TestClients.options(serverURI, methodName);
			opts.setMaxInflight(tokenCount);
			mqttClient.connect(opts).waitForCompletion();

			String[] topicNames = new String[]{topicPrefix + methodName + "/Topic"};
			mqttClient.subscribe(topicNames[0], 2).waitForCompletion();

			for (int i = 0; i < tokenCount; ++i) {
				try {
					pubTokens[i] = mqttClient.publish(topicNames[0], "message".getBytes(StandardCharsets.UTF_8), 0, false);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			int errors = 0;
			for (int i = 0; i < tokenCount; ++i) {
				try {
					pubTokens[i].waitForCompletion(10);
				} catch (Exception e) {
					errors += 1;
				}
			}
			Assert.assertEquals("QoS 0 tokens that did not complete", 0, errors);

			while (mqttV3Receiver.receivedMessageCount() < tokenCount) {
				Thread.sleep(10);
			}
		} catch (Exception exception) {
			Assert.fail("Exception:" + methodName + " exception=" + exception);
		} finally {
			TestClients.shutdownQuietly(mqttClient);
		}
	}

	/**
	 * Seventy thousand QoS 0 publishes complete, and the client can then be forcibly
	 * disconnected.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 300_000)
	public void testPublishManyQoS0Messages() throws Exception {
		String clientId = name.getMethodName();
		MqttAsyncClient asyncClient = new MqttAsyncClient(serverURI.toString(), clientId);
		try {
			IMqttToken connectToken = asyncClient.connect();
			connectToken.waitForCompletion(5000);
			Assert.assertEquals(clientId, asyncClient.getClientId());
			Assert.assertTrue(asyncClient.isConnected());

			MqttMessage testMessage = new MqttMessage("Test Payload".getBytes(StandardCharsets.UTF_8));
			testMessage.setQos(0);
			testMessage.setRetained(false);

			int noOfMessages = 70000;
			int failedWaits = 0;
			for (int i = 0; i < noOfMessages; i++) {
				IMqttDeliveryToken deliveryToken = asyncClient.publish(topicPrefix + clientId, testMessage);
				try {
					deliveryToken.waitForCompletion(5000);
				} catch (Exception e) {
					failedWaits++;
				}
			}
			Assert.assertEquals("QoS 0 publishes whose wait failed", 0, failedWaits);

			asyncClient.disconnectForcibly(0, 2000);
			Assert.assertFalse(asyncClient.isConnected());
		} finally {
			TestClients.shutdownQuietly(asyncClient);
		}
	}
}
