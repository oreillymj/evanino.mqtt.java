/*******************************************************************************
 * Ported from Eclipse Paho's Java client tests,
 * org/eclipse/paho/client/mqttv3/test/SendReceiveTest.java,
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

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.UUID;

/**
 * Port of Paho's {@code SendReceiveTest}: publish/subscribe round trips with the blocking
 * client. Requires a broker; see {@link MosquittoBroker}.
 * <p>
 * Paho's {@code IMqttClient} has {@code subscribe(String[], int[])} and
 * {@code unsubscribe(String[])}; the equivalents here are
 * {@link MqttClient#subscribeWithResponse(String[], int[], IMqttMessageListener[])} and one
 * {@link MqttClient#unsubscribe(String)} per topic.
 */
public class SendReceiveTest {

	/** The broker used by every test in this class. */
	@ClassRule
	public static final MosquittoBroker broker = new MosquittoBroker();

	/** Gives each test its own client ID and topics. */
	@Rule
	public final TestName name = new TestName();

	private static final String className = SendReceiveTest.class.getName();
	private static URI serverURI;
	private static String topicPrefix;

	/**
	 * Picks up the broker URI and a unique topic prefix for the run.
	 */
	@BeforeClass
	public static void setUpBeforeClass() {
		serverURI = broker.getServerURI();
		topicPrefix = "SendReceiveTest-" + UUID.randomUUID() + "-";
	}

	/**
	 * A client can connect and disconnect twice.
	 */
	@Test
	public void testConnect() {
		String methodName = name.getMethodName();
		MqttClient mqttClient = null;
		try {
			mqttClient = TestClients.createMqttClient(serverURI, methodName);
			mqttClient.connect(mqttClient.getOptions());
			mqttClient.disconnect();
			mqttClient.connect(mqttClient.getOptions());
			mqttClient.disconnect();
		} catch (Exception exception) {
			Assert.fail("Failed:" + methodName + " exception=" + exception);
		} finally {
			TestClients.closeQuietly(mqttClient);
		}
	}

	/**
	 * After a reconnect with {@code cleanSession=false}, a QoS 1 publish arrives on a QoS 0
	 * subscription at QoS 0.
	 */
	@Test
	public void testRemoteConnect() {
		String methodName = name.getMethodName();
		MqttClient mqttClient = null;
		try {
			mqttClient = TestClients.createMqttClient(serverURI, methodName);
			mqttClient.connect(mqttClient.getOptions());
			mqttClient.disconnect();

			MqttV3Receiver mqttV3Receiver = new MqttV3Receiver();
			mqttClient.setCallback(mqttV3Receiver);
			MqttConnectOptions mqttConnectOptions = TestClients.options(serverURI, methodName);
			mqttConnectOptions.setCleanSession(false);
			mqttClient.connect(mqttConnectOptions);

			String[] topicNames = new String[]{topicPrefix + methodName + "/Topic"};
			int[] topicQos = {0};
			mqttClient.subscribeWithResponse(topicNames, topicQos, null);

			byte[] payload = ("Message payload " + className + "." + methodName).getBytes(StandardCharsets.UTF_8);
			MqttTopic mqttTopic = mqttClient.getTopic(topicNames[0]);
			mqttTopic.publish(payload, 1, false);
			boolean ok = mqttV3Receiver.validateReceipt(topicNames[0], 0, payload);
			if (!ok) {
				Assert.fail("Receive failed");
			}
			mqttClient.disconnect();
		} catch (Exception exception) {
			Assert.fail("Failed:" + methodName + " exception=" + exception);
		} finally {
			TestClients.closeQuietly(mqttClient);
		}
	}

	/**
	 * A 10,000-byte payload makes the round trip intact, after an unsubscribe and resubscribe.
	 */
	@Test
	public void testLargeMessage() {
		String methodName = name.getMethodName();
		MqttClient mqttClient = null;
		try {
			mqttClient = TestClients.createMqttClient(serverURI, methodName);
			MqttV3Receiver mqttV3Receiver = new MqttV3Receiver();
			mqttClient.setCallback(mqttV3Receiver);
			mqttClient.connect(mqttClient.getOptions());

			int largeSize = 10000;
			String[] topicNames = new String[]{topicPrefix + methodName + "/Topic"};
			int[] topicQos = {0};
			byte[] message = new byte[largeSize];
			Arrays.fill(message, (byte) 's');

			mqttClient.subscribeWithResponse(topicNames, topicQos, null);
			for (String topicName : topicNames) {
				mqttClient.unsubscribe(topicName);
			}
			mqttClient.subscribeWithResponse(topicNames, topicQos, null);

			MqttTopic mqttTopic = mqttClient.getTopic(topicNames[0]);
			mqttTopic.publish(message, 0, false);
			boolean ok = mqttV3Receiver.validateReceipt(topicNames[0], 0, message);
			if (!ok) {
				Assert.fail("Receive failed");
			}
		} catch (Exception exception) {
			Assert.fail("Failed to instantiate:" + methodName + " exception=" + exception);
		} finally {
			TestClients.closeQuietly(mqttClient);
		}
	}

	/**
	 * Each message arrives at the lower of its publish QoS and the subscription QoS, for every
	 * combination of the two.
	 */
	@Test
	public void testQoSPreserved() {
		String methodName = name.getMethodName();
		MqttClient mqttClient = null;
		try {
			mqttClient = TestClients.createMqttClient(serverURI, methodName);
			MqttV3Receiver mqttV3Receiver = new MqttV3Receiver();
			mqttClient.setCallback(mqttV3Receiver);
			mqttClient.connect(mqttClient.getOptions());

			String[] topicNames = new String[]{topicPrefix + methodName + "/Topic0",
					topicPrefix + methodName + "/Topic1", topicPrefix + methodName + "/Topic2"};
			int[] topicQos = {0, 1, 2};
			mqttClient.subscribeWithResponse(topicNames, topicQos, null);

			for (int i = 0; i < topicNames.length; i++) {
				byte[] message = ("Message payload " + className + "." + methodName + " " + topicNames[i])
						.getBytes(StandardCharsets.UTF_8);
				MqttTopic mqttTopic = mqttClient.getTopic(topicNames[i]);
				for (int iQos = 0; iQos < 3; iQos++) {
					mqttTopic.publish(message, iQos, false);
					boolean ok = mqttV3Receiver.validateReceipt(topicNames[i], Math.min(iQos, topicQos[i]), message);
					if (!ok) {
						Assert.fail("Receive failed sub Qos=" + topicQos[i] + " PublishQos=" + iQos);
					}
				}
			}
		} catch (Exception exception) {
			Assert.fail("Failed:" + methodName + " exception=" + exception);
		} finally {
			TestClients.closeQuietly(mqttClient);
		}
	}

	/**
	 * Every message from two publishers reaches each of ten subscribers.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testMultipleClients() throws Exception {
		MqttClient[] mqttPublisher = new MqttClient[2];
		MqttClient[] mqttSubscriber = new MqttClient[10];
		try {
			String[] topicNames = new String[]{topicPrefix + name.getMethodName() + "/Topic"};
			int[] topicQos = {0};

			MqttTopic[] mqttTopic = new MqttTopic[mqttPublisher.length];
			for (int i = 0; i < mqttPublisher.length; i++) {
				mqttPublisher[i] = TestClients.createMqttClient(serverURI, "MultiPub" + i);
				mqttPublisher[i].connect(mqttPublisher[i].getOptions());
				mqttTopic[i] = mqttPublisher[i].getTopic(topicNames[0]);
			}

			MqttV3Receiver[] mqttV3Receiver = new MqttV3Receiver[mqttSubscriber.length];
			for (int i = 0; i < mqttSubscriber.length; i++) {
				mqttSubscriber[i] = TestClients.createMqttClient(serverURI, "MultiSubscriber" + i);
				mqttV3Receiver[i] = new MqttV3Receiver();
				mqttSubscriber[i].setCallback(mqttV3Receiver[i]);
				mqttSubscriber[i].connect(mqttSubscriber[i].getOptions());
				mqttSubscriber[i].subscribeWithResponse(topicNames, topicQos, null);
			}

			for (int iMessage = 0; iMessage < 10; iMessage++) {
				byte[] payload = ("Message " + iMessage).getBytes(StandardCharsets.UTF_8);
				for (MqttTopic topic : mqttTopic) {
					topic.publish(payload, 0, false);
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
		} finally {
			for (MqttClient publisher : mqttPublisher) {
				TestClients.closeQuietly(publisher);
			}
			for (MqttClient subscriber : mqttSubscriber) {
				TestClients.closeQuietly(subscriber);
			}
		}
	}

	/**
	 * A {@code cleanSession=false} subscription receives messages, and reconnecting with
	 * {@code cleanSession=true} discards both the queued messages and the subscription.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testCleanStart() throws Exception {
		String methodName = name.getMethodName();
		MqttClient mqttClient = null;
		try {
			mqttClient = TestClients.createMqttClient(serverURI, methodName);
			MqttV3Receiver mqttV3Receiver = new MqttV3Receiver();
			mqttClient.setCallback(mqttV3Receiver);
			MqttConnectOptions mqttConnectOptions = TestClients.options(serverURI, methodName);
			// Clean start: true  - The broker cleans up all client state, including subscriptions, when the client is disconnected.
			// Clean start: false - The broker remembers all client state, including subscriptions, when the client is disconnected.
			//                      Matching publications will get queued in the broker whilst the client is disconnected.
			// For Mqtt V3 cleanSession=false, implies new subscriptions are durable.
			mqttConnectOptions.setCleanSession(false);
			mqttClient.connect(mqttConnectOptions);

			String[] topicNames = new String[]{topicPrefix + methodName + "/Topic"};
			int[] topicQos = {0};
			mqttClient.subscribeWithResponse(topicNames, topicQos, null);

			byte[] payload = ("Message payload " + className + "." + methodName + " First").getBytes(StandardCharsets.UTF_8);
			MqttTopic mqttTopic = mqttClient.getTopic(topicNames[0]);
			mqttTopic.publish(payload, 1, false);
			boolean ok = mqttV3Receiver.validateReceipt(topicNames[0], 0, payload);
			if (!ok) {
				Assert.fail("Receive failed");
			}

			// Disconnect and reconnect to make sure the subscription and all queued messages are cleared.
			mqttClient.disconnect();
			mqttClient.close();

			// Send a message from another client, to our durable subscription.
			mqttClient = TestClients.createMqttClient(serverURI, methodName + "Other");
			mqttV3Receiver = new MqttV3Receiver();
			mqttClient.setCallback(mqttV3Receiver);
			mqttConnectOptions = TestClients.options(serverURI, methodName + "Other");
			mqttConnectOptions.setCleanSession(true);
			mqttClient.connect(mqttConnectOptions);
			// Receive the publication so that we can be sure the first client has also received it.
			// Otherwise the first client may reconnect with its clean session before the message has arrived.
			mqttClient.subscribeWithResponse(topicNames, topicQos, null);
			payload = ("Message payload " + className + "." + methodName + " Other client").getBytes(StandardCharsets.UTF_8);
			mqttTopic = mqttClient.getTopic(topicNames[0]);
			mqttTopic.publish(payload, 1, false);
			ok = mqttV3Receiver.validateReceipt(topicNames[0], 0, payload);
			if (!ok) {
				Assert.fail("Receive failed");
			}
			mqttClient.disconnect();
			mqttClient.close();

			// Reconnect and check we have no messages.
			mqttClient = TestClients.createMqttClient(serverURI, methodName);
			mqttV3Receiver = new MqttV3Receiver();
			mqttClient.setCallback(mqttV3Receiver);
			mqttConnectOptions = TestClients.options(serverURI, methodName);
			mqttConnectOptions.setCleanSession(true);
			mqttClient.connect(mqttConnectOptions);
			MqttV3Receiver.ReceivedMessage receivedMessage = mqttV3Receiver.receiveNext(100);
			if (receivedMessage != null) {
				Assert.fail("Receive messaqe:" + new String(receivedMessage.message.getPayload(), StandardCharsets.UTF_8));
			}

			// Also check that subscription is cancelled.
			payload = ("Message payload " + className + "." + methodName + " Cancelled Subscription").getBytes(StandardCharsets.UTF_8);
			mqttTopic = mqttClient.getTopic(topicNames[0]);
			mqttTopic.publish(payload, 1, false);
			receivedMessage = mqttV3Receiver.receiveNext(100);
			if (receivedMessage != null) {
				Assert.fail("Receive messaqe:" + new String(receivedMessage.message.getPayload(), StandardCharsets.UTF_8));
			}
		} finally {
			TestClients.closeQuietly(mqttClient);
		}
	}
}
