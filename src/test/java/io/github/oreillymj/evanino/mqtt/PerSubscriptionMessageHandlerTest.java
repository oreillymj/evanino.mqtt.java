/*******************************************************************************
 * Ported from Eclipse Paho's Java client tests,
 * org/eclipse/paho/client/mqttv3/test/PerSubscriptionMessageHandlerTest.java,
 * https://github.com/eclipse-paho/paho.mqtt.java. This project uses the Paho code under
 * the Eclipse Distribution License v1.0 (see licenses/EDL-1.0.txt).
 *
 * The original notice:
 *
 *   Copyright (c) 2015 IBM Corp.
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
 *      Ian Craggs - initial API and implementation and/or initial documentation
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

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.UUID;

/**
 * Port of Paho's {@code PerSubscriptionMessageHandlerTest}: messages are delivered to the
 * {@link IMqttMessageListener} given at subscribe time. Requires a broker; see
 * {@link MosquittoBroker}.
 * <p>
 * Paho's blocking {@code subscribe(topic, qos, listener)} is
 * {@link MqttClient#subscribeWithResponse(String[], int[], IMqttMessageListener[])} here, and
 * the asynchronous one is
 * {@link MqttAsyncClient#subscribe(String, int, Object, IMqttActionListener, IMqttMessageListener)}.
 */
public class PerSubscriptionMessageHandlerTest {

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
		topicPrefix = "PerSubscriptionMessageHandlerTest-" + UUID.randomUUID() + "-";
	}

	/**
	 * Collects the messages delivered to a subscription.
	 */
	static class Listener implements IMqttMessageListener {
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
		 * Returns the next message if one has already arrived, without waiting.
		 *
		 * @return the next message, or {@code null} if none is queued
		 */
		public MqttMessage getNextMessageNow() {
			synchronized (messages) {
				return messages.isEmpty() ? null : messages.remove(0);
			}
		}

		/**
		 * Records a message unless it is a duplicate.
		 *
		 * @param topic   the topic the message was published to
		 * @param message the received message
		 */
		@Override
		public void messageArrived(String topic, MqttMessage message) {
			if (!message.isDuplicate()) {
				synchronized (messages) {
					messages.add(message);
					messages.notifyAll();
				}
			}
		}
	}

	/**
	 * A message published on a topic reaches the listener given to the blocking subscribe.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testSyncSubs1() throws Exception {
		Listener mylistener = new Listener();
		MqttClient mqttClient = TestClients.createMqttClient(serverURI, name.getMethodName());
		try {
			String mytopic = topicPrefix + "PerSubscriptionTest/topic";

			mqttClient.connect(mqttClient.getOptions());
			mqttClient.subscribeWithResponse(new String[]{mytopic}, new int[]{2}, new IMqttMessageListener[]{mylistener});

			MqttMessage message = new MqttMessage();
			message.setPayload("testSyncSubs1".getBytes(StandardCharsets.UTF_8));
			mqttClient.publish(mytopic, message);

			MqttMessage msg = mylistener.getNextMessage();
			Assert.assertNotNull(msg);
			Assert.assertEquals("testSyncSubs1", msg.toString());

			mqttClient.disconnect();
		} finally {
			TestClients.closeQuietly(mqttClient);
		}
	}

	/**
	 * A message published on a topic reaches the listener given to the asynchronous subscribe.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testAsyncSubs1() throws Exception {
		Listener mylistener = new Listener();
		MqttAsyncClient mqttClient = TestClients.createMqttAsyncClient(serverURI, name.getMethodName());
		try {
			String mytopic = topicPrefix + "PerSubscriptionTest/topic";

			IMqttToken token = mqttClient.connect(null, null, null);
			token.waitForCompletion();
			token = mqttClient.subscribe(mytopic, 2, null, null, mylistener);
			token.waitForCompletion();

			MqttMessage message = new MqttMessage();
			message.setPayload("testAsyncSubs1".getBytes(StandardCharsets.UTF_8));
			token = mqttClient.publish(mytopic, message);
			token.waitForCompletion();

			MqttMessage msg = mylistener.getNextMessage();
			Assert.assertNotNull(msg);
			Assert.assertEquals("testAsyncSubs1", msg.toString());

			token = mqttClient.disconnect();
			token.waitForCompletion();
		} finally {
			TestClients.shutdownQuietly(mqttClient);
		}
	}

	/**
	 * In Paho, a subscription handler survives a {@code cleanSession=false} reconnect.
	 */
	@Ignore("Commented out in Paho's own test suite")
	@Test
	public void testSyncCleanSessionFalse() {
	}

	/**
	 * In Paho, a subscription handler survives a {@code cleanSession=false} reconnect.
	 */
	@Ignore("Commented out in Paho's own test suite")
	@Test
	public void testAsyncCleanSessionFalse() {
	}

	/**
	 * In Paho, unsubscribing removes the subscription's message handler.
	 */
	@Ignore("Commented out in Paho's own test suite")
	@Test
	public void testSyncUnsubscribeRemove() {
	}
}
