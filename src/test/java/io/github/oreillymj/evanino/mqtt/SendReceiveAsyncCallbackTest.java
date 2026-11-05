/*******************************************************************************
 * Ported from Eclipse Paho's Java client tests,
 * org/eclipse/paho/client/mqttv3/test/SendReceiveAsyncCallbackTest.java,
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
import java.util.ArrayList;
import java.util.UUID;

/**
 * Port of Paho's {@code SendReceiveAsyncCallbackTest}: connect, subscribe, a chain of QoS 2
 * publishes and disconnect, each step driven from the previous step's
 * {@link IMqttActionListener}. Requires a broker; see {@link MosquittoBroker}.
 * <p>
 * Differences from Paho:
 * <ul>
 * <li>Paho runs the test over both TCP and WebSockets; only TCP is covered here.</li>
 * <li>Paho reaches the client through {@code token.getClient()}, which this client does not
 * implement (it returns {@code null}), so the listeners use the client directly.</li>
 * <li>Paho finishes by removing two "might cancel" publishes with {@code removeMessage};
 * this client has no {@code removeMessage}, so that step is left out.</li>
 * </ul>
 */
public class SendReceiveAsyncCallbackTest {

	/** The broker used by every test in this class. */
	@ClassRule
	public static final MosquittoBroker broker = new MosquittoBroker();

	/** Gives each test its own client ID. */
	@Rule
	public final TestName name = new TestName();

	private final int messageCount = 5;
	private static URI serverURI;
	private static String topicFilter;

	private volatile boolean testFinished = false;
	private volatile String callbackFailure;
	private MqttAsyncClient mqttClient;
	private final Listener myListener = new Listener();
	private final OnPublish myOnPublish = new OnPublish(1);

	/**
	 * Picks up the broker URI and a unique topic for the run.
	 */
	@BeforeClass
	public static void setUpBeforeClass() {
		serverURI = broker.getServerURI();
		String topicPrefix = "SendReceiveAsyncCallbackTest-" + UUID.randomUUID() + "-";
		topicFilter = topicPrefix + "SendReceiveAsyncCallback/topic";
	}

	/**
	 * Connect, subscribe, publish five QoS 2 messages and disconnect, each step started from
	 * the previous step's success callback; all five messages must arrive.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 60_000)
	public void test1() throws Exception {
		String methodName = name.getMethodName();
		try {
			testFinished = false;
			mqttClient = TestClients.createMqttAsyncClient(serverURI, methodName);
			mqttClient.connect(null, null, new OnConnect(1));

			int count = 0;
			while (!testFinished && ++count < 80) {
				Thread.sleep(500);
			}
			Assert.assertNull(callbackFailure, callbackFailure);
			Assert.assertTrue("Callbacks not called", testFinished);

			count = 0;
			while (myListener.size() < messageCount && ++count < 10) {
				Thread.sleep(500);
			}
			Assert.assertEquals("All messages received", messageCount, myListener.size());

			testFinished = false;
			mqttClient.disconnect(30000, null, new OnDisconnect(1));
			count = 0;
			while (!testFinished && ++count < 80) {
				Thread.sleep(500);
			}
			Assert.assertNull(callbackFailure, callbackFailure);
			Assert.assertTrue("Callbacks not called", testFinished);
		} catch (Exception exception) {
			Assert.fail("Failed:" + methodName + " exception=" + exception);
		} finally {
			TestClients.shutdownQuietly(mqttClient);
		}
	}

	/**
	 * Records a failure raised inside a callback, which runs off the test thread where an
	 * assertion would be lost, and ends the wait.
	 *
	 * @param message what went wrong
	 */
	private void failInCallback(String message) {
		callbackFailure = message;
		testFinished = true;
	}

	/**
	 * Ends the test when the disconnect completes.
	 */
	class OnDisconnect implements IMqttActionListener {
		private final int testno;

		/**
		 * Creates the listener.
		 *
		 * @param testno the test this listener belongs to
		 */
		OnDisconnect(int testno) {
			this.testno = testno;
		}

		/** {@inheritDoc} */
		@Override
		public void onSuccess(IMqttToken token) {
			if (testno == 1) {
				testFinished = true;
			} else {
				failInCallback("Wrong test number: " + testno);
			}
		}

		/** {@inheritDoc} */
		@Override
		public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
			failInCallback("disconnect failed: " + exception);
		}
	}

	/**
	 * Collects the messages delivered to the subscription.
	 */
	static class Listener implements IMqttMessageListener {
		final ArrayList<MqttMessage> messages = new ArrayList<>();

		/**
		 * Returns the number of messages received.
		 *
		 * @return the message count
		 */
		int size() {
			synchronized (messages) {
				return messages.size();
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
			String msgstr = new String(message.getPayload(), StandardCharsets.UTF_8);
			if (!message.isDuplicate()) {
				synchronized (messages) {
					if (!msgstr.equals("might cancel")) {
						messages.add(message);
						messages.notifyAll();
					}
				}
			}
		}
	}

	/**
	 * Publishes the next message each time a publish completes, until all have been sent.
	 */
	class OnPublish implements IMqttActionListener {
		private final int testno;
		private int count;

		/**
		 * Creates the listener.
		 *
		 * @param testno the test this listener belongs to
		 */
		OnPublish(int testno) {
			this.testno = testno;
			count = 0;
		}

		/** {@inheritDoc} */
		@Override
		public void onSuccess(IMqttToken token) {
			if (testno == 1) {
				try {
					if (++count < messageCount) {
						publishMyData(mqttClient, this);
					} else {
						testFinished = true;
					}
				} catch (Exception exception) {
					failInCallback("publish failed: " + exception);
				}
			} else {
				failInCallback("Wrong test number: " + testno);
			}
		}

		/** {@inheritDoc} */
		@Override
		public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
			failInCallback("publish failed: " + exception);
		}
	}

	/**
	 * Publishes the first message once the subscription is in place.
	 */
	class OnSubscribe implements IMqttActionListener {
		private final int testno;

		/**
		 * Creates the listener.
		 *
		 * @param testno the test this listener belongs to
		 */
		OnSubscribe(int testno) {
			this.testno = testno;
		}

		/** {@inheritDoc} */
		@Override
		public void onSuccess(IMqttToken token) {
			if (testno == 1) {
				try {
					publishMyData(mqttClient, myOnPublish);
				} catch (Exception exception) {
					failInCallback("publish failed: " + exception);
				}
			} else {
				failInCallback("Wrong test number: " + testno);
			}
		}

		/** {@inheritDoc} */
		@Override
		public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
			failInCallback("subscribe failed: " + exception);
		}
	}

	/**
	 * Subscribes, with the message listener, once the connection is established.
	 */
	class OnConnect implements IMqttActionListener {
		private final int testno;

		/**
		 * Creates the listener.
		 *
		 * @param testno the test this listener belongs to
		 */
		OnConnect(int testno) {
			this.testno = testno;
		}

		/** {@inheritDoc} */
		@Override
		public void onSuccess(IMqttToken token) {
			try {
				if (testno == 1) {
					mqttClient.subscribe(topicFilter, 2, null, new OnSubscribe(1), myListener);
				} else {
					failInCallback("Wrong test number: " + testno);
				}
			} catch (Exception exception) {
				failInCallback("subscribe failed: " + exception);
			}
		}

		/** {@inheritDoc} */
		@Override
		public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
			failInCallback("onConnect failed: " + exception);
		}
	}

	/**
	 * Publishes "my data" at QoS 2; equivalent to Paho's
	 * {@code publish(topicFilter, "my data".getBytes(), 2, false, null, listener)}.
	 *
	 * @param client   the client to publish with
	 * @param listener the listener to notify when the publish completes
	 * @throws MqttException if the publish cannot be started
	 */
	private static void publishMyData(MqttAsyncClient client, IMqttActionListener listener) throws MqttException {
		MqttMessage message = new MqttMessage("my data".getBytes(StandardCharsets.UTF_8));
		message.setQos(2);
		client.publish(topicFilter, message, null, listener);
	}
}
