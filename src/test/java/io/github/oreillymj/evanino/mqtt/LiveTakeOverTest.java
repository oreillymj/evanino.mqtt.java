/*******************************************************************************
 * Ported from Eclipse Paho's Java client tests,
 * org/eclipse/paho/client/mqttv3/test/LiveTakeOverTest.java,
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
import org.junit.Test;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Port of Paho's {@code LiveTakeOverTest}: a second client connecting with the same client ID
 * takes over the first client's session, including its subscription, while the first client
 * is publishing. Requires a broker; see {@link MosquittoBroker}.
 */
public class LiveTakeOverTest {

	/** The broker used by every test in this class. */
	@ClassRule
	public static final MosquittoBroker broker = new MosquittoBroker();

	/**
	 * The first client's lifecycle, used to coordinate it with the test thread.
	 */
	enum FirstClientState {
		/** Not yet connected. */
		INITIAL,
		/** Connected and subscribed. */
		READY,
		/** Publishing. */
		RUNNING,
		/** Finished. */
		FINISHED,
		/** Failed. */
		ERROR
	}

	private static final String ClientId = "TakeOverClient";
	private static URI serverURI;
	private static String topicPrefix;
	private static String FirstSubTopicString;

	/**
	 * Picks up the broker URI and unique topics for the run.
	 */
	@BeforeClass
	public static void setUpBeforeClass() {
		serverURI = broker.getServerURI();
		topicPrefix = "FirstClientState-" + UUID.randomUUID() + "-";
		FirstSubTopicString = topicPrefix + "FirstClient/Topic";
	}

	/**
	 * While a first client publishes to its own subscription, a second client with the same
	 * ID and {@code cleanSession=false} connects, takes over the session, and receives a
	 * message published on the first client's subscription.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testLiveTakeOver() throws Exception {
		MqttClient mqttClient = null;
		try {
			FirstClient firstClient = new FirstClient();
			Thread firstClientThread = new Thread(firstClient, "first-client");
			firstClientThread.start();
			firstClient.waitForState(FirstClientState.READY);

			//Tell the first client to go and let it publish for a couple of seconds
			firstClient.setState(FirstClientState.RUNNING);
			Thread.sleep(2000);

			//Now lets take over the connection
			// Create a second MQTT client connection with the same clientid. The
			// server should spot this and kick the first client connection off.
			MqttConnectOptions mqttConnectOptions = TestClients.options(serverURI, ClientId);
			mqttConnectOptions.setCleanSession(false);
			mqttConnectOptions.setWill(topicPrefix + "WillTopic", "payload".getBytes(StandardCharsets.UTF_8), 2, true);
			mqttClient = new MqttClient(mqttConnectOptions);
			MqttV3Receiver mqttV3Receiver = new MqttV3Receiver();
			mqttClient.setCallback(mqttV3Receiver);
			mqttClient.connect(mqttConnectOptions);

			//We should have taken over the first Client's subscription...we may have some
			//of his publishes arrive.
			//Ignore his publishes that arrive...
			MqttV3Receiver.ReceivedMessage oldMsg;
			do {
				oldMsg = mqttV3Receiver.receiveNext(1000);
			} while (oldMsg != null);

			//Now check we have grabbed his subscription by publishing..
			byte[] payload = ("Message payload from second client " + getClass().getName() + ".testLiveTakeOver")
					.getBytes(StandardCharsets.UTF_8);
			MqttTopic mqttTopic = mqttClient.getTopic(FirstSubTopicString);
			mqttTopic.publish(payload, 1, false);
			boolean ok = mqttV3Receiver.validateReceipt(FirstSubTopicString, 1, payload);
			if (!ok) {
				throw new Exception("Receive failed");
			}

			firstClientThread.join(10_000);
			Assert.assertNotEquals("first client failed", FirstClientState.ERROR, firstClient.getState());
		} finally {
			TestClients.closeQuietly(mqttClient);
		}
	}

	/**
	 * The first client: connects, subscribes, then publishes until it is disconnected by the
	 * takeover.
	 */
	class FirstClient implements Runnable {
		private FirstClientState state = FirstClientState.INITIAL;
		private final Object stateLock = new Object();
		private MqttClient mqttClient = null;

		/**
		 * Waits for the first client to reach a state, failing if it errors.
		 *
		 * @param desiredState the state to wait for
		 * @throws InterruptedException if interrupted while waiting
		 */
		void waitForState(FirstClientState desiredState) throws InterruptedException {
			synchronized (stateLock) {
				while ((state != desiredState) && (state != FirstClientState.ERROR)) {
					stateLock.wait();
				}
				if (state == FirstClientState.ERROR) {
					Assert.fail("Firstclient entered an ERROR state");
				}
			}
		}

		/**
		 * Returns the first client's current state.
		 *
		 * @return the state
		 */
		FirstClientState getState() {
			synchronized (stateLock) {
				return state;
			}
		}

		/**
		 * Moves the first client to a new state and wakes any waiter.
		 *
		 * @param newState the new state
		 */
		void setState(FirstClientState newState) {
			synchronized (stateLock) {
				state = newState;
				stateLock.notifyAll();
			}
		}

		/**
		 * Connects with {@code cleanSession=false} and a will, and subscribes at QoS 2.
		 */
		void connectAndSub() {
			try {
				MqttConnectOptions mqttConnectOptions = TestClients.options(serverURI, ClientId);
				mqttConnectOptions.setCleanSession(false);
				mqttConnectOptions.setWill(topicPrefix + "WillTopic", "payload".getBytes(StandardCharsets.UTF_8), 2, true);
				mqttClient = new MqttClient(mqttConnectOptions);
				mqttClient.setCallback(new MqttV3Receiver());
				mqttClient.connect(mqttConnectOptions);
				mqttClient.subscribe(FirstSubTopicString, 2);
			} catch (Exception exception) {
				exception.printStackTrace();
				setState(FirstClientState.ERROR);
			}
		}

		/**
		 * Publishes at QoS 1 in a loop until the connection is taken over.
		 */
		void repeatedlyPub() {
			int i = 0;
			while (mqttClient.isConnected()) {
				try {
					if (i > 999999) {
						i = 0;
					}
					byte[] payload = ("Message payload " + getClass().getName() + ".publish" + (i++))
							.getBytes(StandardCharsets.UTF_8);
					MqttTopic mqttTopic = mqttClient.getTopic(FirstSubTopicString);
					mqttTopic.publish(payload, 1, false);
				} catch (Exception exception) {
					// Don't fail - we are going to get an exception as we disconnected during takeOver
					// Its likely the publish rate is too high i.e. inflight window is full
				}
			}
		}

		/**
		 * Runs the first client.
		 */
		@Override
		public void run() {
			connectAndSub();
			if (getState() == FirstClientState.ERROR) {
				return;
			}
			try {
				setState(FirstClientState.READY);
				waitForState(FirstClientState.RUNNING);
				repeatedlyPub();
				mqttClient.close();
				setState(FirstClientState.FINISHED);
			} catch (InterruptedException | MqttException exception) {
				setState(FirstClientState.ERROR);
			}
		}
	}
}
