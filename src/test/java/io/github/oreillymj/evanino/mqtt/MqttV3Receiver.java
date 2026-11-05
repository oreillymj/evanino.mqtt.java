/*******************************************************************************
 * Ported from Eclipse Paho's Java client tests,
 * org/eclipse/paho/client/mqttv3/test/utilities/MqttV3Receiver.java,
 * https://github.com/eclipse-paho/paho.mqtt.java. This project uses the Paho code under
 * the Eclipse Distribution License v1.0 (see licenses/EDL-1.0.txt).
 *
 * The original notice:
 *
 *   Copyright (c) 2009, 2018 IBM Corp.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Port of Paho's {@code MqttV3Receiver} test utility: a callback that queues every incoming
 * message so tests can check what arrived, in order.
 */
public class MqttV3Receiver extends MqttCallbackAdapter {

	/** How long {@link #validateReceipt(String, int, byte[])} waits for a message. Paho waits 60 s. */
	static final long VALIDATE_WAIT_MILLIS = 10_000;

	/**
	 * A message received by the callback, with the topic it arrived on.
	 */
	public static final class ReceivedMessage {
		/** The topic the message was published to. */
		public final String topic;
		/** The received message. */
		public final MqttMessage message;

		/**
		 * Creates a received-message record.
		 *
		 * @param topic   the topic the message was published to
		 * @param message the received message
		 */
		ReceivedMessage(String topic, MqttMessage message) {
			this.topic = topic;
			this.message = message;
		}
	}

	private final List<ReceivedMessage> receivedMessages = new ArrayList<>();
	private boolean connectionLost = false;

	/**
	 * Waits for the next message, if none is queued yet, and removes it from the queue.
	 *
	 * @param waitMilliseconds the maximum time to wait
	 * @return the next message, or {@code null} if none arrived in time
	 * @throws InterruptedException if interrupted while waiting
	 */
	public synchronized ReceivedMessage receiveNext(long waitMilliseconds) throws InterruptedException {
		long deadline = System.currentTimeMillis() + waitMilliseconds;
		while (receivedMessages.isEmpty()) {
			long remaining = deadline - System.currentTimeMillis();
			if (remaining <= 0) {
				return null;
			}
			wait(remaining);
		}
		return receivedMessages.remove(0);
	}

	/**
	 * Waits for the next message and checks its topic, payload and QoS, as Paho's
	 * {@code validateReceipt} does.
	 *
	 * @param sendTopic   the topic the message was published to
	 * @param expectedQos the QoS the message should arrive at
	 * @param sentBytes   the payload that was published
	 * @return {@code true} if a matching message arrived in time
	 * @throws InterruptedException if interrupted while waiting
	 */
	public boolean validateReceipt(String sendTopic, int expectedQos, byte[] sentBytes) throws InterruptedException {
		ReceivedMessage received = receiveNext(VALIDATE_WAIT_MILLIS);
		if (received == null) {
			System.err.println("MqttV3Receiver: no message received in " + VALIDATE_WAIT_MILLIS + " ms");
			return false;
		}
		if (!sendTopic.equals(received.topic)) {
			System.err.println("MqttV3Receiver: topic sent=" + sendTopic + " received=" + received.topic);
			return false;
		}
		if (!Arrays.equals(sentBytes, received.message.getPayload())) {
			System.err.println("MqttV3Receiver: payload sent=" + Arrays.toString(sentBytes)
					+ " received=" + Arrays.toString(received.message.getPayload()));
			return false;
		}
		if (expectedQos != received.message.getQos()) {
			System.err.println("MqttV3Receiver: expectedQos=" + expectedQos + " received=" + received.message.getQos());
			return false;
		}
		return true;
	}

	/**
	 * Waits until the connection-lost callback has fired.
	 *
	 * @param waitMilliseconds the maximum time to wait
	 * @return {@code true} if the connection was reported lost in time
	 * @throws InterruptedException if interrupted while waiting
	 */
	public synchronized boolean waitForConnectionLost(long waitMilliseconds) throws InterruptedException {
		long deadline = System.currentTimeMillis() + waitMilliseconds;
		while (!connectionLost) {
			long remaining = deadline - System.currentTimeMillis();
			if (remaining <= 0) {
				return false;
			}
			wait(remaining);
		}
		return true;
	}

	/**
	 * Returns the number of messages currently queued.
	 *
	 * @return the queued message count
	 */
	public synchronized int receivedMessageCount() {
		return receivedMessages.size();
	}

	/**
	 * Records that the connection was lost.
	 *
	 * @param cause the reason the connection was lost
	 */
	@Override
	public synchronized void connectionLost(Throwable cause) {
		connectionLost = true;
		notifyAll();
	}

	/**
	 * Queues an incoming message.
	 *
	 * @param topic   the topic the message was published to
	 * @param message the received message
	 */
	@Override
	public synchronized void messageArrived(String topic, MqttMessage message) {
		receivedMessages.add(new ReceivedMessage(topic, message));
		notifyAll();
	}
}
