/*******************************************************************************
 * From Eclipse Paho's Java client (org.eclipse.paho.client.mqttv3.MqttMessage), with changes.
 * The original notice follows. This project uses the Paho code under the Eclipse
 * Distribution License v1.0 (see licenses/EDL-1.0.txt).
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
 *   Contributors:
 *      Dave Locke - initial API and implementation and/or initial documentation
 *      Ian Craggs - ack control (bug 472172)
 *
 * Changes copyright (c) 2026 Michael OReilly, under the Apache License 2.0 (see LICENSE).
 *******************************************************************************/

package io.github.oreillymj.evanino.mqtt;

import java.nio.charset.StandardCharsets;

/**
 * An MQTT message holds the application payload and options
 * specifying how the message is to be delivered
 * The message includes a "payload" (the body of the message)
 * represented as a byte[].
 */
public class MqttMessage {

	private boolean mutable = true;
	private byte[] payload;
	private int qos = 1;
	private boolean retained = false;
	private boolean dup = false;
	private int messageId;
	private String URI = null;

	/**
	 * Returns a QoS level as given if it is 0, 1 or 2, and 0 otherwise: wherever this client
	 * takes a QoS as an {@code int}, a value MQTT does not have means QoS 0.
	 *
	 * @param qos the QoS level
	 * @return {@code qos} if it is 0, 1 or 2, otherwise 0
	 */
	static int validQos(int qos) {
		return (qos < 0 || qos > 2) ? 0 : qos;
	}

	/**
	 * Utility method to validate the supplied QoS value, for code that wants to reject an
	 * invalid one; the client itself treats an invalid QoS as 0 (see {@link #setQos(int)}).
	 *
	 * @param qos The QoS Level
	 * @throws IllegalArgumentException if value of QoS is not 0, 1 or 2.
	 */
	public static void validateQos(int qos) {
		if ((qos < 0) || (qos > 2)) {
			throw new IllegalArgumentException();
		}
	}

	/**
	 * Constructs a message with an empty payload, and all other values
	 * set to defaults.
	 * <p>
	 * The defaults are:
	 * <ul>
	 *    <li>Message QoS set to 1</li>
	 *    <li>Message will not be "retained" by the server</li>
	 * </ul>
	 */
	public MqttMessage() {
		setPayload(new byte[]{});
	}

	/**
	 * Constructs a message with the specified byte array as a payload,
	 * and all other values set to defaults.
	 *
	 * @param payload The Bytearray of the payload
	 */
	public MqttMessage(byte[] payload) {
		setPayload(payload);
	}

	/**
	 * Constructs a message with the specified string, encoded as UTF-8, as a payload,
	 * and all other values set to defaults.
	 *
	 * @param payload the payload text
	 */
	public MqttMessage(String payload) {
		setPayload(payload);
	}

	/**
	 * Constructs a message with the specified string, encoded as UTF-8, as a payload, and
	 * the given delivery options.
	 *
	 * @param payload     the payload text
	 * @param qos         the quality of service, 0, 1 or 2; any other value means 0
	 * @param retained    whether the message should be, or was, retained by the server
	 * @param isDuplicate whether the message might be a duplicate of one already received
	 */
	public MqttMessage(String payload, int qos, boolean retained, boolean isDuplicate) {
		setPayload(payload);
		this.qos = validQos(qos);
		this.retained = retained;
		this.dup = isDuplicate;
	}

	/**
	 * Returns the payload as a byte array.
	 *
	 * @return the payload as a byte array.
	 */
	public byte[] getPayload() {
		return payload;
	}

	/**
	 * Returns the payload decoded as UTF-8.
	 *
	 * @return the payload as a string.
	 */
	public String getPayloadString() {
		return new String(this.payload, StandardCharsets.UTF_8);
	}

	/**
	 * Clears the payload, resetting it to be empty.
	 *
	 * @throws IllegalStateException if this message cannot be edited
	 */
	public void clearPayload() {
		checkMutable();
		this.payload = new byte[]{};
	}

	/**
	 * Sets the payload of this message to be the specified byte array.
	 *
	 * @param payload the payload for this message.
	 * @throws IllegalStateException if this message cannot be edited
	 * @throws NullPointerException  if no payload is provided
	 */
	public void setPayload(byte[] payload) {
		checkMutable();
		if (payload == null) {
			throw new NullPointerException();
		}
		this.payload = payload.clone();
	}

	/**
	 * Sets the payload of this message to the specified string, encoded as UTF-8.
	 * Unlike {@link #setPayload(byte[])}, this does not check that the message is mutable.
	 *
	 * @param payload the payload text
	 * @throws NullPointerException if no payload is provided
	 */
	public void setPayload(String payload) {
		if (payload == null) {
			throw new NullPointerException();
		}
		this.payload = payload.getBytes(StandardCharsets.UTF_8).clone();
	}

	/**
	 * Returns whether or not this message should be/was retained by the server.
	 * For messages received from the server, this method returns whether or not
	 * the message was from a current publisher, or was "retained" by the server as
	 * the last message published on the topic.
	 *
	 * @return <code>true</code> if the message should be, or was, retained by
	 * the server.
	 * @see #setRetained(boolean)
	 */
	public boolean isRetained() {
		return this.retained;
	}

	/**
	 * Returns whether or not this message should be/was retained by the server.
	 * For messages received from the server, this method returns whether or not
	 * the message was from a current publisher, or was "retained" by the server as
	 * the last message published on the topic.
	 *
	 * @return <code>true</code> if the message should be, or was, retained by
	 * the server.
	 * @see #setRetained(boolean)
	 */
	public boolean getRetained() {
		return this.retained;
	}

	/**
	 * Whether or not the publish message should be retained by the messaging engine.
	 * Sending a message with retained set to <code>true</code> and with an empty
	 * byte array as the payload e.g. <code>new byte[0]</code> will clear the
	 * retained message from the server.  The default value is <code>false</code>
	 *
	 * @param retained whether or not the messaging engine should retain the message.
	 * @throws IllegalStateException if this message cannot be edited
	 */
	public void setRetained(boolean retained) {
		checkMutable();
		this.retained = retained;
	}

	/**
	 * Sets whether the message should be retained by the server. Equivalent to
	 * {@link #setRetained(boolean)}.
	 *
	 * @param retained whether or not the messaging engine should retain the message.
	 * @throws IllegalStateException if this message cannot be edited
	 */
	public void isRetained(boolean retained) {
		checkMutable();
		this.retained = retained;
	}

	/**
	 * Returns the quality of service for this message.
	 *
	 * @return the quality of service to use, either 0, 1, or 2.
	 * @see #setQos(int)
	 */
	public int getQos() {
		return qos;
	}

	/**
	 * Sets the quality of service for this message.
	 * <ul>
	 * <li>Quality of Service 0 - indicates that a message should
	 * be delivered at most once (zero or one times).  The message will not be acknowledged
	 * across the network.  This QoS is the fastest, but should only be used for messages
	 * which are not valuable - note that if the server cannot process the message (for
	 * example, there is an authorization problem), the message is silently dropped, and
	 * {@link MqttCallback#deliveryComplete(IMqttDeliveryToken)} is still called once the
	 * message has been written to the network.
	 * Also known as "fire and forget".</li>
	 *
	 * <li>Quality of Service 1 - indicates that a message should
	 * be delivered at least once (one or more times).
	 * The message will be acknowledged across the network.
	 * This is the default QoS.</li>
	 *
	 * <li>Quality of Service 2 - indicates that a message should
	 * be delivered once.  The message will be subject to a two-phase acknowledgement
	 * across the network.</li>
	 *
	 * </ul>
	 * QoS 1 and 2 messages that are in flight are retransmitted in the event of a network
	 * or server problem, as the client holds their state in memory. If the client is shut
	 * down or fails, that state is lost and delivery of in-flight QoS 1 and 2 messages
	 * cannot be maintained.
	 *
	 * @param qos the "quality of service" to use: 0, 1 or 2. Any other value means 0 (Paho
	 *            throws an {@code IllegalArgumentException}).
	 * @throws IllegalStateException if this message cannot be edited
	 */
	public void setQos(int qos) {
		checkMutable();
		this.qos = validQos(qos);
	}

	/**
	 * Returns a string representation of this message's payload.
	 * Makes an attempt to return the payload as a string, decoded with the platform's
	 * default charset. As the MQTT client has no control over the content of the payload
	 * it may fail.
	 *
	 * @return a string representation of this message.
	 */
	public String toString() {
		return new String(payload);
	}

	/**
	 * Sets the mutability of this object (whether or not its values can be
	 * changed.
	 *
	 * @param mutable <code>true</code> if the values can be changed,
	 *                <code>false</code> to prevent them from being changed.
	 */
	protected void setMutable(boolean mutable) {
		this.mutable = mutable;
	}

	/**
	 * Checks that this message can be edited.
	 *
	 * @throws IllegalStateException if this message has been made immutable
	 */
	protected void checkMutable() throws IllegalStateException {
		if (!mutable) {
			throw new IllegalStateException();
		}
	}

	/**
	 * Sets whether this message might be a duplicate of one already received.
	 *
	 * @param dup <code>true</code> if the message might be a duplicate.
	 */
	protected void setDuplicate(boolean dup) {
		this.dup = dup;
	}

	/**
	 * Sets whether this message might be a duplicate of one already received. Equivalent to
	 * {@link #setDuplicate(boolean)}, but public.
	 *
	 * @param duplicate <code>true</code> if the message might be a duplicate.
	 */
	public void isDuplicate(boolean duplicate) {
		this.dup = duplicate;
	}

	/**
	 * Returns whether or not this message might be a duplicate of one which has
	 * already been received.  This will only be set on messages received from
	 * the server.
	 *
	 * @return <code>true</code> if the message might be a duplicate.
	 */
	public boolean isDuplicate() {
		return this.dup;
	}

	/**
	 * Returns whether or not this message might be a duplicate of one which has
	 * already been received.  This will only be set on messages received from
	 * the server.
	 *
	 * @return <code>true</code> if the message might be a duplicate.
	 */
	public boolean getDuplicate() {
		return this.dup;
	}

	/**
	 * This is only to be used internally to provide the MQTT id of a message
	 * received from the server.  Has no effect when publishing messages.
	 *
	 * @param messageId The Message ID
	 */
	public void setId(int messageId) {
		this.messageId = messageId;
	}

	/**
	 * Returns the MQTT id of the message.  This is only applicable to messages
	 * received from the server.
	 *
	 * @return the MQTT id of the message
	 */
	public int getId() {
		return this.messageId;
	}

	// Additional useful fields that a client can use

	/**
	 * Returns the URI of the server which sent the message.  This is only applicable to messages
	 * received from the server. This is useful in failover scenarios when we want to know the broker.
	 *
	 * @return the URI of the message
	 */
	public String getURI() {
		return URI;
	}

	/**
	 * This is only to be used internally to record the URI of the server a message
	 * was received from.  Has no effect when publishing messages.
	 *
	 * @param uri The broker URI which the incoming message came from.
	 */
	public void setURI(String uri) {
		this.URI = uri;
	}

}