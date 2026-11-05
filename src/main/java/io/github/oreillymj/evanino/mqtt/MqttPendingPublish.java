/*******************************************************************************
 * Copyright (c) 2017, 2026 Michael OReilly
 *
 * This program and the accompanying materials are made available under
 * the terms of the Apache License, Version 2.0, which accompanies this
 * distribution (see LICENSE).
 *
 * The Apache License is available at
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Based on netty-mqtt-client by JetLinks (https://github.com/jetlinks/netty-mqtt-client),
 * itself based on netty-mqtt by Jeffrey Kog (https://github.com/jeffreykog/netty-mqtt);
 * see NOTICE.
 *
 * Contributors:
 *    Jeffrey Kog - original netty-mqtt implementation
 *    zhouhao (JetLinks) - netty-mqtt-client
 *    Michael OReilly - Paho-compatible API, and changes since
 *******************************************************************************/

package io.github.oreillymj.evanino.mqtt;

import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoop;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Tracks an outgoing QoS 1 or QoS 2 PUBLISH until the broker completes the acknowledgement
 * flow. The PUBLISH is retransmitted until PUBACK (QoS 1) or PUBREC (QoS 2) arrives, and for
 * QoS 2 the PUBREL is then retransmitted until PUBCOMP arrives.
 */
final class MqttPendingPublish {

    private final int messageId;
    private final MqttDeliveryToken token;
    private final ByteBuf payload;
    private final MqttPublishMessage message;
    private final MqttQoS qos;
    private int maxInflightRetries = -1;

    private final RetransmissionHandler<MqttPublishMessage> publishRetransmissionHandler = new RetransmissionHandler<>();
    private final RetransmissionHandler<MqttMessage> pubrelRetransmissionHandler = new RetransmissionHandler<>();

    /** Orders publishes by creation, so they are resent in the order they were made. */
    private static final AtomicLong SEQUENCE = new AtomicLong();

    private final long sequence = SEQUENCE.incrementAndGet();
    private volatile boolean sent = false;
    /** Set once the PUBLISH has been written on some connection; a resend then sets DUP. */
    private volatile boolean everSent = false;
    /** The PUBREL sent for a QoS 2 publish once PUBREC arrived, or {@code null} before. */
    private volatile MqttMessage pubrelMessage;

    /**
     * Creates a pending publish with no limit on PUBLISH retransmissions.
     *
     * @param messageId the packet identifier
     * @param token     the delivery token to complete or fail
     * @param payload   the message payload, retained for retransmission
     * @param message   the PUBLISH packet
     * @param qos       the QoS of the publish
     */
    MqttPendingPublish(int messageId, MqttDeliveryToken token, ByteBuf payload, MqttPublishMessage message, MqttQoS qos) {
        this.messageId = messageId;
        this.token=token;
        this.payload = payload;
        this.message = message;
        this.qos = qos;
        this.publishRetransmissionHandler.setOriginalMessage(message);
    }

    /**
     * Creates a pending publish with a limit on PUBLISH and PUBREL retransmissions.
     *
     * @param messageId          the packet identifier
     * @param token              the delivery token to complete or fail
     * @param payload            the message payload, retained for retransmission
     * @param message            the PUBLISH packet
     * @param qos                the QoS of the publish
     * @param maxInflightRetries the maximum number of retransmissions before the token fails
     *                           with {@link MqttException#REASON_CODE_CLIENT_TIMEOUT}; values
     *                           of 0 or less mean no limit
     */
    MqttPendingPublish(int messageId, MqttDeliveryToken token, ByteBuf payload, MqttPublishMessage message, MqttQoS qos, int maxInflightRetries) {
        this.messageId = messageId;
        this.token=token;
        this.payload = payload;
        this.message = message;
        this.qos = qos;
        this.maxInflightRetries = maxInflightRetries;

        if (this.maxInflightRetries>0) {
            this.publishRetransmissionHandler.setMaxInflightRetries(this.maxInflightRetries);
            this.pubrelRetransmissionHandler.setMaxInflightRetries(this.maxInflightRetries);
        }

        this.publishRetransmissionHandler.setOriginalMessage(message);
    }

    /**
     * Returns the packet identifier.
     *
     * @return the packet identifier
     */
    int getMessageId() {
        return messageId;
    }

    /**
     * Returns the delivery token for this publish.
     *
     * @return the delivery token
     */
    MqttDeliveryToken getToken() { return token; }

    /**
     * Returns the message payload.
     *
     * @return the payload buffer
     */
    ByteBuf getPayload() {
        return payload;
    }

    /**
     * Returns whether the PUBLISH has been written to the channel.
     *
     * @return {@code true} once sent
     */
    boolean isSent() {
        return sent;
    }

    /**
     * Records whether the PUBLISH has been written to the channel.
     *
     * @param sent {@code true} once sent
     */
    void setSent(boolean sent) {
        this.sent = sent;
        if (sent) {
            this.everSent = true;
        }
    }

    /**
     * Returns the order in which this publish was made relative to others.
     *
     * @return a number that increases with each publish made
     */
    long getSequence() {
        return sequence;
    }

    /**
     * Returns whether the PUBLISH has been written on a connection at some time, so that
     * sending it again is a resend.
     *
     * @return {@code true} once it has been sent on any connection
     */
    boolean wasEverSent() {
        return everSent;
    }

    /**
     * Returns the PUBREL to send for a QoS 2 publish whose PUBREC has arrived.
     *
     * @return the PUBREL, or {@code null} if PUBREC has not arrived
     */
    MqttMessage getPubrelMessage() {
        return pubrelMessage;
    }

    /**
     * Returns the PUBLISH to send again on a new connection: a copy with the DUP flag set,
     * holding its own reference to the payload.
     *
     * @return the PUBLISH to resend
     */
    MqttPublishMessage newDuplicate() {
        return new MqttPublishMessage(
                new io.netty.handler.codec.mqtt.MqttFixedHeader(message.fixedHeader().messageType(), true,
                        message.fixedHeader().qosLevel(), message.fixedHeader().isRetain(), 0),
                message.variableHeader(), payload.retain());
    }

    /**
     * Keeps the publish for the session after its connection has been lost: stops
     * retransmission and marks it unsent, so the next CONNACK sends it again.
     */
    void suspend() {
        publishRetransmissionHandler.stop();
        pubrelRetransmissionHandler.stop();
        this.sent = false;
    }

    /**
     * Returns the PUBLISH packet.
     *
     * @return the publish message
     */
    MqttPublishMessage getMessage() {
        return message;
    }

    /**
     * Returns the QoS of the publish.
     *
     * @return the QoS level
     */
    MqttQoS getQos() {
        return qos;
    }

    /**
     * Starts retransmitting the PUBLISH, with the DUP flag set, until
     * {@link #onPubackReceived()} is called. If the retry limit is exceeded the delivery token
     * fails with {@link MqttException#REASON_CODE_CLIENT_TIMEOUT}.
     *
     * @param eventLoop  the event loop used to schedule retransmissions
     * @param sendPacket writes a packet to the channel
     * @param intervalSeconds the delay before the first resend on a live connection, or 0 for none
     */
    void startPublishRetransmissionTimer(EventLoop eventLoop, Consumer<Object> sendPacket, int intervalSeconds) {
        this.publishRetransmissionHandler.setHandle(((fixedHeader, originalMessage) ->
                sendPacket.accept(new MqttPublishMessage(fixedHeader, originalMessage.variableHeader(), this.payload.retain()))));
        this.publishRetransmissionHandler.setOnMaxRetriesExceeded(() ->
                this.token.markFailed(new MqttException(MqttException.REASON_CODE_CLIENT_TIMEOUT)));
        this.publishRetransmissionHandler.start(eventLoop, intervalSeconds);
    }

    /**
     * Stops PUBLISH retransmission. Called when PUBACK (QoS 1) or PUBREC (QoS 2) arrives.
     */
    void onPubackReceived() {
        this.publishRetransmissionHandler.stop();
    }

    /**
     * Sets the PUBREL packet to retransmit for a QoS 2 publish.
     *
     * @param pubrelMessage the PUBREL sent in response to PUBREC
     */
    void setPubrelMessage(MqttMessage pubrelMessage) {
        this.pubrelMessage = pubrelMessage;
        this.pubrelRetransmissionHandler.setOriginalMessage(pubrelMessage);
    }

    /**
     * Starts retransmitting the PUBREL until {@link #onPubcompReceived()} is called. If the
     * retry limit is exceeded the delivery token fails with
     * {@link MqttException#REASON_CODE_CLIENT_TIMEOUT}.
     * {@link #setPubrelMessage(MqttMessage)} must be called first.
     *
     * @param eventLoop  the event loop used to schedule retransmissions
     * @param sendPacket writes a packet to the channel
     * @param intervalSeconds the delay before the first resend on a live connection, or 0 for none
     */
    void startPubrelRetransmissionTimer(EventLoop eventLoop, Consumer<Object> sendPacket, int intervalSeconds) {
        this.pubrelRetransmissionHandler.setHandle((fixedHeader, originalMessage) ->
                sendPacket.accept(new MqttMessage(fixedHeader, originalMessage.variableHeader())));
        this.pubrelRetransmissionHandler.setOnMaxRetriesExceeded(() ->
                this.token.markFailed(new MqttException(MqttException.REASON_CODE_CLIENT_TIMEOUT)));
        this.pubrelRetransmissionHandler.start(eventLoop, intervalSeconds);
    }

    /**
     * Stops PUBREL retransmission once PUBCOMP arrives.
     */
    void onPubcompReceived() {
        this.pubrelRetransmissionHandler.stop();
    }
}
