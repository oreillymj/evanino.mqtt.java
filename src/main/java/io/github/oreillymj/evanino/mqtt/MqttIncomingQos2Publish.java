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

import io.netty.channel.EventLoop;
import io.netty.handler.codec.mqtt.*;

import java.util.function.Consumer;

/**
 * Tracks an incoming QoS 2 PUBLISH between sending PUBREC and receiving the broker's PUBREL,
 * retransmitting the PUBREC until the PUBREL arrives.
 */
final class MqttIncomingQos2Publish {

    private final MqttPublishMessage incomingPublish;

    /**
     * Whether this tracker holds a reference to the payload, released when the message is
     * delivered at PUBREL or by {@link #discard()}. Without manual acknowledgements the payload
     * is kept until PUBREL; with them the message has already been delivered.
     */
    private boolean ownsPayload;

    private final RetransmissionHandler<io.netty.handler.codec.mqtt.MqttMessage> retransmissionHandler = new RetransmissionHandler<>();

    /**
     * Creates a tracker for an incoming QoS 2 publish.
     *
     * @param incomingPublish the PUBLISH received from the broker
     * @param originalMessage the PUBREC sent in response, retransmitted until PUBREL arrives
     * @param ownsPayload     whether the tracker holds a reference to the payload, to be
     *                        released on delivery or by {@link #discard()}
     */
    MqttIncomingQos2Publish(MqttPublishMessage incomingPublish, io.netty.handler.codec.mqtt.MqttMessage originalMessage,
                            boolean ownsPayload) {
        this.incomingPublish = incomingPublish;
        this.ownsPayload = ownsPayload;
        this.retransmissionHandler.setOriginalMessage(originalMessage);
    }

    /**
     * Hands the payload reference to the caller, which delivers the message and releases it.
     * Later calls, and {@link #discard()}, then leave the payload alone.
     *
     * @return {@code true} if the tracker held the reference
     */
    boolean takePayload() {
        boolean owned = ownsPayload;
        ownsPayload = false;
        return owned;
    }

    /**
     * Stops PUBREC retransmission and releases the payload if the tracker still holds it, for a
     * message that will not be delivered: replaced by a resent PUBLISH, or dropped with its session.
     */
    void discard() {
        onPubrelReceived();
        if (takePayload()) {
            incomingPublish.payload().release();
        }
    }

    /**
     * Returns the PUBLISH received from the broker.
     *
     * @return the incoming publish message
     */
    MqttPublishMessage getIncomingPublish() {
        return incomingPublish;
    }

    /**
     * Starts periodically retransmitting the PUBREC until {@link #onPubrelReceived()} is called.
     *
     * @param eventLoop  the event loop used to schedule retransmissions
     * @param sendPacket writes a packet to the channel
     * @param intervalSeconds the delay before the first resend on a live connection, or 0 for none
     */
    void startPubrecRetransmitTimer(EventLoop eventLoop, Consumer<Object> sendPacket, int intervalSeconds) {
        this.retransmissionHandler.setHandle((fixedHeader, originalMessage) ->
                sendPacket.accept(new io.netty.handler.codec.mqtt.MqttMessage(fixedHeader, originalMessage.variableHeader())));
        this.retransmissionHandler.start(eventLoop, intervalSeconds);
    }

    /**
     * Stops PUBREC retransmission once the broker's PUBREL has been received.
     */
    void onPubrelReceived() {
        this.retransmissionHandler.stop();
    }
}
