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
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Tracks an UNSUBSCRIBE that has been sent but not yet acknowledged by the broker,
 * retransmitting it until the UNSUBACK arrives.
 */
final class MqttPendingUnsubscription {

    private final MqttToken<Void> token; // was: Promise<Void> future
    private final List<String> topics;
    private final MqttUnsubscribeMessage unsubscribeMessage;
    private final Set<MqttPendingUnsubscribeHandler> handlers = new HashSet<>();
    private final RetransmissionHandler<MqttUnsubscribeMessage> retransmissionHandler = new RetransmissionHandler<>();


    /**
     * Creates a pending unsubscription for several topic filters.
     *
     * @param token              the token to complete when the UNSUBACK arrives
     * @param topics             the topic filters being unsubscribed
     * @param unsubscribeMessage the UNSUBSCRIBE packet, retransmitted until acknowledged
     */
    MqttPendingUnsubscription(MqttToken<Void> token, List<String> topics, MqttUnsubscribeMessage unsubscribeMessage) {
        this.token = token;
        this.topics = topics;
        this.unsubscribeMessage=unsubscribeMessage;
        this.retransmissionHandler.setOriginalMessage(unsubscribeMessage);
    }


    /**
     * Creates a pending unsubscription for a single topic filter.
     *
     * @param token              the token to complete when the UNSUBACK arrives
     * @param topic              the topic filter being unsubscribed
     * @param unsubscribeMessage the UNSUBSCRIBE packet, retransmitted until acknowledged
     */
    MqttPendingUnsubscription(MqttToken<Void> token, String topic, MqttUnsubscribeMessage unsubscribeMessage) {
        this.token = token;
        List<String> topics = new ArrayList<String>();
        topics.add(topic);
        this.topics = topics;
        this.unsubscribeMessage=unsubscribeMessage;
        this.retransmissionHandler.setOriginalMessage(unsubscribeMessage);
    }

    /**
     * Returns the token completed when the UNSUBACK arrives.
     *
     * @return the unsubscribe token
     */
    MqttToken<Void> getToken() { return token; } // was: getFuture()
    /**
     * Returns the topic filters being unsubscribed.
     *
     * @return the topic filters
     */
    List<String> getTopics() { return topics; }
    /**
     * Returns the first topic filter being unsubscribed.
     *
     * @return the first topic filter
     */
    String getTopic() { return topics.get(0); }
    /**
     * Returns the UNSUBSCRIBE packet.
     *
     * @return the unsubscribe message
     */
    MqttUnsubscribeMessage  getUnSubscribeMessage() { return unsubscribeMessage; }

    /**
     * Registers a callback associated with this unsubscription.
     *
     * @param handler the callback
     * @param once    whether the callback should fire only once
     */
    void addHandler(MqttCallback handler, boolean once) {
        this.handlers.add(new MqttPendingUnsubscribeHandler(handler, once));
    }

    /**
     * Returns the callbacks registered with {@link #addHandler(MqttCallback, boolean)}.
     *
     * @return the registered handlers
     */
    Set<MqttPendingUnsubscribeHandler> getHandlers() { return handlers; }

    /**
     * Starts periodically retransmitting the UNSUBSCRIBE, with the DUP flag set, until
     * {@link #onUnsubackReceived()} is called.
     *
     * @param eventLoop  the event loop used to schedule retransmissions
     * @param sendPacket writes a packet to the channel
     * @param intervalSeconds the delay before the first resend on a live connection, or 0 for none
     */
    void startRetransmissionTimer(EventLoop eventLoop, Consumer<Object> sendPacket, int intervalSeconds) {
        this.retransmissionHandler.setHandle((fixedHeader, originalMessage) ->
                sendPacket.accept(new MqttUnsubscribeMessage(fixedHeader, originalMessage.variableHeader(), originalMessage.payload())));
        this.retransmissionHandler.start(eventLoop, intervalSeconds);
    }

    /**
     * Stops retransmission once the broker's UNSUBACK has been received.
     */
    void onUnsubackReceived() {
        this.retransmissionHandler.stop();
    }

    /**
     * A callback registered against a pending unsubscription.
     */
    static final class MqttPendingUnsubscribeHandler {
        private final MqttCallback handler;
        private final boolean once;

        /**
         * Creates a handler entry.
         *
         * @param handler the callback
         * @param once    whether the callback should fire only once
         */
        MqttPendingUnsubscribeHandler(MqttCallback handler, boolean once) {
            this.handler = handler;
            this.once = once;
        }

        /**
         * Returns the callback.
         *
         * @return the callback
         */
        MqttCallback getHandler() { return handler; }
        /**
         * Returns whether the callback should fire only once.
         *
         * @return {@code true} for a one-shot callback
         */
        boolean isOnce() { return once; }
    }

}