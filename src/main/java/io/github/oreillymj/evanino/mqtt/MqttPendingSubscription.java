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
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * Tracks a SUBSCRIBE that has been sent but not yet acknowledged by the broker,
 * retransmitting it until the SUBACK arrives, and holds the callbacks to register for each
 * topic once it is granted.
 */
final class MqttPendingSubscription {

	private final MqttToken<Void> token;
	private final List<String> topics;
	private final List<MqttQoS> qosLevels;
	private final MqttSubscribeMessage subscribeMessage;
	private final RetransmissionHandler<MqttSubscribeMessage> retransmissionHandler = new RetransmissionHandler<>();
	private final java.util.concurrent.atomic.AtomicBoolean sent = new java.util.concurrent.atomic.AtomicBoolean();

	// One collection for both use cases. An entry with topic == null applies to every
	// topic in the packet; an entry with a topic applies to that topic only.
	// Copy-on-write: addHandler() runs on the app thread, handleSubAck() iterates on the
	// I/O thread, writes are rare and the list is tiny.
	private final List<MqttPendingSubscribeHandler> handlers = new CopyOnWriteArrayList<>();

	/**
	 * Creates a pending subscription for several topic filters.
	 *
	 * @param token     the token to complete when the SUBACK arrives
	 * @param topics    the topic filters being subscribed
	 * @param message   the SUBSCRIBE packet, retransmitted until acknowledged
	 * @param qosLevels the requested QoS for each topic filter, in the same order
	 */
	MqttPendingSubscription(MqttToken<Void> token, List<String> topics, MqttSubscribeMessage message, List<MqttQoS> qosLevels) {
		this.token = token;
		this.topics = topics;
		this.subscribeMessage = message;
		this.qosLevels = qosLevels;
		this.retransmissionHandler.setOriginalMessage(message);
	}

	/**
	 * Creates a pending subscription for a single topic filter.
	 *
	 * @param token   the token to complete when the SUBACK arrives
	 * @param topic   the topic filter being subscribed
	 * @param message the SUBSCRIBE packet, retransmitted until acknowledged
	 * @param qos     the requested QoS
	 */
	MqttPendingSubscription(MqttToken<Void> token, String topic, MqttSubscribeMessage message, MqttQoS qos) {
		this(token, Collections.singletonList(topic), message, Collections.singletonList(qos));
	}

	// ---- handler registration -------------------------------------------------------

	/**
	 * Handler applies to every topic in this SUBSCRIBE (single-topic and shared-handler batches).
	 *
	 * @param handler the callback for matching messages; {@code null} is ignored
	 * @param once    whether the callback should fire only for the first matching message
	 */
	void addHandler(MqttCallback handler, boolean once) {
		addHandler(handler, once, null);
	}

	/**
	 * Handler applies only to {@code topic} (batch subscribe with per-topic listeners).
	 *
	 * @param handler the callback for matching messages; {@code null} is ignored
	 * @param once    whether the callback should fire only for the first matching message
	 * @param topic   the topic filter the handler applies to, or {@code null} for every topic
	 */
	void addHandler(MqttCallback handler, boolean once, String topic) {
		if (handler != null) {
			handlers.add(new MqttPendingSubscribeHandler(handler, once, topic));
		}
	}

	/**
	 * The handlers that should be registered when the broker grants {@code topic}.
	 *
	 * @param topic the granted topic filter
	 * @return the handlers registered for that topic or for every topic
	 */
	List<MqttPendingSubscribeHandler> getHandlersFor(String topic) {
		List<MqttPendingSubscribeHandler> result = new ArrayList<>();
		for (MqttPendingSubscribeHandler h : handlers) {
			if (h.topic == null || h.topic.equals(topic)) {
				result.add(h);
			}
		}
		return result;
	}

	// ---- unchanged accessors ----------------------------------------------------------

	/**
	 * Returns the token completed when the SUBACK arrives.
	 *
	 * @return the subscribe token
	 */
	MqttToken<Void> getToken() {
		return token;
	}

	/**
	 * Returns the topic filters being subscribed.
	 *
	 * @return the topic filters
	 */
	List<String> getTopics() {
		return topics;
	}

	/**
	 * Returns the requested QoS for each topic filter.
	 *
	 * @return the QoS levels, in topic order
	 */
	List<MqttQoS> getQosLevels() {
		return qosLevels;
	}

	/**
	 * Returns the first topic filter being subscribed.
	 *
	 * @return the first topic filter
	 */
	String getTopic() {
		return topics.get(0);
	}

	/**
	 * Returns the requested QoS of the first topic filter.
	 *
	 * @return the first QoS level
	 */
	MqttQoS getQOS() {
		return qosLevels.get(0);
	}

	/**
	 * Returns whether the SUBSCRIBE has been written to the channel.
	 *
	 * @return {@code true} once sent
	 */
	boolean isSent() {
		return sent.get();
	}

	/**
	 * Records whether the SUBSCRIBE has been written to the channel.
	 *
	 * @param sent {@code true} once sent
	 */
	void setSent(boolean sent) {
		this.sent.set(sent);
	}

	/**
	 * Claims the sending of the SUBSCRIBE: returns {@code true} to exactly one caller, which
	 * must then send it (and {@link #setSent(boolean) clear} the claim if it cannot). The
	 * subscribing thread and the CONNACK handler, which sends subscribes made before the
	 * connection was ready, can both try; without the claim both could send it.
	 *
	 * @return {@code true} if the caller is to send the SUBSCRIBE
	 */
	boolean claimSend() {
		return sent.compareAndSet(false, true);
	}

	/**
	 * Returns the SUBSCRIBE packet.
	 *
	 * @return the subscribe message
	 */
	MqttSubscribeMessage getSubscribeMessage() {
		return subscribeMessage;
	}

	/**
	 * Starts retransmitting the SUBSCRIBE, with the DUP flag set, until
	 * {@link #onSubackReceived()} is called. Does nothing unless the packet has been sent.
	 *
	 * @param eventLoop  the event loop used to schedule retransmissions
	 * @param sendPacket writes a packet to the channel
	 * @param intervalSeconds the delay before the first resend on a live connection, or 0 for none
	 */
	void startRetransmitTimer(EventLoop eventLoop, Consumer<Object> sendPacket, int intervalSeconds) {
		if (this.sent.get()) {
			this.retransmissionHandler.setHandle((fixedHeader, originalMessage) ->
					sendPacket.accept(new MqttSubscribeMessage(fixedHeader, originalMessage.variableHeader(), originalMessage.payload())));
			this.retransmissionHandler.start(eventLoop, intervalSeconds);
		}
	}

	/**
	 * Stops retransmission once the broker's SUBACK has been received.
	 */
	void onSubackReceived() {
		this.retransmissionHandler.stop();
	}

	/**
	 * A callback waiting to be registered when a topic is granted.
	 */
	static final class MqttPendingSubscribeHandler {
		private final MqttCallback handler;
		private final boolean once;
		private final String topic;   // null = all topics in the packet

		/**
		 * Creates a handler entry.
		 *
		 * @param handler the callback
		 * @param once    whether the callback should fire only once
		 * @param topic   the topic filter the handler applies to, or {@code null} for every topic
		 */
		MqttPendingSubscribeHandler(MqttCallback handler, boolean once, String topic) {
			this.handler = handler;
			this.once = once;
			this.topic = topic;
		}

		/**
		 * Returns the callback.
		 *
		 * @return the callback
		 */
		MqttCallback getHandler() {
			return handler;
		}

		/**
		 * Returns whether the callback should fire only once.
		 *
		 * @return {@code true} for a one-shot callback
		 */
		boolean isOnce() {
			return once;
		}
	}
}