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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;
import io.netty.handler.codec.mqtt.*;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.Promise;

import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

// https://stackoverflow.com/questions/50612403/catch-all-exception-handling-for-outbound-channelhandler
/**
 * Last handler in the client pipeline, implementing the MQTT protocol flow. It sends CONNECT
 * once the channel (and, for WebSockets, the handshake) is ready, and handles every packet
 * from the broker: CONNACK, SUBACK, UNSUBACK, incoming PUBLISH with its QoS 1 and 2
 * acknowledgements, and the PUBACK / PUBREC / PUBCOMP flow for outgoing publishes. Incoming
 * messages are dispatched to subscription callbacks on the client's callback executor.
 */
final class MqttChannelHandler extends SimpleChannelInboundHandler<io.netty.handler.codec.mqtt.MqttMessage> {

	private final String TAG = "MqttChannelHandler->";
	private boolean enableLogging = false;

	private SimpleLogger logger = new SimpleLogger();

	private final MqttAsyncClient client;
	private Promise<MqttConnectResult> connectFuture;

	private volatile boolean channelActive = false;


	/**
	 * Lifecycle of the handler.
	 */
	private enum State {
		/** CONNECT not yet sent. */
		NEW,
		/** CONNECT sent; handling packets. */
		INITIALIZED,
		/** Channel gone; will not be re-initialized. */
		DESTROYED
	}

	private State state = State.NEW;

	private boolean webSocketHandShakeComplete = false;

	private final Map<Integer, PendingIncomingAck> pendingIncomingAcks = new ConcurrentHashMap<>();
	private final Deque<Integer> pendingAckOrder = new ConcurrentLinkedDeque<>();


    /*
=====================================================================================================================
|	Clean Session Flag	|	Retain Flag	|	Subscribe QOS	| Publish QOS	|	Published Message Always Received	|
=====================================================================================================================
|	      TRUE      	|	   FALSE	|	       0	    |		0		|				No						|
|	      TRUE	        |	   FALSE	|	       0	    |		1		|				No						|
|	      TRUE	        |	   FALSE	|	       1	    |		0		|				No						|
|	      TRUE	        |	   FALSE	|	       1	    |		1		|				No						|
|	      FALSE	        |	   FALSE	|	       0	    |		0		|				No						|
|	      FALSE	        |	   FALSE	|	       0	    |		1		|				No						|
|	      FALSE	        |	   FALSE	|	       1	    |		0		|				No						|
|	      FALSE	        |	   FALSE	|	       1	    |		1		|			Yes -All Messages			|
|	                	|		        |	        		|				|										|
|	      TRUE	        |	   TRUE	    |	       0		|		0		|			Yes - Last Message Only		|
|	      TRUE	        |	   TRUE	    |	       0		|		1		|			Yes - Last Message Only		|
|	      TRUE	        |	   TRUE	    |	       1		|		0		|			Yes - Last Message Only		|
|	      TRUE	        |	   TRUE	    |	       1		|		1		|			Yes - Last Message Only		|
|	      FALSE	        |	   TRUE	    |	       0		|		0		|			Yes - Last Message Only		|
|	      FALSE	        |	   TRUE	    |	       0		|		1		|			Yes - Last Message Only		|
|	      FALSE	        |	   TRUE	    |	       1		|		0		|			Yes - Last Message Only		|
|	      FALSE	        |	   TRUE	    |	       1		|		1		|			Yes -All Messages			|
=====================================================================================================================
*/

/*
Publish sequence

Outgoing
Client QOS0 PUB
Client QOS1 PUB : Broker PUBACK
Client QOS2 PUB : Broker PUBREC : Client PUBREL : Broker PUBCOMP


Incoming
Client QOS0 PUB
Client QOS1 PUB : Send PUBACK
Client QOS2 PUB : Send PUBREC : Broker PUBREL : Send PUBCOMP

 */

	/**
	 * Logs a message if channel logging is enabled.
	 *
	 * @param data the message to log
	 */
	private void log(String data) {
		if (enableLogging) {
			logger.log(data);
		}
	}

	/**
	 * Logs a message with the given tag if channel logging is enabled.
	 *
	 * @param tag  the prefix written before the message
	 * @param data the message to log
	 */
	private void log(String tag, String data) {
		if (enableLogging) {
			logger.log(tag + data);
		}
	}

	/**
	 * Creates the protocol handler for one connection attempt.
	 *
	 * @param client        the client that owns the connection
	 * @param connectFuture the future to complete when CONNACK is received
	 */
	MqttChannelHandler(MqttAsyncClient client, Promise<MqttConnectResult> connectFuture) {
		this.client = client;
		this.connectFuture = connectFuture;
		enableLogging = client.getClientConfig().getChannelLogging();
	}

	/**
	 * Propagates the event, then sends CONNECT once a WebSocket handshake has completed.
	 *
	 * @param ctx the channel handler context
	 * @param evt the user event
	 * @throws Exception if the superclass fails
	 */
	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
		super.userEventTriggered(ctx, evt);
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "userEventTriggered()->");
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "userEventTriggered()-> " + evt.toString());

		if (evt instanceof WebSocketClientProtocolHandler.ClientHandshakeStateEvent) {
			WebSocketClientProtocolHandler.ClientHandshakeStateEvent handshakeEvent =
					(WebSocketClientProtocolHandler.ClientHandshakeStateEvent) evt;
			if (handshakeEvent == WebSocketClientProtocolHandler.ClientHandshakeStateEvent.HANDSHAKE_COMPLETE) {
				if (BuildConfig.LOGGING && enableLogging) log(TAG, "userEventTriggered()->WebSocket handshake complete. Connecting....");
				webSocketHandShakeComplete = true;
				this.initialize(ctx);
			}
		}
	}

	/**
	 * Sends CONNECT if the channel is already active and registered; otherwise this happens
	 * in {@link #channelActive(ChannelHandlerContext)}.
	 *
	 * @param ctx the channel handler context
	 * @throws Exception if the superclass fails
	 */
	@Override
	public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
		super.handlerAdded(ctx);
		if (ctx.channel().isActive() && ctx.channel().isRegistered()) {
			// channelActive() event has been fired already, which means this.channelActive() will
			// not be invoked. We have to initialize here instead.
			channelActive = true;
			this.initialize(ctx);
		} else {
			// channelActive() event has not been fired yet.  this.channelActive() will be invoked
			// and initialization will occur there.
		}
	}

	/**
	 * Stops all retransmission timers and clears in-flight state.
	 *
	 * @param ctx the channel handler context
	 * @throws Exception if the superclass fails
	 */
	@Override
	public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
		destroy();
		super.handlerRemoved(ctx);
	}

	/**
	 * Sends CONNECT early if the channel is already active, then propagates the event.
	 *
	 * @param ctx the channel handler context
	 * @throws Exception if the superclass fails
	 */
	@Override
	public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
		// Initialize early if channel is active already.
		if (ctx.channel().isActive()) {
			this.initialize(ctx);

		}
		super.channelRegistered(ctx);
	}


	/**
	 * Dispatches a packet from the broker to the matching handler. Packets are ignored until
	 * the handler has been initialized. A packet that could not be decoded closes the
	 * connection (see {@link MqttAsyncClient#decodeFailed(Channel, Throwable)}).
	 *
	 * @param ctx the channel handler context
	 * @param msg the decoded MQTT packet
	 * @throws Exception if handling fails
	 */
	protected void channelRead0(ChannelHandlerContext ctx, io.netty.handler.codec.mqtt.MqttMessage msg) throws Exception {
		if (msg.decoderResult().isFailure()) {
			// The decoder discards everything after a bad packet: the connection is unusable.
			this.client.decodeFailed(ctx.channel(), msg.decoderResult().cause());
			return;
		}
		if (!channelActive) {
			return;
		}
		switch (msg.fixedHeader().messageType()) {
			case CONNACK:
				handleConack(ctx.channel(), (MqttConnAckMessage) msg);
				break;
			case SUBACK:
				handleSubAck((MqttSubAckMessage) msg);
				break;
			case PUBLISH:
				handleIncomingPublish(ctx.channel(), (MqttPublishMessage) msg);
				break;
			case UNSUBACK:
				handleUnsuback((MqttUnsubAckMessage) msg);
				break;
			case PUBACK:
				handlePuback((MqttPubAckMessage) msg);
				break;
			case PUBREC:
				handlePubrec(ctx.channel(), msg);
				break;
			case PUBREL:
				handlePubrel(ctx.channel(), msg);
				break;
			case PUBCOMP:
				handlePubcomp(ctx.channel(), msg);
				break;
			case PINGREQ:
				handlePingReq(ctx.channel());
				break;
			case PINGRESP:
				handlePingResp(ctx.channel());
				break;
			case DISCONNECT:
				handleDisconnect(ctx.channel());
				break;
		}
	}

	/**
	 * Logs the exception and closes the channel if it is no longer active and writable; a
	 * decoder exception closes it in any case (see
	 * {@link MqttAsyncClient#decodeFailed(Channel, Throwable)}). The exception is not propagated
	 * further.
	 *
	 * @param channelhandlercontext the channel handler context
	 * @param throwable             the exception
	 * @throws Exception declared by the superclass; not thrown
	 */
	@Override
	public void exceptionCaught(ChannelHandlerContext channelhandlercontext, Throwable throwable) throws Exception {

		if (BuildConfig.LOGGING && enableLogging) log(TAG, "Exception caught!!! " + throwable.toString());

		if (NettyDebug.PRINT_STACK_TRACES) {
			throwable.printStackTrace();
		}

		if (throwable instanceof io.netty.handler.codec.DecoderException) {
			// A WebSocket frame that could not be decoded, for example one over the size limit:
			// as for an MQTT packet that could not be decoded, the connection is unusable.
			this.client.decodeFailed(channelhandlercontext.channel(), throwable);
			return;
		}


		if (!(channelhandlercontext.channel().isActive() && channelhandlercontext.channel().isWritable())) {
			channelhandlercontext.close();
		}

		// https://www.baeldung.com/netty-exception-handling
	}

	/**
	 * Ends the work in flight on this connection, failing it with
	 * {@link MqttException#REASON_CODE_CONNECTION_LOST} or keeping it for the session (see
	 * {@link MqttAsyncClient#failInFlight(Throwable)}), forgets the connection's subscription
	 * state (keeping subscriptions for a persistent session, see
	 * {@link MqttAsyncClient#connectionEnded()}), propagates the event and stops retransmission
	 * timers.
	 *
	 * @param ctx the channel handler context
	 * @throws Exception if the superclass fails
	 */
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "channelInactive!!! ");
		MqttException cause = new MqttException(MqttException.REASON_CODE_CONNECTION_LOST);

		// Normally already done when the channel's close future completed.
		this.client.failInFlight(cause);
		this.client.connectionEnded();

		super.channelInactive(ctx);
		this.destroy();
	}

	/**
	 * Propagates the event, then sends CONNECT.
	 *
	 * @param ctx the channel handler context
	 * @throws Exception if the superclass fails
	 */
	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		super.channelActive(ctx);

		if (BuildConfig.LOGGING && enableLogging) log(TAG, "channelActive");
		initialize(ctx);
	}


	/**
	 * Builds and sends the CONNECT packet from the client's options, the first time it is
	 * called. For WebSocket connections it waits until the handshake has completed. Later calls,
	 * and calls after {@link #destroy()}, do nothing.
	 *
	 * @param ctx the channel handler context
	 */
	private synchronized void initialize(ChannelHandlerContext ctx) {
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "initialize()->state=" + state.name());
		switch (state) {
			case NEW:
				if ((this.client.getClientConfig().isWebSocket()) && (!webSocketHandShakeComplete)) {
					return;
				}
				break;
			case INITIALIZED:
				return;
			case DESTROYED:
				return;
		}

		state = State.INITIALIZED;
		enableLogging = this.client.getClientConfig().getChannelLogging();
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "initialize()->state set to=" + state.name());
		channelActive = true;
		MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.CONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0);
		MqttConnectVariableHeader variableHeader = new MqttConnectVariableHeader(
				this.client.getClientConfig().getMqttVersion().protocolName(),  // Protocol Name
				this.client.getClientConfig().getMqttVersion().protocolLevel(), // Protocol Level
				this.client.getClientConfig().getUserName() != null,                // Has Username
				this.client.getClientConfig().getPassword() != null,                // Has Password
				this.client.getClientConfig().getLastWill() != null                 // Will Retain
						&& this.client.getClientConfig().getLastWill().isRetain(),
				this.client.getClientConfig().getLastWill() != null                 // Will QOS
						? this.client.getClientConfig().getLastWill().getQos().value()
						: 0,
				this.client.getClientConfig().getLastWill() != null,                // Has Will
				this.client.getClientConfig().isCleanSession(),                     // Clean Session
				this.client.getClientConfig().getConnectKeepAlive()                    // Keep Alive; 0 when off
		);
		String clientId = this.client.getClientConfig().getConnectClientId(); // truncated for MQTT 3.1
		MqttConnectPayload payload = new MqttConnectPayload(
				clientId,
				this.client.getClientConfig().getLastWill() != null ? this.client.getClientConfig().getLastWill().getTopic() : null,
				this.client.getClientConfig().getLastWill() != null ? this.client.getClientConfig().getLastWill().getPayload() : null,   // the bytes as given
				this.client.getClientConfig().getUserName(),
				this.client.getClientConfig().getPasswordString() != null ? this.client.getClientConfig().getPasswordString().getBytes(CharsetUtil.UTF_8) : null
		);

		if (BuildConfig.LOGGING && enableLogging) log(TAG, "initialize()->ClientID=" + clientId);
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "initialize()->sending MqttConnectMessage");
		Channel channel = ctx.channel();
		channel.writeAndFlush(new MqttConnectMessage(fixedHeader, variableHeader, payload)).addListener(f -> {
			if (!f.isSuccess()) {
				// For example the encoder rejecting a field. Without this the connect would wait
				// for a CONNACK that can never come, as the broker never received CONNECT. If the
				// client closed the channel itself (a connect timeout), closing reports that.
				if (BuildConfig.LOGGING && enableLogging) log(TAG, "initialize()->failed to send CONNECT: " + f.cause());
				if (!this.client.isClosingWithError(channel)) {
					connectFuture.tryFailure(new ChannelClosedException("Failed to send CONNECT", f.cause(), true));
				}
				channel.close();
			}
		});

	}


	/**
	 * Stops the retransmission timers of in-flight publishes, incoming QoS 2 publishes and
	 * subscriptions, and clears them. Runs once; later calls do nothing.
	 */
	private void destroy() {
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "destroy()");
		if (state == State.DESTROYED) {
			return;
		}
		state = State.DESTROYED;
		channelActive = false;
		webSocketHandShakeComplete = false;
		// https://www.geeksforgeeks.org/how-to-iterate-hashmap-in-java/
		// Publishes sent on this channel have been failed; any left were never sent and are held
		// for the next connection, so they are kept.
		for (Map.Entry<Integer, MqttPendingPublish> pendingPublish : this.client.getPendingPublishes().entrySet()) {
			if (pendingPublish.getValue().isSent()) {
				pendingPublish.getValue().onPubackReceived();
				pendingPublish.getValue().onPubcompReceived();
			}
		}

		for (Map.Entry<Integer, MqttIncomingQos2Publish> incomingQos2Publish : this.client.getQos2PendingIncomingPublishes().entrySet()) {
			incomingQos2Publish.getValue().onPubrelReceived();
		}

		for (Map.Entry<Integer, MqttPendingSubscription> pendingSubscription : this.client.getPendingSubscriptions().entrySet()) {
			pendingSubscription.getValue().onSubackReceived();
		}
		this.client.getPendingSubscriptions().clear();
	}

	/**
	 * Returns the number of incoming messages awaiting manual acknowledgement.
	 *
	 * @return the pending acknowledgement count
	 */
	private int getPendingAckCount() {
		return pendingIncomingAcks.size();
	}


	/**
	 * Records an incoming message awaiting manual acknowledgement.
	 *
	 * @param messageId the packet identifier
	 * @param qos       the QoS of the message
	 */
	private void trackPendingAck(int messageId, MqttQoS qos) {
		pendingIncomingAcks.put(messageId, new PendingIncomingAck(messageId, qos));
		pendingAckOrder.addLast(messageId);
	}

	/**
	 * Records an incoming message awaiting manual acknowledgement, keeping the message so it
	 * can be delivered when earlier messages have been acknowledged.
	 *
	 * @param messageId the packet identifier
	 * @param qos       the QoS of the message
	 * @param message   the incoming publish
	 */
	private void trackPendingAck(int messageId, MqttQoS qos, MqttPublishMessage message) {
		pendingIncomingAcks.put(messageId, new PendingIncomingAck(messageId, qos, message));
		pendingAckOrder.addLast(messageId);
	}

	/**
	 * Completes a manual acknowledgement: sends PUBACK for QoS 1, or PUBREC (and starts its
	 * retransmission) for QoS 2, then delivers the next message waiting for acknowledgement.
	 * Unknown identifiers, and calls made with manual acknowledgements disabled, are ignored.
	 *
	 * @param messageId the packet identifier of the message being acknowledged
	 * @throws MqttException declared for callers; not currently thrown
	 */
	void completeIncomingAck(int messageId) throws MqttException {

		PendingIncomingAck pending;
		PendingIncomingAck nextPending = null;
		synchronized (this) {
			pending = pendingIncomingAcks.remove(messageId);
			pendingAckOrder.remove(messageId);

			if (pending == null || !this.client.getClientConfig().isManualAcks()) {
				return;
			}

			// Remove the completed ID from the head if it happens to be there.
			while (!pendingAckOrder.isEmpty()) {
				Integer nextId = pendingAckOrder.peekFirst();
				if (nextId == null) {
					break;
				}
				PendingIncomingAck candidate = pendingIncomingAcks.get(nextId);
				if (candidate != null) {
					nextPending = candidate;
					break;
				}
				pendingAckOrder.pollFirst(); // discard the stale head, actually advance
			}
		}

		MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader.from(messageId);

		if (pending.getQos() == MqttQoS.AT_LEAST_ONCE) {
			MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
			if (BuildConfig.LOGGING && enableLogging) log("MqttChannelHandler->completeIncomingAck()->Send PUBACK for " + this.client.getClientID());
			this.client.sendAndFlushPacket(new MqttPubAckMessage(fixedHeader, variableHeader));
			if (nextPending != null) {
				invokeHandlersForIncomingPublish(nextPending.getMessage(), MqttQoS.AT_LEAST_ONCE.value());
			}
		} else if (pending.getQos() == MqttQoS.EXACTLY_ONCE) {
			MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_MOST_ONCE, false, 0);
			//See pubrec spec, all bits including QOS set to 0
			io.netty.handler.codec.mqtt.MqttMessage pubrecMessage = new io.netty.handler.codec.mqtt.MqttMessage(fixedHeader, variableHeader);
			MqttPublishMessage message = pending.getMessage();
			MqttIncomingQos2Publish incomingQos2Publish = new MqttIncomingQos2Publish(message, pubrecMessage, false);
			this.client.getQos2PendingIncomingPublishes().put(message.variableHeader().packetId(), incomingQos2Publish);
			incomingQos2Publish.startPubrecRetransmitTimer(this.client.getEventLoop().next(), this.client::sendAndFlushPacket,
					this.client.getClientConfig().getRetransmissionInterval());
			if (BuildConfig.LOGGING && enableLogging) log("MqttChannelHandler->completeIncomingAck()->Send PUBREC for " + this.client.getClientID());
			this.client.sendAndFlushPacket(pubrecMessage);
			// PUBREL/PUBCOMP handled entirely by handlePubrel above once the broker replies
			if (nextPending != null) {
				invokeHandlersForIncomingPublish(nextPending.getMessage(), MqttQoS.EXACTLY_ONCE.value());
			}
		}
	}

	/**
	 * Discards all incoming messages awaiting manual acknowledgement.
	 */
	void clearPendingAcks() {
		pendingIncomingAcks.clear();
	}


	/**
	 * Delivers an incoming message to every matching subscription's callback, or to the
	 * client's default callback if none matched, on the callback executor. One-shot
	 * subscriptions are unsubscribed after delivery. An exception from a callback is logged,
	 * and drops the connection if {@link MqttConnectOptions#setMessageArrivedErrorsTriggerDisconnect(boolean)}
	 * is enabled. The message payload is released afterwards.
	 *
	 * @param message the incoming publish
	 * @param qos     the QoS to report on the delivered message
	 */
	private void invokeHandlersForIncomingPublish(MqttPublishMessage message, int qos) {
		boolean handlerInvoked = false;
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "invokeHandlersForIncomingPublish->Start");
		if (this.client != null) {
			Multimap<String, MqttSubscription> s = this.client.getSubscriptions();
			if (s != null) {
				if (BuildConfig.LOGGING && enableLogging) log(TAG, "invokeHandlersForIncomingPublish->s.size()=" + s.size());
			}
		}

		for (MqttSubscription subscription : ImmutableSet.copyOf(this.client.getSubscriptions().values())) {
			if (subscription.matches(message.variableHeader().topicName())) {
				if (subscription.isOnce() && subscription.isCalled()) {
					continue;
				}
				message.payload().markReaderIndex();
				subscription.setCalled(true);
				try {
					MqttCallback handler = subscription.getHandler();
					if (handler != null) {
						handlerInvoked = true;
						MqttMessage info = new MqttMessage();
						info.setPayload(ByteBufUtil.getBytes(message.payload())); // raw bytes; payloads need not be text
						info.setQos(qos);
						info.isDuplicate(message.fixedHeader().isDup());
						info.isRetained(message.fixedHeader().isRetain());
						info.setURI(client.getServerURI());
						info.setId(message.variableHeader().packetId());
						//handler.onMessage(message.variableHeader().topicName(), message.payload());//Trigger onMessage event listener
						if (BuildConfig.LOGGING && enableLogging) log(TAG, "invokeHandlersForIncomingPublish()->handler.onMessage()");
						String topic = message.variableHeader().topicName();
						this.client.runMessageCallback(() -> {
							try {
								MqttCallbackEvents.messageArrived(handler, topic, info);
							} catch (Exception ex) {
								log(TAG, "invokeHandlersForIncomingPublish()->exception in user callback: " + ex);
								this.client.messageArrivedFailed(ex);
							}
						});

						//handler.onMessage(message.variableHeader().topicName(), info);//Trigger onMessage event listener
						if (subscription.isOnce()) {
							try {
								this.client.unsubscribe(subscription.getTopic(), handler);
							} catch (MqttException me) {
								log(TAG, "invokeHandlersForIncomingPublish()->MQttException in invokeHandlersForIncomingPublish while trying to unsubcribe to " + subscription.getTopic());
							}
						}
					} else {
					}
					message.payload().resetReaderIndex();

				} catch (NullPointerException npe) {
					log(TAG, "invokeHandlersForIncomingPublish()->NPE in invokeHandlersForIncomingPublish");
				}
			}
		}
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "invokeHandlersForIncomingPublish->handlerInvoked=" + handlerInvoked);
		if (!handlerInvoked && client.getDefaultCallbackHandler() != null) {
			if (BuildConfig.LOGGING && enableLogging) log(TAG, "invokeHandlersForIncomingPublish->default handler Invoked");
			MqttMessage info = new MqttMessage();
			info.setPayload(ByteBufUtil.getBytes(message.payload())); // raw bytes; payloads need not be text
			info.setQos(qos);
			info.isDuplicate(message.fixedHeader().isDup());
			info.isRetained(message.fixedHeader().isRetain());
			info.setURI(client.getServerURI());
			// Needed to acknowledge the message with messageArrivedComplete under manual acks,
			// e.g. for a message redelivered from a resumed session with no local subscription.
			info.setId(message.variableHeader().packetId());
			MqttCallback handler = client.getDefaultCallbackHandler();
			String topic = message.variableHeader().topicName();
			this.client.runMessageCallback(() -> {
				try {
					MqttCallbackEvents.messageArrived(handler, topic, info);
				} catch (Exception ex) {
					log(TAG, "invokeHandlersForIncomingPublish()->exception in user callback: " + ex);
					this.client.messageArrivedFailed(ex);
				}
			});
		}
		message.payload().release();
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "invokeHandlersForIncomingPublish->End");
	}

	/**
	 * Notifies the client's default callback, on the callback executor, that an outgoing
	 * publish has been delivered.
	 *
	 * @param token the delivery token of the publish; {@code null} is ignored
	 */
	private void invokeDeliveryCompleteForOutgoingPublish(MqttDeliveryToken token) {
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "invokeDeliveryCompleteForOutgoingPublish->Start");
		if (this.client == null) {
			return;
		}

		if (token == null) {
			if (BuildConfig.LOGGING && enableLogging) log(TAG + "invokeDeliveryCompleteForOutgoingPublish()->token is null, skipping");
			return;
		}
		MqttCallback handler = client.getDefaultCallbackHandler(); // single read, null-checked
		if (handler == null) {
			return;
		}
		this.client.runMessageCallback(() -> {
			try {
				MqttCallbackEvents.deliveryComplete(handler, token);
			} catch (Exception ex) {
				log(TAG + "invokeDeliveryCompleteForOutgoingPublish()->exception in user callback: " + ex);
			}
		});
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "invokeDeliveryCompleteForOutgoingPublish->End");
	}

	/**
	 * Handles CONNACK. On acceptance, completes the connect future, sends any subscribes and
	 * publishes queued while disconnected, and notifies the callback. On refusal, fails the
	 * connect future with a matching reason code, notifies the callback and closes the channel.
	 *
	 * @param channel the channel to the broker
	 * @param message the CONNACK packet
	 */
	private void handleConack(Channel channel, MqttConnAckMessage message) {
		switch (message.variableHeader().connectReturnCode()) {
			case CONNECTION_ACCEPTED:
				if (BuildConfig.LOGGING && enableLogging) log(TAG, "handleCONNACK()->CONNECTION_ACCEPTED=true");
				boolean isSessionPresent = message.variableHeader().isSessionPresent();
				if (!isSessionPresent) {
					// Subscriptions kept from a persistent session the broker no longer has. Done
					// before the connect completes, so a subscribe from its listener is sent.
					this.client.clearSubscriptions();
				}
				// Mark the client connected before completing the future, so a caller woken by
				// connect().waitForCompletion() sees isConnected() == true.
				this.client.isConnected(true);
				this.client.setHasEverConnected(true); // new — records that a real connection was achieved at least once
				this.connectFuture.trySuccess(new MqttConnectResult(true, isSessionPresent, MqttConnectReturnCode.CONNECTION_ACCEPTED, channel.closeFuture()));
				// Subscribes made before the connection was ready. claimSend(): a subscribe made
				// just now, on another thread, may be sending its own SUBSCRIBE at this moment.
				this.client.getPendingSubscriptions().values().forEach(pending -> {
					if (pending.claimSend()) {
						channel.write(pending.getSubscribeMessage());
					}
				});

				if (!isSessionPresent) {
					// The broker has no session, so no PUBREL will come for incoming QoS 2
					// publishes kept from the previous connection.
					this.client.getQos2PendingIncomingPublishes().values().forEach(incoming -> {
						incoming.discard();
						this.client.forgetIncoming(incoming.getIncomingPublish().variableHeader().packetId());
					});
					this.client.getQos2PendingIncomingPublishes().clear();
				}
				// Publishes held while disconnected, and those of the session to send again.
				this.client.sendSessionPublishes(channel);
				this.client.reconnected();
				if (this.client.isReconnect()) {
					this.client.onSuccessfulReconnect();
					this.client.onConnectComplete(true);
				} else {
					this.client.onConnectComplete(false);
				}

				break;

			case CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD:
			case CONNECTION_REFUSED_IDENTIFIER_REJECTED:
			case CONNECTION_REFUSED_NOT_AUTHORIZED:
			case CONNECTION_REFUSED_SERVER_UNAVAILABLE:
			case CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION:
				if (BuildConfig.LOGGING && enableLogging) log(TAG, "handleConack()->connection refused, code=" + message.variableHeader().connectReturnCode());
				this.client.isConnected(false);
				connectFuture.tryFailure(reasonCodeFor(message.variableHeader().connectReturnCode()));
				this.client.onConnectFailed(getReturnMessage(message.variableHeader().connectReturnCode()));
				channel.close();
				// Don't start reconnect logic here
				//
				break;
		}
	}

	/**
	 * Maps a CONNACK refusal code to an {@link MqttException}: an {@link MqttSecurityException}
	 * for bad credentials and for not authorised, as Paho throws.
	 *
	 * @param code the CONNACK return code
	 * @return an exception carrying the matching reason code
	 */
	private MqttException reasonCodeFor(MqttConnectReturnCode code) {
		switch (code) {
			case CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD:
				return new MqttSecurityException(MqttException.REASON_CODE_FAILED_AUTHENTICATION);
			case CONNECTION_REFUSED_NOT_AUTHORIZED:
				return new MqttSecurityException(MqttException.REASON_CODE_NOT_AUTHORIZED);
			case CONNECTION_REFUSED_IDENTIFIER_REJECTED:
				return new MqttException(MqttException.REASON_CODE_INVALID_CLIENT_ID);
			case CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION:
				return new MqttException(MqttException.REASON_CODE_INVALID_PROTOCOL_VERSION);
			case CONNECTION_REFUSED_SERVER_UNAVAILABLE:
			default:
				return new MqttException(MqttException.REASON_CODE_SERVER_CONNECT_ERROR);
		}
	}

	/**
	 * Returns the name of a CONNACK return code.
	 *
	 * @param returnCode the return code from the broker
	 * @return the constant name of the code, or {@code "Unknown"} if it is not recognised
	 */
	public String getReturnMessage(MqttConnectReturnCode returnCode) {
		String retval = "Unknown";
		switch (returnCode) {
			case CONNECTION_ACCEPTED:
				retval = "CONNECTION_ACCEPTED";
				break;
			case CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION:
				retval = "CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION";
				break;
			case CONNECTION_REFUSED_IDENTIFIER_REJECTED:
				retval = "CONNECTION_REFUSED_IDENTIFIER_REJECTED";
				break;
			case CONNECTION_REFUSED_SERVER_UNAVAILABLE:
				retval = "CONNECTION_REFUSED_SERVER_UNAVAILABLE";
				break;
			case CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD:
				retval = "CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD";
				break;
			case CONNECTION_REFUSED_NOT_AUTHORIZED:
				retval = "CONNECTION_REFUSED_NOT_AUTHORIZED";
				break;
			case CONNECTION_REFUSED_UNSPECIFIED_ERROR:
				retval = "CONNECTION_REFUSED_UNSPECIFIED_ERROR";
				break;
			case CONNECTION_REFUSED_MALFORMED_PACKET:
				retval = "CONNECTION_REFUSED_MALFORMED_PACKET";
				break;
			case CONNECTION_REFUSED_PROTOCOL_ERROR:
				retval = "CONNECTION_REFUSED_PROTOCOL_ERROR";
				break;
			case CONNECTION_REFUSED_IMPLEMENTATION_SPECIFIC:
				retval = "CONNECTION_REFUSED_IMPLEMENTATION_SPECIFIC";
				break;
			case CONNECTION_REFUSED_UNSUPPORTED_PROTOCOL_VERSION:
				retval = "CONNECTION_REFUSED_UNSUPPORTED_PROTOCOL_VERSION";
				break;
			case CONNECTION_REFUSED_CLIENT_IDENTIFIER_NOT_VALID:
				retval = "CONNECTION_REFUSED_CLIENT_IDENTIFIER_NOT_VALID";
				break;
			case CONNECTION_REFUSED_BAD_USERNAME_OR_PASSWORD:
				retval = "CONNECTION_REFUSED_BAD_USERNAME_OR_PASSWORD";
				break;
			case CONNECTION_REFUSED_NOT_AUTHORIZED_5:
				retval = "CONNECTION_REFUSED_NOT_AUTHORIZED_5";
				break;
			case CONNECTION_REFUSED_SERVER_UNAVAILABLE_5:
				retval = "CONNECTION_REFUSED_SERVER_UNAVAILABLE_5";
				break;
			case CONNECTION_REFUSED_SERVER_BUSY:
				retval = "CONNECTION_REFUSED_SERVER_BUSY";
				break;
			case CONNECTION_REFUSED_BANNED:
				retval = "CONNECTION_REFUSED_BANNED";
				break;
			case CONNECTION_REFUSED_BAD_AUTHENTICATION_METHOD:
				retval = "CONNECTION_REFUSED_BAD_AUTHENTICATION_METHOD";
				break;
			case CONNECTION_REFUSED_TOPIC_NAME_INVALID:
				retval = "CONNECTION_REFUSED_TOPIC_NAME_INVALID";
				break;
			case CONNECTION_REFUSED_PACKET_TOO_LARGE:
				retval = "CONNECTION_REFUSED_PACKET_TOO_LARGE";
				break;
			case CONNECTION_REFUSED_QUOTA_EXCEEDED:
				retval = "CONNECTION_REFUSED_QUOTA_EXCEEDED";
				break;
			case CONNECTION_REFUSED_PAYLOAD_FORMAT_INVALID:
				retval = "CONNECTION_REFUSED_PAYLOAD_FORMAT_INVALID";
				break;
			case CONNECTION_REFUSED_RETAIN_NOT_SUPPORTED:
				retval = "CONNECTION_REFUSED_RETAIN_NOT_SUPPORTED";
				break;
			case CONNECTION_REFUSED_QOS_NOT_SUPPORTED:
				retval = "CONNECTION_REFUSED_QOS_NOT_SUPPORTED";
				break;
			case CONNECTION_REFUSED_USE_ANOTHER_SERVER:
				retval = "CONNECTION_REFUSED_USE_ANOTHER_SERVER";
				break;
			case CONNECTION_REFUSED_SERVER_MOVED:
				retval = "CONNECTION_REFUSED_SERVER_MOVED";
				break;
			case CONNECTION_REFUSED_CONNECTION_RATE_EXCEEDED:
				retval = "CONNECTION_REFUSED_CONNECTION_RATE_EXCEEDED";
				break;


		}//switch
		return retval;
	}

	/**
	 * Handles DISCONNECT from the broker by ignoring any further packets on this channel.
	 *
	 * @param channel the channel to the broker
	 */
	private void handleDisconnect(Channel channel) {
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "handleDisconnect()->channelInactive!!! ");
		channelActive = false;
	}

	/**
	 * Handles SUBACK: stops retransmission, registers the pending callbacks for each granted
	 * topic, records the granted QoS on the token and completes it. Rejected topics get no
	 * callbacks.
	 *
	 * @param message the SUBACK packet
	 */
	private void handleSubAck(MqttSubAckMessage message) {
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "handleSubAck->Start");
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "handleSubAck->msgid=" + message.variableHeader().messageId());
		// Not grantedQoSLevels() or reasonCodes(): Netty decodes a granted QoS MQTT does not have
		// (3 to 0x7f) as a null reason code, which both throw on, and the subscribe would never
		// complete. Such a value is taken as QoS 0, as an invalid QoS is everywhere else.
		List<Integer> returnCodes = new java.util.ArrayList<>();
		for (MqttReasonCodes.SubAck code : message.payload().typedReasonCodes()) {
			returnCodes.add(code == null ? MqttQoS.AT_MOST_ONCE.value() : code.byteValue() & 0xff);
		}
		MqttPendingSubscription pendingSubscription = this.client.getPendingSubscriptions().remove(message.variableHeader().messageId());
		if (pendingSubscription == null) {
			return;
		}
		pendingSubscription.onSubackReceived();

		List<String> topics = pendingSubscription.getTopics();
		boolean sizesMatch = true;

		if (topics.size() != returnCodes.size()) {
			if (BuildConfig.LOGGING && enableLogging) log(TAG, "handleSubAck->mismatched topic/returnCode count, expected="
					+ topics.size() + " got=" + returnCodes.size());
			sizesMatch = false;
		}

		for (int i = 0; i < topics.size(); i++) {
			String topic = topics.get(i);
			int returnCode = (i < returnCodes.size()) ? returnCodes.get(i) : MqttQoS.FAILURE.value();
			boolean granted = returnCode < MqttQoS.FAILURE.value();   // 0x80 and up refuse it

			this.client.getPendingSubscribeTopics().remove(topic);

			if (granted) {
				// A granted QoS MQTT does not have (3 to 0x7f) is taken as 0.
				MqttQoS grantedQos = MqttQoS.valueOf(MqttMessage.validQos(returnCode));
				if (BuildConfig.LOGGING && enableLogging) log(TAG, "handleSubAck->subscribed to " + topic + " at QOS=" + grantedQos.value());
				// The broker keeps one subscription per topic filter, so a SUBSCRIBE for a topic
				// that is already subscribed replaces it at the new QoS. Mirror that locally:
				// otherwise an existing entry at the old QoS and the new entry would both match,
				// delivering each message twice to the same handler.
				replaceExistingSubscriptions(topic, grantedQos, pendingSubscription.getHandlersFor(topic));
				for (MqttPendingSubscription.MqttPendingSubscribeHandler handler : pendingSubscription.getHandlersFor(topic)) {
					MqttSubscription subscription = new MqttSubscription(topic, handler.getHandler(), grantedQos, handler.isOnce());
					this.client.getSubscriptions().put(topic, subscription);
					this.client.getHandlerToSubscription().put(handler.getHandler(), subscription);
				}
				this.client.getServerSubscriptions().put(topic, grantedQos.value());
			} else {
				if (BuildConfig.LOGGING && enableLogging) log(TAG, "handleSubAck->broker rejected subscription for topic=" + topic);
				// not added to subscriptions/serverSubscriptions — no handler fires for this topic
			}
		}

		// in handleSubAck, before trySuccess:
		int[] grantedArray = returnCodes.stream().mapToInt(Integer::intValue).toArray();
		pendingSubscription.getToken().setGrantedQos(grantedArray);
		pendingSubscription.getToken().trySuccess(null);
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "handleSubAck->End");
	}

	/**
	 * Handles UNSUBACK: stops retransmission, removes the topics from the broker-side
	 * subscription set and completes the token. Duplicate UNSUBACKs are ignored.
	 *
	 * @param message the UNSUBACK packet
	 */
	/**
	 * Updates the local subscriptions for a topic the broker has just granted again, possibly
	 * at a different QoS. Entries whose handler is about to be registered for the topic are
	 * removed, as the new entry replaces them; entries for other handlers are kept, with their
	 * QoS updated to the granted one, which now applies to the whole topic.
	 *
	 * @param topic       the granted topic filter
	 * @param grantedQos  the QoS the broker granted
	 * @param newHandlers the handlers about to be registered for the topic
	 */
	private void replaceExistingSubscriptions(String topic, MqttQoS grantedQos,
											  List<MqttPendingSubscription.MqttPendingSubscribeHandler> newHandlers) {
		Multimap<String, MqttSubscription> subscriptions = this.client.getSubscriptions();
		Multimap<MqttCallback, MqttSubscription> byHandler = this.client.getHandlerToSubscription();
		for (MqttSubscription existing : ImmutableSet.copyOf(subscriptions.get(topic))) {
			boolean replaced = false;
			for (MqttPendingSubscription.MqttPendingSubscribeHandler h : newHandlers) {
				if (java.util.Objects.equals(h.getHandler(), existing.getHandler())) {
					replaced = true;
					break;
				}
			}
			if (!replaced && existing.getQOS() == grantedQos) {
				continue;   // another handler, already at the granted QoS
			}
			subscriptions.remove(topic, existing);
			byHandler.remove(existing.getHandler(), existing);
			if (!replaced) {
				MqttSubscription updated = new MqttSubscription(topic, existing.getHandler(), grantedQos, existing.isOnce());
				updated.setCalled(existing.isCalled());
				subscriptions.put(topic, updated);
				byHandler.put(existing.getHandler(), updated);
			}
		}
	}

	private void handleUnsuback(MqttUnsubAckMessage message) {
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "handleUnsuback()->Start");
		int messageId = message.variableHeader().messageId();

		// remove() returns the entry atomically, so a duplicate UNSUBACK finds null and exits
		MqttPendingUnsubscription unsubscription = this.client.getPendingServerUnsubscribes().remove(messageId);
		if (unsubscription == null) {
			return;
		}
		unsubscription.onUnsubackReceived();

		List<String> orphaned = this.client.onTopicsUnsubscribed(unsubscription.getTopics());
		if (!orphaned.isEmpty()) {
			if (BuildConfig.LOGGING && enableLogging) log(TAG, "handleUnsuback->handlers were added during unsubscribe for " + orphaned
					+ "; broker has dropped these topics, they need re-subscribing");
		}

		// complete the token last, after state is consistent
		unsubscription.getToken().trySuccess(null);
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "handleUnsuback()->End");
	}


	/**
	 * Handles an incoming PUBLISH according to its QoS. QoS 0 messages are delivered
	 * immediately. QoS 1 messages are delivered and acknowledged with PUBACK, unless manual
	 * acknowledgements are enabled. QoS 2 messages are answered with PUBREC and delivered when
	 * PUBREL arrives, or with manual acknowledgements, delivered first and answered with PUBREC
	 * once the application acknowledges them. With manual acknowledgements, messages are
	 * delivered one at a time.
	 *
	 * @param channel the channel to the broker
	 * @param message the PUBLISH packet
	 */
	private void handleIncomingPublish(Channel channel, MqttPublishMessage message) {
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "handleIncomingPublish()->Start");
		if (message == null) {
			return;
		}
		boolean deliverNow = false;
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "handleIncomingPublish()->, q=" + message.fixedHeader().qosLevel().value() + ",msgid=" + message.variableHeader().packetId());
		switch (message.fixedHeader().qosLevel()) {
			case AT_MOST_ONCE:
				if (BuildConfig.LOGGING && enableLogging) log(TAG, "handleIncomingPublish()->QOS0->PUB->PING");
				if (BuildConfig.LOGGING && enableLogging) log(TAG, "handleIncomingPublish()->Send QOS0 PINGREQ for " + this.client.getClientID());
				invokeHandlersForIncomingPublish(message, MqttQoS.AT_MOST_ONCE.value());
				//No control packet sent back to broker, so send a PingReq - //https://github.com/mqttjs/MQTT.js/issues/357
				if (message.variableHeader().packetId() != -1) {
					MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PINGREQ, false, MqttQoS.AT_MOST_ONCE, false, 0);
					if (BuildConfig.LOGGING && enableLogging) log("MqttChannelHandler->handleIncomingPublish()->Send QOS0 PINGREQ for " + this.client.getClientID());
					ChannelFuture channelFuture = channel.writeAndFlush(new io.netty.handler.codec.mqtt.MqttMessage(fixedHeader));
				}
				break;
			case AT_LEAST_ONCE:
				if (BuildConfig.LOGGING && enableLogging) log(TAG, "handleIncomingPublish()->QOS1->PUB->Send PUBACK");
				synchronized (this) {
					deliverNow = (getPendingAckCount() == 0);
					if (message.variableHeader().packetId() != -1 && this.client.getClientConfig().isManualAcks()) {
						trackPendingAck(message.variableHeader().packetId(), MqttQoS.AT_LEAST_ONCE, message);
					}
				}
				if (deliverNow) {
					invokeHandlersForIncomingPublish(message, MqttQoS.AT_LEAST_ONCE.value());
				}
				if (message.variableHeader().packetId() != -1 && !this.client.getClientConfig().isManualAcks()) {
					MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
					MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader.from(message.variableHeader().packetId());
					if (BuildConfig.LOGGING && enableLogging) log(TAG, "handleIncomingPublish()->Sending QOS1 PubACK for " + this.client.getClientID());
					channel.writeAndFlush(new MqttPubAckMessage(fixedHeader, variableHeader));
					if (BuildConfig.LOGGING && enableLogging) log(TAG, "handleIncomingPublish()->Sent QOS1 PubACK for " + this.client.getClientID());
					if (BuildConfig.LOGGING && enableLogging) log(TAG, "handleIncomingPublish()->QOS1->PUB->PUBACK. Done");
				}
				break;

			case EXACTLY_ONCE:
				if (BuildConfig.LOGGING && enableLogging) log(TAG, "handleIncomingPublish()->QOS2->PUB->PubREC->PubREL->PubCOMP");
				if (message.variableHeader().packetId() != -1) {
					if (!this.client.getClientConfig().isManualAcks()) {
						// https://public.dhe.ibm.com/software/dw/webservices/ws-mqtt/mqtt-v3r1.html#pubrec
						MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_MOST_ONCE, false, 0);
						//See pubrec spec, all bits including QOS set to 0
						MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader.from(message.variableHeader().packetId());
						io.netty.handler.codec.mqtt.MqttMessage pubrecMessage = new io.netty.handler.codec.mqtt.MqttMessage(fixedHeader, variableHeader);

						// Inbound PUBLISHes arrive holding two references (the decoder's and the one
						// MqttPingHandler adds): one is released when channelRead0 returns, and the
						// tracker keeps the other until the message is delivered at PUBREL.
						MqttIncomingQos2Publish incomingQos2Publish = new MqttIncomingQos2Publish(message, pubrecMessage, true);
						MqttIncomingQos2Publish previous = this.client.getQos2PendingIncomingPublishes().put(message.variableHeader().packetId(), incomingQos2Publish);
						this.client.storeIncoming(message);
						if (previous != null) {
							previous.discard();   // a resent PUBLISH replaces it
						}
						incomingQos2Publish.startPubrecRetransmitTimer(this.client.getEventLoop().next(), this.client::sendAndFlushPacket,
					this.client.getClientConfig().getRetransmissionInterval());
						if (BuildConfig.LOGGING && enableLogging) log(TAG, "handleIncomingPublish()->Sending QOS2 PubREC for " + this.client.getClientID());
						ChannelFuture channelFuture = channel.writeAndFlush(pubrecMessage);
						if (BuildConfig.LOGGING && enableLogging) log(TAG, "handleIncomingPublish()->Sent QOS2 PubREC for " + this.client.getClientID() + " Waiting for PubREL");
					} else {
						// The second reference (see above) is released when the message is delivered.
						deliverNow = false;
						synchronized (this) {
							deliverNow = (getPendingAckCount() == 0);
							trackPendingAck(message.variableHeader().packetId(), MqttQoS.EXACTLY_ONCE, message);
						}
						if (deliverNow) {
							// invoke the handler so the user can ACK — that ACK (messageArrivedComplete)
							// triggers the PUBREC via completeIncomingAck
							this.invokeHandlersForIncomingPublish(message, MqttQoS.EXACTLY_ONCE.value());
						}

					}
				}
				break;
		}
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "handleIncomingPublish->End");
	}


	/**
	 * Answers a PINGREQ from the broker with PINGRESP, if the channel is open.
	 *
	 * @param channel the channel to the broker
	 */
	private void handlePingReq(Channel channel) {
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "handlePingReq");
		if (!channel.isOpen()) {
			if (BuildConfig.LOGGING && enableLogging) log(TAG, "handlePingReq->returning");
			return;
		}
		MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0);
		channel.writeAndFlush(new io.netty.handler.codec.mqtt.MqttMessage(fixedHeader));
	}

	/**
	 * Handles PINGRESP. Keep-alive timing is managed by {@link MqttPingHandler}, so this does nothing.
	 *
	 * @param channel the channel to the broker
	 */
	private void handlePingResp(Channel channel) {
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "handlePingRESP()->Start");
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "handlePingRESP()->End");
	}

	/**
	 * Handles PUBACK for an outgoing QoS 1 publish: stops retransmission, releases the
	 * payload, completes the delivery token and notifies the callback.
	 *
	 * @param message the PUBACK packet
	 */
	private void handlePuback(MqttPubAckMessage message) {
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "handlePubACK()->Start");
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "handlePubACK()->QOS1 PUB sent, PUBACK received.");
		MqttPendingPublish pendingPublish = this.client.getPendingPublishes().get(message.variableHeader().messageId());
		if (pendingPublish == null) {
			if (BuildConfig.LOGGING && enableLogging) log(TAG, "handlePubACK()->PendingPublish is Null. End");
			return;
		}
		MqttDeliveryToken token = pendingPublish.getToken();

		pendingPublish.onPubackReceived();
		this.client.getPendingPublishes().remove(message.variableHeader().messageId());
		this.client.forgetOutgoing(message.variableHeader().messageId());
		pendingPublish.getPayload().release();
		pendingPublish.getToken().markComplete();
		invokeDeliveryCompleteForOutgoingPublish(token);

		if (BuildConfig.LOGGING && enableLogging) log(TAG, "handlePubACK()->QOS1 PUB sent, PUBACK received. MQTT Sequence complete.");
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "handlePubACK()->End");
	}

	/**
	 * Handles PUBREC for an outgoing QoS 2 publish: stops PUBLISH retransmission, sends PUBREL
	 * and retransmits it until PUBCOMP arrives.
	 *
	 * @param channel the channel to the broker
	 * @param message the PUBREC packet
	 */
	private synchronized void handlePubrec(Channel channel, io.netty.handler.codec.mqtt.MqttMessage message) {
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "handlePubREC()->Start");
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "handlePubREC()->QOS2 PUB sent, PUBREC received. Send PUBREL");
		MqttPendingPublish pendingPublish = this.client.getPendingPublishes().get(((MqttMessageIdVariableHeader) message.variableHeader()).messageId());
		if (pendingPublish != null) {
			pendingPublish.onPubackReceived();

			// https://public.dhe.ibm.com/software/dw/webservices/ws-mqtt/mqtt-v3r1.html#pubrel
			MqttFixedHeader fixedHeader = MqttPubRelFixedHeader.getFixedHeader();
			MqttMessageIdVariableHeader variableHeader = (MqttMessageIdVariableHeader) message.variableHeader();
			io.netty.handler.codec.mqtt.MqttMessage pubrelMessage = new io.netty.handler.codec.mqtt.MqttMessage(fixedHeader, variableHeader);
			if (BuildConfig.LOGGING && enableLogging) log(TAG, "handlePubREC()->Sending QOS2 PUBREL for " + this.client.getClientID());
			channel.writeAndFlush(pubrelMessage);
			if (BuildConfig.LOGGING && enableLogging) log(TAG, "handlePubREC()->Sent QOS2 PUBREL for " + this.client.getClientID());

			pendingPublish.setPubrelMessage(pubrelMessage);
			this.client.storeOutgoing(pendingPublish);
			pendingPublish.startPubrelRetransmissionTimer(this.client.getEventLoop().next(), this.client::sendAndFlushPacket,
					this.client.getClientConfig().getRetransmissionInterval());
			if (BuildConfig.LOGGING && enableLogging) log(TAG, "handlePubREC()->QOS2 PUB sent, PUBREC received. Sent PUBREL. Waiting for PUBCOMP");
		}
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "handlePubREC()->End");
	}

	/**
	 * Handles PUBREL for an incoming QoS 2 publish: stops PUBREC retransmission, delivers the
	 * message (unless it was already delivered under manual acknowledgements) and answers with PUBCOMP.
	 *
	 * @param channel the channel to the broker
	 * @param message the PUBREL packet
	 */
	private synchronized void handlePubrel(Channel channel, io.netty.handler.codec.mqtt.MqttMessage message) {
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "handlePubREL()->Start");
		if (this.client.getQos2PendingIncomingPublishes().containsKey(((MqttMessageIdVariableHeader) message.variableHeader()).messageId())) {
			MqttIncomingQos2Publish incomingQos2Publish = this.client.getQos2PendingIncomingPublishes().get(((MqttMessageIdVariableHeader) message.variableHeader()).messageId());
			if (incomingQos2Publish != null) {
				incomingQos2Publish.onPubrelReceived();
				this.client.getQos2PendingIncomingPublishes().remove(incomingQos2Publish.getIncomingPublish().variableHeader().packetId());
				this.client.forgetIncoming(incomingQos2Publish.getIncomingPublish().variableHeader().packetId());
				// Without manual acks, deliver now with the payload reference the tracker held;
				// with them, the user has already had the message, before PUBREC was sent.
				if (incomingQos2Publish.takePayload()) {
					this.invokeHandlersForIncomingPublish(incomingQos2Publish.getIncomingPublish(), MqttQoS.EXACTLY_ONCE.value());
				}
			}
		}
		// in manual mode: message was already delivered (or queued for delivery via the
		// pending-ack chain) when PUBLISH arrived — nothing to redeliver here. Either way,
		// PUBREL must be answered with PUBCOMP.


		// https://public.dhe.ibm.com/software/dw/webservices/ws-mqtt/mqtt-v3r1.html#pubcomp
		MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBCOMP, false, MqttQoS.AT_MOST_ONCE, false, 0);
		MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader.from(((MqttMessageIdVariableHeader) message.variableHeader()).messageId());
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "handlePubREL()->Sending QOS2 PubCOMP for " + this.client.getClientID());
		channel.writeAndFlush(new io.netty.handler.codec.mqtt.MqttMessage(fixedHeader, variableHeader));
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "handlePubREL()->Sent QOS2 PubIN->PubCOMP for " + this.client.getClientID() + ". MQTT Sequence complete.");
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "handlePubREL()->End");
	}

	/**
	 * Handles PUBCOMP for an outgoing QoS 2 publish: completes the delivery token, releases the
	 * payload, stops PUBREL retransmission and notifies the callback.
	 *
	 * @param channel the channel to the broker
	 * @param message the PUBCOMP packet
	 */
	private void handlePubcomp(Channel channel, io.netty.handler.codec.mqtt.MqttMessage message) {
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "handlePubCOMP()->Start");
		MqttMessageIdVariableHeader variableHeader = (MqttMessageIdVariableHeader) message.variableHeader();
		MqttPendingPublish pendingPublish = this.client.getPendingPublishes().get(variableHeader.messageId());
		if (pendingPublish != null) {
			MqttDeliveryToken token = pendingPublish.getToken();
			// Remove before completing the token, so a caller woken by the token already has
			// the in-flight slot back, as with PUBACK.
			this.client.getPendingPublishes().remove(variableHeader.messageId());
			this.client.forgetOutgoing(variableHeader.messageId());
			pendingPublish.getToken().markComplete();
			pendingPublish.getPayload().release();
			pendingPublish.onPubcompReceived(); //stops the retransmission timer.
			invokeDeliveryCompleteForOutgoingPublish(token);
		}
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "handlePubCOMP()->QOS2 PUB sent, PUBREC received. Sent PUBREL. PUBCOMP received. MQTT Sequence complete.");
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "handlePubCOMP()->End");
	}

}//class
