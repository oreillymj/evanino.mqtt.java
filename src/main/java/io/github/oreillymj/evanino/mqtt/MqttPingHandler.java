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

import io.netty.channel.*;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.ScheduledFuture;

import java.util.concurrent.TimeUnit;

/**
 * Implements the MQTT keep-alive. When the pipeline's idle-state handler reports the channel
 * as writer-idle, a PINGREQ is sent and a response timer is started; if no PINGRESP arrives
 * within the keep-alive interval, a DISCONNECT is sent and the channel is closed. PINGREQs
 * from the broker are answered with PINGRESP. Ping packets are consumed here; all other
 * inbound messages are passed on.
 */
final class MqttPingHandler extends ChannelInboundHandlerAdapter {

	private final String TAG = "MqttPingHandler->";
	private final boolean enableLogging = false;

	private SimpleLogger logger = new SimpleLogger();


	private int keepaliveSeconds = 60;

	private int keepaliveSecondsRespTimeOut = 90;

	private long errorcount = 0;

	private long sendreqmillis = 0;


	private ScheduledFuture<?> pingRespTimeout;

	private ScheduledFuture<?> pingReqSchedFuture;

	private ChannelFuture pingReqFuture = null;

	private final MqttAsyncClient client;

	private volatile boolean channelactive = false;

	private KeepAliveTracker keepAliveTracker;

	private final boolean testWatchDog = false;
	private boolean loggingEnabled = false;

	/**
	 * Lifecycle of the handler.
	 */
	private enum State {
		/** Not yet initialized. */
		NEW,
		/** Initialized and handling pings. */
		INITIALIZED,
		/** Destroyed; will not be re-initialized. */
		DESTROYED
	}

	private State state = State.NEW;

	/**
	 * Creates a ping handler for a client connection.
	 *
	 * @param client           the client, whose configuration controls ping logging
	 * @param keepaliveSeconds the MQTT keep-alive interval in seconds; 0 or less disables pings
	 */
	MqttPingHandler(MqttAsyncClient client, int keepaliveSeconds) {
		this.client = client;
		this.keepaliveSeconds = keepaliveSeconds;
		this.loggingEnabled = client.getClientConfig().getPingLogging();
	}

	/**
	 * Logs a message if logging is enabled for this handler.
	 *
	 * @param data the message to log
	 */
	private void log(String data) {
		if (enableLogging) {
			logger.log(data);
		}
	}

	/**
	 * Logs a message with the given tag if logging is enabled for this handler.
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
	 * Initializes the handler if the channel is already active and registered; otherwise it
	 * is initialized from {@link #channelActive(ChannelHandlerContext)}.
	 *
	 * @param ctx the channel handler context
	 * @throws Exception if initialization fails
	 */
	@Override
	public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
		if (ctx.channel().isActive() && ctx.channel().isRegistered()) {
			// channelActvie() event has been fired already, which means this.channelActive() will
			// not be invoked. We have to initialize here instead.
			initialize();
		} else {
			// channelActive() event has not been fired yet.  this.channelActive() will be invoked
			// and initialization will occur there.
		}
	}

	/**
	 * Destroys the handler, cancelling any pending ping response timer.
	 *
	 * @param ctx the channel handler context
	 * @throws Exception if the superclass fails
	 */
	@Override
	public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
		destroy();
	}

	/**
	 * Initializes the handler early if the channel is already active, then propagates the event.
	 *
	 * @param ctx the channel handler context
	 * @throws Exception if the superclass fails
	 */
	@Override
	public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
		// Initialize early if channel is active already.
		if (ctx.channel().isActive()) {
			initialize();
		}
		super.channelRegistered(ctx);
	}


	/**
	 * Initializes the handler, then propagates the event.
	 *
	 * @param ctx the channel handler context
	 * @throws Exception if the superclass fails
	 */
	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		// This method will be invoked only if this handler was added
		// before channelActive() event is fired.  If a user adds this handler
		// after the channelActive() event, initialize() will be called by handlerAdded().
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "channelActive");
		initialize();
		super.channelActive(ctx);
	}


	/**
	 * Destroys the handler, then propagates the event.
	 *
	 * @param ctx the channel handler context
	 * @throws Exception if the superclass fails
	 */
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "channelInactive!!! ");
		destroy();
		super.channelInactive(ctx);
	}

	/**
	 * Counts and logs the exception, closes the channel if it is no longer active and
	 * writable, then propagates the exception.
	 *
	 * @param channelhandlercontext the channel handler context
	 * @param throwable             the exception
	 * @throws Exception if the superclass fails
	 */
	public void exceptionCaught(ChannelHandlerContext channelhandlercontext, Throwable throwable) throws Exception {
		//  https://stackoverflow.com/questions/30994095/how-to-catch-all-exception-in-netty
		//  https://www.baeldung.com/netty-exception-handling#:~:text=Handling%20Exceptions%20in%20the%20Channel,deprecated%20in%20the%20ChannelHandler%20interface.

		// https://github.com/netty/netty/issues/12835
		errorcount++;
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "Exception caught!!! Error count=" + errorcount + " info " + throwable.toString());

		// https://www.baeldung.com/netty-exception-handling
		if (!(channelhandlercontext.channel().isActive() && channelhandlercontext.channel().isWritable())) {
			channelhandlercontext.close();
		}
		super.exceptionCaught(channelhandlercontext, throwable);
	}


	/**
	 * Initializes the handler the first time it is called; later calls, and calls after
	 * {@link #destroy()}, do nothing.
	 */
	private void initialize() {
		switch (state) {
			case INITIALIZED:
			case DESTROYED:
				return;
		}

		state = State.INITIALIZED;
		keepAliveTracker = client.getKeepAliveTracker();
		channelactive = true;

	}

	/**
	 * Marks the handler destroyed and cancels any pending ping response timer.
	 */
	private void destroy() {
		state = State.DESTROYED;
		channelactive = false;
		this.pingReqFuture = null;
		if (this.pingRespTimeout != null) {
			this.pingRespTimeout.cancel(false); //Don't allow interrupt
		}
		this.pingRespTimeout = null;
	}

	/**
	 * Answers PINGREQ with PINGRESP and handles PINGRESP; both are consumed. Other MQTT
	 * messages are retained and passed on, and non-MQTT messages are passed on unchanged.
	 *
	 * @param ctx the channel handler context
	 * @param msg the inbound message; {@code null} is dropped
	 * @throws Exception if the next handler fails
	 */
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		if (msg == null) {
			return;
		}

		if (!(msg instanceof MqttMessage)) {
			ctx.fireChannelRead(msg);
			return;
		}

		MqttMessage message = (MqttMessage) msg;
		if (message.decoderResult().isFailure() || message.fixedHeader() == null) {
			// Not decoded (its fixed header may be missing): MqttChannelHandler closes the connection.
			ctx.fireChannelRead(ReferenceCountUtil.retain(msg));
			return;
		}
		if (message.fixedHeader().messageType() == MqttMessageType.PINGREQ) {
			this.handlePingReq(ctx.channel());
		} else if (message.fixedHeader().messageType() == MqttMessageType.PINGRESP) {
			this.handlePingResp();
		} else {
			ctx.fireChannelRead(ReferenceCountUtil.retain(msg));
		}
	}


	/**
	 * Propagates the event, then sends a PINGREQ if it is a writer-idle event, keep-alive is
	 * enabled and the channel is active.
	 *
	 * @param ctx the channel handler context
	 * @param evt the user event
	 * @throws Exception if the superclass fails
	 */
	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
		super.userEventTriggered(ctx, evt);

		if (evt instanceof IdleStateEvent) {
			IdleStateEvent event = (IdleStateEvent) evt;

			switch (event.state()) {
				case READER_IDLE:
					break;
				case WRITER_IDLE:
					if ((loggingEnabled) && (keepAliveTracker != null)) {
						if (BuildConfig.LOGGING && enableLogging) log(TAG, "userEventTriggered()->WRITER_IDLE->previous keep alive time=" + keepAliveTracker.getLastPingDateTime());
						if (BuildConfig.LOGGING && enableLogging) log(TAG, "userEventTriggered()->WRITER_IDLE->last keep alive time=" + keepAliveTracker.getPingDateTime());
						if (BuildConfig.LOGGING && enableLogging) log(TAG, "userEventTriggered()->WRITER_IDLE->last keep alive delta secs=" + keepAliveTracker.getPingDeltaSecs());
					}
					if ((ctx != null) && (ctx.channel() != null)) {
						if (BuildConfig.LOGGING && enableLogging) log(TAG, "userEventTriggered()->WRITER_IDLE->ctx.channel().isOpen()" + ctx.channel().isOpen());
						if (BuildConfig.LOGGING && enableLogging) log(TAG, "userEventTriggered()->WRITER_IDLE->ctx.channel().isActive()" + ctx.channel().isActive());
						if ((keepaliveSeconds > 0) && ctx.channel().isActive()) {
							this.sendPingReq(ctx.channel());
						}
					}
					break;
				case ALL_IDLE:

					break;
			}
		}
	}

	/**
	 * Sends a PINGREQ if none is outstanding or the last one was sent more than a keep-alive
	 * interval ago, records the ping time in
	 * {@link KeepAliveTracker}, and starts the response timer if it is not already running.
	 * When the timer fires before a PINGRESP arrives, a DISCONNECT is sent and the channel
	 * is closed.
	 *
	 * @param channel the channel to the broker
	 */
	private void sendPingReq(Channel channel) {
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "sendPingReq()");
		if (testWatchDog) {
			return;
		}
		if ((!channelactive) || (!channel.isActive())) {
			if (BuildConfig.LOGGING && enableLogging) log(TAG, "sendPingReq()->returning");
			return;
		}
		long pingdelta = System.currentTimeMillis() - keepAliveTracker.getPingEpoch();
		boolean keepaliveexceeded = (pingdelta > this.keepaliveSeconds * 1000L); // pingdelta is in millis
		if ((this.pingReqFuture == null) || (keepaliveexceeded)) {
			MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PINGREQ, false, MqttQoS.AT_MOST_ONCE, false, 0);
			pingReqFuture = channel.writeAndFlush(new MqttMessage(fixedHeader));
			if (BuildConfig.LOGGING && enableLogging) log(TAG, "sendPingReq()->Sent MQTT PingReq");
			keepAliveTracker.setPingEpoch(System.currentTimeMillis());

			if (this.pingReqSchedFuture != null) {
				this.pingReqSchedFuture.cancel(false);
				this.pingReqSchedFuture = channel.eventLoop().schedule(() -> {
					sendPingReq(channel);
				}, this.keepaliveSeconds + 1, TimeUnit.SECONDS);
			}
		}

		if (this.pingRespTimeout == null) {
			if (BuildConfig.LOGGING && enableLogging) log(TAG, "sendPingReq()->create pingRespTimeout");
			this.pingRespTimeout = channel.eventLoop().schedule(() -> {
				if (BuildConfig.LOGGING && enableLogging) log(TAG, "sendPingReq()->pingRespTimeout activated " + System.currentTimeMillis());
				MqttFixedHeader fixedHeader2 = new MqttFixedHeader(MqttMessageType.DISCONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0);
				if (channelactive) {
					ChannelFuture channelFuture2 = channel.writeAndFlush(new MqttMessage(fixedHeader2)).addListener(ChannelFutureListener.CLOSE);
					//TODO: what do when the connection is closed ?
					if (channelFuture2 != null) {
						if (channelFuture2.cause() != null) {
							if (BuildConfig.LOGGING && enableLogging) log(TAG, "sendPingReq()->failure.cause->" + channelFuture2.cause());
							throw new ChannelClosedException(logger.getDate() + TAG + "sendPingReq()->pingRespTimeout activated->Channel is closed!");
						}
					}
				}
			}, this.keepaliveSeconds, TimeUnit.SECONDS);
		}
	}

	/**
	 * Answers a PINGREQ from the broker with PINGRESP, if the channel is open.
	 *
	 * @param channel the channel to the broker
	 */
	private void handlePingReq(Channel channel) {
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "handlePingReq()");
		if (!channel.isOpen()) {
			if (BuildConfig.LOGGING && enableLogging) log(TAG, "handlePingReq()->channel is not open, returning");
			return;
		}
		MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0);
		channel.writeAndFlush(new MqttMessage(fixedHeader));
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "sendPingReq()->Sent MQTT PingResp");
	}

	/**
	 * Handles a PINGRESP by cancelling the response timer and allowing the next PINGREQ.
	 */
	private void handlePingResp() {
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "handlePingResp");
		long now = System.currentTimeMillis();
		long delta = ((now - keepAliveTracker.getPingEpoch()) / 1000);
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "handlePingResp()->Keep alive ping response delta=" + delta + " keep alive interval " + this.keepaliveSeconds);
		if (this.pingRespTimeout != null) {
			this.pingRespTimeout.cancel(true);
			this.pingRespTimeout = null;

			pingReqFuture = null;
		}
	}
}
