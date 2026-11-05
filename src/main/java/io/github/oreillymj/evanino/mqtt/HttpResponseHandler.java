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
 * Contributors:
 *    Michael OReilly - initial API and implementation
 *******************************************************************************/

package io.github.oreillymj.evanino.mqtt;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakeException;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;
import io.netty.util.CharsetUtil;

/**
 * Checks the HTTP response to the WebSocket upgrade request. Anything other than
 * {@code 101 Switching Protocols} is treated as an error, and a failed handshake closes the
 * channel.
 */
public class HttpResponseHandler extends SimpleChannelInboundHandler<FullHttpResponse> {
	private final String TAG = "HttpResponseHandler";
	private final boolean enableLogging = false;

	private SimpleLogger logger = new SimpleLogger();

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
	 * Passes a {@code 101 Switching Protocols} response down the pipeline and rejects any
	 * other response.
	 *
	 * @param ctx the channel handler context
	 * @param msg the HTTP response
	 * @throws IllegalStateException if the response status is not 101
	 */
	@Override
	protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) {
		if (BuildConfig.LOGGING && enableLogging) log("HttpResponseHandler->channelRead0()->");
		if (msg.status().code() != 101) { // 101 Switching Protocols is expected for WebSocket upgrade
			throw new IllegalStateException(
					"Unexpected FullHttpResponse (status=" + msg.status() +
							", content=" + msg.content().toString(CharsetUtil.UTF_8) + ')');
		} else {
			// If it's a 101 Switching Protocols response, we just let it pass through
			ctx.fireChannelRead(msg.retain());
		}
	}

	/**
	 * Handles WebSocket handshake state events, forwarding {@code HANDSHAKE_COMPLETE} to the
	 * next handler. Other events are passed to the superclass.
	 *
	 * @param ctx the channel handler context
	 * @param evt the user event
	 * @throws Exception if the superclass fails to handle the event
	 */
	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
		if (BuildConfig.LOGGING && enableLogging) log("HttpResponseHandler->userEventTriggered()->");
		if (BuildConfig.LOGGING && enableLogging) log("HttpResponseHandler->userEventTriggered()-> " + evt.toString());
		if (evt instanceof WebSocketClientProtocolHandler.ClientHandshakeStateEvent) {
			WebSocketClientProtocolHandler.ClientHandshakeStateEvent handshakeEvent =
					(WebSocketClientProtocolHandler.ClientHandshakeStateEvent) evt;
			switch (handshakeEvent) {
				case HANDSHAKE_ISSUED:
					if (BuildConfig.LOGGING && enableLogging) log("HttpResponseHandler->userEventTriggered()->WebSocket Handshake Issued.");
					break;
				case HANDSHAKE_COMPLETE:
					if (BuildConfig.LOGGING && enableLogging) log("HttpResponseHandler->userEventTriggered()->WebSocket Handshake complete");
					// Fire the event to let other handlers know the handshake is complete
					ctx.fireUserEventTriggered(evt);
					break;
				case HANDSHAKE_TIMEOUT:
					if (BuildConfig.LOGGING && enableLogging) log("HttpResponseHandler->userEventTriggered()->WebSocket Handshake Timeout.");
					break;
			}
		} else {
			super.userEventTriggered(ctx, evt);
		}
	}

	/**
	 * Closes the channel if the WebSocket handshake failed. Other exceptions are swallowed
	 * and not propagated further down the pipeline.
	 *
	 * @param ctx   the channel handler context
	 * @param cause the exception
	 */
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		if (BuildConfig.LOGGING && enableLogging) log("HttpResponseHandler->exceptionCaught()->");
		if (BuildConfig.LOGGING && enableLogging) log("HttpResponseHandler->exceptionCaught()->Reason=" + cause.getMessage());
		if (cause instanceof WebSocketClientHandshakeException) {
			if (NettyDebug.PRINT_STACK_TRACES) {
				cause.printStackTrace();
			}
			if (BuildConfig.LOGGING && enableLogging) log("HttpResponseHandler->exceptionCaught()->Reason=" + cause.getMessage());
			ctx.close();
		}
	}

}
