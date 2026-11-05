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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;
import io.netty.handler.codec.http.websocketx.*;

import java.util.List;

/**
 * Adapts MQTT traffic to a WebSocket connection: outgoing MQTT bytes are wrapped in binary
 * frames, and incoming binary and continuation frames are unwrapped back to bytes. Ping
 * frames are answered with pongs and a close frame closes the channel.
 */
public class MqttWebSocketCodec extends MessageToMessageCodec<WebSocketFrame, ByteBuf> {

    private final String TAG = "MqttWebSocketCodec";
    private final boolean enableLogging=false;

    private SimpleLogger logger = new SimpleLogger();

    /**
     * Logs a message if logging is enabled for this codec.
     *
     * @param data the message to log
     */
    private void log(String data){
        if (enableLogging){
            logger.log(data);
        }
    }


    /**
     * Wraps outgoing MQTT bytes in a {@link BinaryWebSocketFrame}.
     *
     * @param ctx the channel handler context
     * @param msg the encoded MQTT bytes
     * @param out receives the resulting frame
     * @throws Exception if encoding fails
     */
    @Override
    protected void encode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
        if (BuildConfig.LOGGING && enableLogging) log("MqttWebSocketCodec encoding");
        out.add(new BinaryWebSocketFrame(msg.retain()));
    }

    /**
     * Unwraps an incoming WebSocket frame. Binary and continuation frames yield their content;
     * a close frame closes the channel; a ping is answered with a pong; a pong is ignored.
     *
     * @param ctx   the channel handler context
     * @param frame the incoming frame
     * @param out   receives the unwrapped MQTT bytes, if any
     * @throws UnsupportedOperationException if the frame is of any other type, such as text
     * @throws Exception if decoding fails
     */
    @Override
    protected void decode(ChannelHandlerContext ctx, WebSocketFrame  frame, List<Object> out) throws Exception {
        if (BuildConfig.LOGGING && enableLogging) log("MqttWebSocketCodec decoding");
        if (frame instanceof BinaryWebSocketFrame || frame instanceof ContinuationWebSocketFrame) {
            out.add(frame.content().retain());
        } else if (frame instanceof CloseWebSocketFrame) {
            ctx.close();
        } else if (frame instanceof PingWebSocketFrame) {
            ctx.writeAndFlush(new PongWebSocketFrame(frame.content().retain()));
        } else if (frame instanceof PongWebSocketFrame) {
            // Pong frames can be ignored
        } else {
            throw new UnsupportedOperationException("Unsupported frame type: " + frame.getClass().getName());
        }
    }

}