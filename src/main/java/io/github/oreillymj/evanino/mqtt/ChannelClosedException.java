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

/**
 * Signals that the channel to the broker was closed. {@link #isDuringConnect()} tells whether
 * the close happened while the connection was still being established, which
 * {@link MqttExceptionUtils#wrapAsMqttException(Throwable)} uses to choose between a
 * connect error and a lost connection.
 * <p>
 * Originally created by Valerii Sosliuk on 12/26/2017.
 */
public class ChannelClosedException extends RuntimeException {
    private final boolean duringConnect;

    private static final long serialVersionUID = 6266638352424706909L;

    /**
     * Creates an exception with no message or cause, not raised during connect.
     */
    public ChannelClosedException() {
        this(null,null,false);
    }

    /**
     * Creates an exception with the given message, not raised during connect.
     *
     * @param message description of the failure
     */
    public ChannelClosedException(String message) {
        this(message,null,false);
    }

    /**
     * Creates an exception with the given message and cause, not raised during connect.
     *
     * @param message description of the failure
     * @param cause   the underlying failure
     */
    public ChannelClosedException(String message, Throwable cause) {
        this(message,cause,false);
    }

    /**
     * Creates an exception wrapping the given cause, not raised during connect.
     *
     * @param cause the underlying failure
     */
    public ChannelClosedException(Throwable cause) {
        this(null,cause,false);
    }

    /**
     * Creates an exception with full control over suppression and stack trace capture,
     * not raised during connect.
     *
     * @param message            description of the failure
     * @param cause              the underlying failure
     * @param enableSuppression  whether suppressed exceptions can be added
     * @param writableStackTrace whether the stack trace should be captured
     */
    public ChannelClosedException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
        this.duringConnect=false;
    }

    /**
     * Creates an exception with the given message.
     *
     * @param message       description of the failure
     * @param duringConnect whether the channel closed while connecting
     */
    public ChannelClosedException(String message, boolean duringConnect) {
        this(message, null, duringConnect);
    }

    /**
     * Creates an exception with the given message and cause.
     *
     * @param message       description of the failure
     * @param cause         the underlying failure
     * @param duringConnect whether the channel closed while connecting
     */
    public ChannelClosedException(String message, Throwable cause, boolean duringConnect) {
        super(message, cause);
        this.duringConnect = duringConnect;
    }

    /**
     * Returns whether the channel closed while the connection was being established.
     *
     * @return {@code true} if the close happened during connect
     */
    public boolean isDuringConnect() {
        return duringConnect;
    }
}
