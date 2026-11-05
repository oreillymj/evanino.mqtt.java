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

/**
 * Helpers for converting arbitrary failures into Paho-style {@link MqttException}s.
 */
public final class MqttExceptionUtils {

    /**
     * Prevents instantiation; this class only holds static helpers.
     */
    private MqttExceptionUtils() {
        // no instances
    }

    /**
     * Converts a failure into an {@link MqttException} with the closest matching reason code.
     * An {@code MqttException} is returned unchanged; timeouts, SSL errors, closed channels and
     * I/O errors map to their specific reason codes; anything else becomes
     * {@link MqttException#REASON_CODE_CLIENT_EXCEPTION}.
     *
     * @param t the failure to convert; {@code null} yields
     *          {@link MqttException#REASON_CODE_UNEXPECTED_ERROR}
     * @return an {@code MqttException} describing the failure, with {@code t} as its cause
     */
    public static MqttException wrapAsMqttException(Throwable t) {
        if (t == null) {
            return new MqttException(MqttException.REASON_CODE_UNEXPECTED_ERROR);
        }
        if (t instanceof MqttException) {
            return (MqttException) t;
        }
        if (t instanceof java.util.concurrent.TimeoutException) {
            return new MqttException(MqttException.REASON_CODE_CLIENT_TIMEOUT, t);
        }
        if (t instanceof javax.net.ssl.SSLException) {
            return new MqttException(MqttException.REASON_CODE_SSL_CONFIG_ERROR, t);
        }
        if (t instanceof ChannelClosedException) {
            ChannelClosedException cce = (ChannelClosedException) t;
            return cce.isDuringConnect()
                    ? new MqttException(MqttException.REASON_CODE_SERVER_CONNECT_ERROR, t)
                    : new MqttException(MqttException.REASON_CODE_CONNECTION_LOST, t);
        }
        if (t instanceof java.io.IOException) {
            return new MqttException(MqttException.REASON_CODE_SERVER_CONNECT_ERROR, t);
        }
        return new MqttException(MqttException.REASON_CODE_CLIENT_EXCEPTION, t);
    }
}