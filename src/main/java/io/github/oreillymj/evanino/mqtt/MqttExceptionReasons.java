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
 * Human readable descriptions of {@link MqttException} reason codes, used in place of Paho's
 * resource-bundle message catalog.
 */
public final class MqttExceptionReasons {

    /**
     * Prevents instantiation; this class only holds static helpers.
     */
    private MqttExceptionReasons() {
        // Utility class
    }

    /**
     * Returns an exception whose message describes the given reason code.
     *
     * @param code an {@link MqttException} reason code
     * @return a plain {@link Exception} carrying the description; unrecognised codes yield
     *         an "Unknown MQTT error code" message
     */
    public static Throwable getReason(int code) {
        return new Exception(getMessage(code));
    }

    /**
     * Returns a description of the given reason code.
     *
     * @param code an {@link MqttException} reason code
     * @return the description; for an unrecognised code, "Unknown MQTT error code: " and the code
     */
    public static String getMessage(int code) {

        switch (code) {

            case MqttException.REASON_CODE_CLIENT_EXCEPTION:
                return "The MQTT client encountered an exception. " +
                        "See the underlying cause for more details.";

            case MqttException.REASON_CODE_INVALID_PROTOCOL_VERSION:
                return "The MQTT protocol version requested by the client " +
                        "is not supported by the broker.";

            case MqttException.REASON_CODE_INVALID_CLIENT_ID:
                return "The broker rejected the supplied MQTT client ID.";

            case MqttException.REASON_CODE_BROKER_UNAVAILABLE:
                return "The MQTT broker is unavailable or unable to handle the request.";

            case MqttException.REASON_CODE_FAILED_AUTHENTICATION:
                return "MQTT authentication failed. The supplied username or password " +
                        "may be incorrect.";

            case MqttException.REASON_CODE_NOT_AUTHORIZED:
                return "The MQTT client is not authorized to perform the requested operation.";

            case MqttException.REASON_CODE_UNEXPECTED_ERROR:
                return "The MQTT broker reported an unexpected error.";

            case MqttException.REASON_CODE_SUBSCRIBE_FAILED:
                return "The MQTT broker rejected the subscription request.";

            case MqttException.REASON_CODE_CLIENT_TIMEOUT:
                return "The MQTT client timed out waiting for a response from the broker. " +
                        "The broker may no longer be responding to keep-alive messages.";

            case MqttException.REASON_CODE_NO_MESSAGE_IDS_AVAILABLE:
                return "The MQTT client has no message IDs available for a new operation.";

            case MqttException.REASON_CODE_WRITE_TIMEOUT:
                return "The MQTT client timed out while waiting to write data to the broker.";

            case MqttException.REASON_CODE_CLIENT_CONNECTED:
                return "The MQTT client is already connected.";

            case MqttException.REASON_CODE_CLIENT_ALREADY_DISCONNECTED:
                return "The MQTT client is already disconnected.";

            case MqttException.REASON_CODE_CLIENT_DISCONNECTING:
                return "The MQTT client is currently disconnecting and cannot accept " +
                        "new operations.";

            case MqttException.REASON_CODE_SERVER_CONNECT_ERROR:
                return "Unable to connect to the MQTT server.";

            case MqttException.REASON_CODE_CLIENT_NOT_CONNECTED:
                return "The MQTT client is not connected to the broker. " +
                        "Connect the client before performing this operation.";

            case MqttException.REASON_CODE_SOCKET_FACTORY_MISMATCH:
                return "The configured socket factory does not match the MQTT server URI. " +
                        "TCP connections require a regular SocketFactory and SSL connections " +
                        "require an SSLSocketFactory.";

            case MqttException.REASON_CODE_SSL_CONFIG_ERROR:
                return "The MQTT SSL/TLS configuration is invalid.";

            case MqttException.REASON_CODE_CLIENT_DISCONNECT_PROHIBITED:
                return "The MQTT client cannot be disconnected from within an MQTT callback. " +
                        "Disconnect must be performed from another thread.";

            case MqttException.REASON_CODE_INVALID_MESSAGE:
                return "The server returned data that is not a valid MQTT packet. " +
                        "The client may be connecting to a non-MQTT server or to an SSL " +
                        "endpoint without SSL enabled.";

            case MqttException.REASON_CODE_CONNECTION_LOST:
                return "The MQTT connection to the broker was unexpectedly lost.";

            case MqttException.REASON_CODE_CONNECT_IN_PROGRESS:
                return "An MQTT connection attempt is already in progress. " +
                        "Only one connection attempt can be active at a time.";

            case MqttException.REASON_CODE_CLIENT_CLOSED:
                return "The MQTT client has been closed. No further operations are permitted. " +
                        "Create a new MQTT client to continue.";

            case MqttException.REASON_CODE_TOKEN_INUSE:
                return "The MQTT token is already associated with another operation. " +
                        "Wait for the operation to complete or reset the token before reusing it.";

            case MqttException.REASON_CODE_MAX_INFLIGHT:
                return "The maximum number of MQTT in-flight messages has been reached. " +
                        "Wait for existing messages to complete before sending more.";

            case MqttException.REASON_CODE_DISCONNECTED_BUFFER_FULL:
                return "The disconnected-publishing buffer is full. " +
                        "The client must reconnect or buffered messages must be deleted " +
                        "before another message can be published.";

            default:
                return "Unknown MQTT error code: " + code;
        }
    }
}