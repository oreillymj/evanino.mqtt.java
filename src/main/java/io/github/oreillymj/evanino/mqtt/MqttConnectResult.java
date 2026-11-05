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

import io.netty.channel.ChannelFuture;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;

/**
 * The outcome of a connection attempt, built from the broker's CONNACK.
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public final class MqttConnectResult {

	private final boolean success;
	private final boolean serverSessionPresent;
	private final MqttConnectReturnCode returnCode;
	private final ChannelFuture closeFuture;

	/**
	 * Creates a connect result.
	 *
	 * @param success              whether the broker accepted the connection
	 * @param serverSessionPresent whether the broker resumed an existing session
	 * @param returnCode           the CONNACK return code
	 * @param closeFuture          a future that completes when the channel closes
	 */
	MqttConnectResult(boolean success, boolean serverSessionPresent, MqttConnectReturnCode returnCode, ChannelFuture closeFuture) {
		this.success = success;
		this.serverSessionPresent = serverSessionPresent;
		this.returnCode = returnCode;
		this.closeFuture = closeFuture;
	}


	/**
	 * Returns whether the broker accepted the connection.
	 *
	 * @return {@code true} if the connection was accepted
	 */
	public boolean isSuccess() {
		return this.success;
	}

	/**
	 * Returns whether the broker resumed an existing session for this client.
	 *
	 * @return {@code true} if a session was present on the broker
	 */
	public boolean isServerSessionPresent() {
		return this.serverSessionPresent;
	}

	/**
	 * Returns the CONNACK return code.
	 *
	 * @return the return code
	 */
	public MqttConnectReturnCode getReturnCode() {
		return returnCode;
	}

	/**
	 * Returns a future that completes when the connection's channel closes.
	 *
	 * @return the channel's close future
	 */
	public ChannelFuture getCloseFuture() {
		return closeFuture;
	}

	/**
	 * Returns the name of the CONNACK return code.
	 *
	 * @return the constant name of the return code, or {@code "Unknown"} if it is not recognised
	 */
	public String getReturnMessage() {
		String retval = "Unknown";
		switch (this.returnCode) {
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
}
