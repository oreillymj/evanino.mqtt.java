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
 * Paho-compatible exception thrown by {@link MqttClientPersistence} implementations when
 * storing, retrieving or removing persisted data fails. Always carries the reason code
 * {@link MqttException#REASON_CODE_CLIENT_EXCEPTION}.
 */
public class MqttPersistenceException extends MqttException {
	/**
	 * Creates an exception with no cause.
	 */
	public MqttPersistenceException() {
		super(MqttException.REASON_CODE_CLIENT_EXCEPTION);
	}

	/**
	 * Creates an exception wrapping the given cause.
	 *
	 * @param cause the underlying failure
	 */
	public MqttPersistenceException(Throwable cause) {
		super(MqttException.REASON_CODE_CLIENT_EXCEPTION, cause);
	}

	/**
	 * Creates an exception with a descriptive message, carried as the cause.
	 *
	 * @param message description of the failure
	 */
	public MqttPersistenceException(String message) {
		super(MqttException.REASON_CODE_CLIENT_EXCEPTION, new Throwable(message));
	}
}