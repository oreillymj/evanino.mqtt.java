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
 * Paho-compatible representation of data that can be stored by an
 * {@link MqttClientPersistence} implementation. The data is split into a header part and a
 * payload part, each described by a backing array, an offset and a length.
 */
public interface IMqttPersistable {
	/**
	 * Returns the array that holds the header bytes.
	 *
	 * @return the header byte array
	 * @throws MqttPersistenceException if the header cannot be obtained
	 */
	byte[] getHeaderBytes() throws MqttPersistenceException;

	/**
	 * Returns the offset of the header within {@link #getHeaderBytes()}.
	 *
	 * @return the header offset
	 * @throws MqttPersistenceException if the header cannot be obtained
	 */
	int getHeaderOffset() throws MqttPersistenceException;

	/**
	 * Returns the number of header bytes, starting at {@link #getHeaderOffset()}.
	 *
	 * @return the header length
	 * @throws MqttPersistenceException if the header cannot be obtained
	 */
	int getHeaderLength() throws MqttPersistenceException;

	/**
	 * Returns the array that holds the payload bytes.
	 *
	 * @return the payload byte array, which may be {@code null} if there is no payload
	 * @throws MqttPersistenceException if the payload cannot be obtained
	 */
	byte[] getPayloadBytes() throws MqttPersistenceException;

	/**
	 * Returns the offset of the payload within {@link #getPayloadBytes()}.
	 *
	 * @return the payload offset
	 * @throws MqttPersistenceException if the payload cannot be obtained
	 */
	int getPayloadOffset() throws MqttPersistenceException;

	/**
	 * Returns the number of payload bytes, starting at {@link #getPayloadOffset()}.
	 *
	 * @return the payload length
	 * @throws MqttPersistenceException if the payload cannot be obtained
	 */
	int getPayloadLength() throws MqttPersistenceException;
}