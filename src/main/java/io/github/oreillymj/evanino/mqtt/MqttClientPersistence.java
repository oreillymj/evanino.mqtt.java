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

import java.util.Enumeration;

/**
 * Paho-compatible persistence store, used to keep in-flight QoS 1 and QoS 2 messages so
 * they can be redelivered after a restart.
 */
public interface MqttClientPersistence {
	/**
	 * Opens the store for the given client and server. Must be called before any other method.
	 *
	 * @param clientId  the client identifier the data belongs to
	 * @param serverURI the URI of the broker the client connects to
	 * @throws MqttPersistenceException if the store cannot be opened
	 */
	void open(String clientId, String serverURI) throws MqttPersistenceException;

	/**
	 * Closes the store and releases its resources.
	 *
	 * @throws MqttPersistenceException if the store cannot be closed
	 */
	void close() throws MqttPersistenceException;

	/**
	 * Stores data under the given key, replacing any existing entry.
	 *
	 * @param key         the key to store the data under
	 * @param persistable the data to store
	 * @throws MqttPersistenceException if the data cannot be stored
	 */
	void put(String key, IMqttPersistable persistable) throws MqttPersistenceException;

	/**
	 * Retrieves the data stored under the given key.
	 *
	 * @param key the key to look up
	 * @return the stored data
	 * @throws MqttPersistenceException if the data cannot be read; implementations may also
	 *                                  throw this when there is no entry for the key
	 */
	IMqttPersistable get(String key) throws MqttPersistenceException;

	/**
	 * Removes the data stored under the given key, if any.
	 *
	 * @param key the key to remove
	 * @throws MqttPersistenceException if the data cannot be removed
	 */
	void remove(String key) throws MqttPersistenceException;

	/**
	 * Returns all keys currently in the store.
	 *
	 * @return the stored keys
	 * @throws MqttPersistenceException if the keys cannot be read
	 */
	Enumeration<String> keys() throws MqttPersistenceException;

	/**
	 * Removes all data from the store.
	 *
	 * @throws MqttPersistenceException if the store cannot be cleared
	 */
	void clear() throws MqttPersistenceException;
}