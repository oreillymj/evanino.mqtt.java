/*******************************************************************************
 * Copyright (c) 2026 Michael OReilly
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

import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * An {@link MqttClientPersistence} that keeps entries in memory, as Paho's class of the same
 * name does. The state survives a reconnect and a new client created with the same instance in
 * the same process, not a restart. It is what the client does without any persistence.
 */
public class MemoryPersistence implements MqttClientPersistence {

	private final Map<String, IMqttPersistable> entries = new LinkedHashMap<>();

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void open(String clientId, String serverURI) {
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * The entries are kept, for a client created later with this instance.
	 */
	@Override
	public void close() {
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public synchronized void put(String key, IMqttPersistable persistable) {
		entries.put(key, persistable);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public synchronized IMqttPersistable get(String key) {
		return entries.get(key);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public synchronized void remove(String key) {
		entries.remove(key);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public synchronized Enumeration<String> keys() {
		return Collections.enumeration(new java.util.ArrayList<>(entries.keySet()));
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public synchronized void clear() {
		entries.clear();
	}
}
