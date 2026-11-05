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

import java.io.*;
import java.util.*;

/**
 * Keeps the session state that must survive a restart in an {@link MqttClientPersistence}: the
 * QoS 1 and 2 publishes awaiting acknowledgement, and the incoming QoS 2 messages awaiting
 * PUBREL. Outgoing entries are stored under {@code s-<packet id>}, incoming ones under
 * {@code r-<packet id>}. A store that fails is reported and otherwise ignored, so messaging
 * carries on with the state in memory.
 */
final class SessionStore {

	/**
	 * Version of the header layout written by {@link #putOutgoing}.
	 */
	private static final int VERSION = 1;

	private final MqttClientPersistence persistence;
	private final SimpleLogger logger = new SimpleLogger();
	private boolean open;

	/**
	 * Wraps a persistence.
	 *
	 * @param persistence the persistence
	 */
	SessionStore(MqttClientPersistence persistence) {
		this.persistence = persistence;
	}

	/**
	 * An outgoing publish read back from the store.
	 */
	static final class Outgoing {
		final int packetId;
		final int qos;
		final boolean retained;
		final boolean pubrel;
		final long sequence;
		final String topic;
		final byte[] payload;

		Outgoing(int packetId, int qos, boolean retained, boolean pubrel, long sequence, String topic, byte[] payload) {
			this.packetId = packetId;
			this.qos = qos;
			this.retained = retained;
			this.pubrel = pubrel;
			this.sequence = sequence;
			this.topic = topic;
			this.payload = payload;
		}
	}

	/**
	 * An incoming QoS 2 message read back from the store.
	 */
	static final class Incoming {
		final int packetId;
		final boolean retained;
		final String topic;
		final byte[] payload;

		Incoming(int packetId, boolean retained, String topic, byte[] payload) {
			this.packetId = packetId;
			this.retained = retained;
			this.topic = topic;
			this.payload = payload;
		}
	}

	/**
	 * Opens the persistence for a client, once.
	 *
	 * @param clientId  the client ID
	 * @param serverURI the broker URI
	 * @return {@code true} if it was opened by this call
	 */
	synchronized boolean open(String clientId, String serverURI) {
		if (open) {
			return false;
		}
		try {
			persistence.open(clientId, serverURI);
			open = true;
			return true;
		} catch (MqttPersistenceException e) {
			report("open", e);
			return false;
		}
	}

	/**
	 * Closes the persistence, keeping what it holds.
	 */
	synchronized void close() {
		if (!open) {
			return;
		}
		open = false;
		try {
			persistence.close();
		} catch (MqttPersistenceException e) {
			report("close", e);
		}
	}

	/**
	 * Removes everything, as when a clean session starts.
	 */
	synchronized void clear() {
		if (!open) {
			return;
		}
		try {
			persistence.clear();
		} catch (MqttPersistenceException e) {
			report("clear", e);
		}
	}

	/**
	 * Stores, or updates, an outgoing QoS 1 or 2 publish.
	 *
	 * @param publish the publish
	 */
	synchronized void putOutgoing(MqttPendingPublish publish) {
		if (!open) {
			return;
		}
		try {
			ByteArrayOutputStream bytes = new ByteArrayOutputStream();
			DataOutputStream out = new DataOutputStream(bytes);
			out.writeInt(VERSION);
			out.writeInt(publish.getMessageId());
			out.writeByte(publish.getQos().value());
			out.writeBoolean(publish.getMessage().fixedHeader().isRetain());
			out.writeBoolean(publish.getPubrelMessage() != null);
			out.writeLong(publish.getSequence());
			out.writeUTF(publish.getMessage().variableHeader().topicName());
			out.flush();
			byte[] payload = io.netty.buffer.ByteBufUtil.getBytes(publish.getPayload(),
					publish.getPayload().readerIndex(), publish.getPayload().readableBytes(), true);
			persistence.put("s-" + publish.getMessageId(), new MqttPersistable(bytes.toByteArray(), payload));
		} catch (IOException | MqttPersistenceException e) {
			report("store publish " + publish.getMessageId(), e);
		}
	}

	/**
	 * Removes an outgoing publish.
	 *
	 * @param packetId its packet identifier
	 */
	synchronized void removeOutgoing(int packetId) {
		remove("s-" + packetId);
	}

	/**
	 * Stores an incoming QoS 2 message awaiting PUBREL.
	 *
	 * @param packetId the packet identifier
	 * @param topic    the topic
	 * @param retained the retain flag
	 * @param payload  the payload
	 */
	synchronized void putIncoming(int packetId, String topic, boolean retained, byte[] payload) {
		if (!open) {
			return;
		}
		try {
			ByteArrayOutputStream bytes = new ByteArrayOutputStream();
			DataOutputStream out = new DataOutputStream(bytes);
			out.writeInt(VERSION);
			out.writeInt(packetId);
			out.writeBoolean(retained);
			out.writeUTF(topic);
			out.flush();
			persistence.put("r-" + packetId, new MqttPersistable(bytes.toByteArray(), payload));
		} catch (IOException | MqttPersistenceException e) {
			report("store incoming " + packetId, e);
		}
	}

	/**
	 * Removes an incoming QoS 2 message.
	 *
	 * @param packetId its packet identifier
	 */
	synchronized void removeIncoming(int packetId) {
		remove("r-" + packetId);
	}

	/**
	 * Reads the stored outgoing publishes, in the order they were made.
	 *
	 * @return the publishes
	 */
	synchronized List<Outgoing> outgoing() {
		List<Outgoing> result = new ArrayList<>();
		for (String key : keys("s-")) {
			try {
				IMqttPersistable p = persistence.get(key);
				DataInputStream in = header(p);
				if (in.readInt() != VERSION) {
					continue;
				}
				int id = in.readInt();
				int qos = in.readByte();
				boolean retained = in.readBoolean();
				boolean pubrel = in.readBoolean();
				long sequence = in.readLong();
				String topic = in.readUTF();
				result.add(new Outgoing(id, qos, retained, pubrel, sequence, topic, payload(p)));
			} catch (IOException | MqttPersistenceException e) {
				report("read " + key, e);
			}
		}
		result.sort(Comparator.comparingLong(o -> o.sequence));
		return result;
	}

	/**
	 * Reads the stored incoming QoS 2 messages.
	 *
	 * @return the messages
	 */
	synchronized List<Incoming> incoming() {
		List<Incoming> result = new ArrayList<>();
		for (String key : keys("r-")) {
			try {
				IMqttPersistable p = persistence.get(key);
				DataInputStream in = header(p);
				if (in.readInt() != VERSION) {
					continue;
				}
				int id = in.readInt();
				boolean retained = in.readBoolean();
				String topic = in.readUTF();
				result.add(new Incoming(id, retained, topic, payload(p)));
			} catch (IOException | MqttPersistenceException e) {
				report("read " + key, e);
			}
		}
		return result;
	}

	private void remove(String key) {
		if (!open) {
			return;
		}
		try {
			persistence.remove(key);
		} catch (MqttPersistenceException e) {
			report("remove " + key, e);
		}
	}

	private List<String> keys(String prefix) {
		if (!open) {
			return Collections.emptyList();
		}
		List<String> keys = new ArrayList<>();
		try {
			Enumeration<String> all = persistence.keys();
			while (all.hasMoreElements()) {
				String key = all.nextElement();
				if (key.startsWith(prefix)) {
					keys.add(key);
				}
			}
		} catch (MqttPersistenceException e) {
			report("list keys", e);
		}
		return keys;
	}

	private static DataInputStream header(IMqttPersistable p) throws MqttPersistenceException {
		return new DataInputStream(new ByteArrayInputStream(p.getHeaderBytes(), p.getHeaderOffset(), p.getHeaderLength()));
	}

	private static byte[] payload(IMqttPersistable p) throws MqttPersistenceException {
		byte[] payload = new byte[p.getPayloadLength()];
		if (payload.length > 0) {
			System.arraycopy(p.getPayloadBytes(), p.getPayloadOffset(), payload, 0, payload.length);
		}
		return payload;
	}

	private void report(String what, Exception e) {
		logger.log("SessionStore: cannot " + what + ": " + e);
	}
}
