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
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

/**
 * An {@link MqttClientPersistence} that keeps each entry in its own file, so the client's
 * in-flight QoS 1 and 2 state survives a restart, as with Paho's class of the same name. The
 * files are in a directory named after the client ID and broker URI, inside the directory given
 * to the constructor (the working directory by default). Each entry is written to a temporary
 * file and renamed into place, so a crash leaves either the old entry or the new one.
 * <p>
 * Unlike Paho, the client uses no persistence unless one is given to its constructor.
 */
public class MqttDefaultFilePersistence implements MqttClientPersistence {

	private static final String SUFFIX = ".msg";

	private final File root;
	private Path dir;

	/**
	 * Creates a file persistence in the working directory.
	 */
	public MqttDefaultFilePersistence() {
		this(System.getProperty("user.dir"));
	}

	/**
	 * Creates a file persistence in a directory.
	 *
	 * @param directory the directory to keep the clients' state in; created if missing
	 */
	public MqttDefaultFilePersistence(String directory) {
		this.root = new File(directory);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public synchronized void open(String clientId, String serverURI) throws MqttPersistenceException {
		Path path = root.toPath().resolve(directoryName(clientId, serverURI));
		try {
			Files.createDirectories(path);
		} catch (IOException e) {
			throw new MqttPersistenceException(e);
		}
		dir = path;
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * The files are kept, for the next client with the same ID and broker.
	 */
	@Override
	public synchronized void close() {
		dir = null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public synchronized void put(String key, IMqttPersistable persistable) throws MqttPersistenceException {
		Path file = file(key);
		Path temp = file.resolveSibling(file.getFileName() + ".tmp");
		try (OutputStream os = Files.newOutputStream(temp); DataOutputStream out = new DataOutputStream(os)) {
			out.writeInt(persistable.getHeaderLength());
			out.write(persistable.getHeaderBytes(), persistable.getHeaderOffset(), persistable.getHeaderLength());
			out.writeInt(persistable.getPayloadLength());
			if (persistable.getPayloadLength() > 0) {
				out.write(persistable.getPayloadBytes(), persistable.getPayloadOffset(), persistable.getPayloadLength());
			}
		} catch (IOException e) {
			throw new MqttPersistenceException(e);
		}
		try {
			try {
				Files.move(temp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
			} catch (AtomicMoveNotSupportedException e) {
				Files.move(temp, file, StandardCopyOption.REPLACE_EXISTING);
			}
		} catch (IOException e) {
			throw new MqttPersistenceException(e);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public synchronized IMqttPersistable get(String key) throws MqttPersistenceException {
		Path file = file(key);
		if (!Files.exists(file)) {
			return null;
		}
		try (InputStream is = Files.newInputStream(file); DataInputStream in = new DataInputStream(is)) {
			byte[] header = new byte[in.readInt()];
			in.readFully(header);
			byte[] payload = new byte[in.readInt()];
			in.readFully(payload);
			return new MqttPersistable(header, payload);
		} catch (IOException e) {
			throw new MqttPersistenceException(e);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public synchronized void remove(String key) throws MqttPersistenceException {
		try {
			Files.deleteIfExists(file(key));
		} catch (IOException e) {
			throw new MqttPersistenceException(e);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public synchronized Enumeration<String> keys() throws MqttPersistenceException {
		List<String> keys = new ArrayList<>();
		try (DirectoryStream<Path> files = Files.newDirectoryStream(openDir(), "*" + SUFFIX)) {
			for (Path f : files) {
				String name = f.getFileName().toString();
				keys.add(name.substring(0, name.length() - SUFFIX.length()));
			}
		} catch (IOException e) {
			throw new MqttPersistenceException(e);
		}
		return Collections.enumeration(keys);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public synchronized void clear() throws MqttPersistenceException {
		Enumeration<String> keys = keys();
		while (keys.hasMoreElements()) {
			remove(keys.nextElement());
		}
	}

	/**
	 * Returns the file of an entry.
	 *
	 * @param key the entry's key
	 * @return its file
	 * @throws MqttPersistenceException if the persistence is not open
	 */
	private Path file(String key) throws MqttPersistenceException {
		return openDir().resolve(safe(key) + SUFFIX);
	}

	/**
	 * Returns the directory of the open client.
	 *
	 * @return the directory
	 * @throws MqttPersistenceException if the persistence is not open
	 */
	private Path openDir() throws MqttPersistenceException {
		if (dir == null) {
			throw new MqttPersistenceException("persistence is not open");
		}
		return dir;
	}

	/**
	 * Turns a client ID, URI or key into a file name, keeping letters, digits, {@code -} and
	 * {@code _} and replacing anything else with {@code _}.
	 *
	 * @param s the text
	 * @return the file name part
	 */
	/**
	 * The longest directory name used as it is; longer ones are shortened (see below).
	 */
	private static final int MAX_NAME = 100;

	/**
	 * Returns the name of a client's directory: its client ID and server URI, with characters
	 * other than letters, digits, {@code -} and {@code _} replaced by {@code _}. A name longer
	 * than {@value #MAX_NAME} characters is cut to 64 and followed by a SHA-256 hash of the
	 * client ID and URI, so it stays unique and within the 255-character limit file systems
	 * put on a name (a client ID may be up to 65535 bytes; Paho issue 1061). Shorter names are
	 * unchanged, so existing directories are still found.
	 *
	 * @param clientId  the client ID
	 * @param serverURI the server URI
	 * @return the directory name
	 */
	static String directoryName(String clientId, String serverURI) {
		String name = safe(clientId) + "-" + safe(serverURI);
		if (name.length() <= MAX_NAME) {
			return name;
		}
		try {
			byte[] hash = java.security.MessageDigest.getInstance("SHA-256")
					.digest((clientId + "\n" + serverURI).getBytes(java.nio.charset.StandardCharsets.UTF_8));
			StringBuilder b = new StringBuilder(name.substring(0, 64)).append('-');
			for (byte x : hash) {
				b.append(String.format("%02x", x & 0xff));
			}
			return b.toString();
		} catch (java.security.NoSuchAlgorithmException e) {
			throw new IllegalStateException("SHA-256 is not available", e);   // every JVM has it
		}
	}

	private static String safe(String s) {
		StringBuilder b = new StringBuilder(s.length());
		for (char c : s.toCharArray()) {
			b.append(Character.isLetterOrDigit(c) || c == '-' || c == '_' ? c : '_');
		}
		return b.toString();
	}
}
