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
 * Simple {@link IMqttPersistable} backed by copies of the header and payload arrays. Offsets
 * are always 0 and lengths are always the full array length.
 */
public class MqttPersistable implements IMqttPersistable {
	private final byte[] header;
	private final byte[] payload;

	/**
	 * Creates a persistable from copies of the given arrays.
	 *
	 * @param header  the header bytes; {@code null} is stored as an empty array
	 * @param payload the payload bytes; {@code null} is stored as an empty array
	 */
	public MqttPersistable(byte[] header, byte[] payload) {
		// store defensively-copied, fixed-offset arrays — callers of get()
		// always see offset 0, full-array length, regardless of how the
		// original bytes were sliced when put() was called
		this.header = (header != null) ? header.clone() : new byte[0];
		this.payload = (payload != null) ? payload.clone() : new byte[0];
	}

	/**
	 * {@inheritDoc}
	 *
	 * @return the header bytes, never {@code null}
	 */
	@Override
	public byte[] getHeaderBytes() {
		return header;
	}

	/**
	 * {@inheritDoc}
	 *
	 * @return always {@code 0}
	 */
	@Override
	public int getHeaderOffset() {
		return 0;
	}

	/**
	 * {@inheritDoc}
	 *
	 * @return the length of the header array
	 */
	@Override
	public int getHeaderLength() {
		return header.length;
	}

	/**
	 * {@inheritDoc}
	 *
	 * @return the payload bytes, never {@code null}
	 */
	@Override
	public byte[] getPayloadBytes() {
		return payload;
	}

	/**
	 * {@inheritDoc}
	 *
	 * @return always {@code 0}
	 */
	@Override
	public int getPayloadOffset() {
		return 0;
	}

	/**
	 * {@inheritDoc}
	 *
	 * @return the length of the payload array
	 */
	@Override
	public int getPayloadLength() {
		return payload.length;
	}
}