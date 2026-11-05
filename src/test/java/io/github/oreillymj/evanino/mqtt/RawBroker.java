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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;

/**
 * A fake broker for tests that need to control the bytes on the wire: accepts connections one at
 * a time and reads and writes raw MQTT packets.
 */
final class RawBroker implements AutoCloseable {

	/** One MQTT packet: its first byte and its body (variable header and payload). */
	static final class Packet {
		final int first;
		final byte[] body;

		Packet(int first, byte[] body) {
			this.first = first;
			this.body = body;
		}

		int type() {
			return first >> 4;
		}

		/** The packet identifier, for packets whose body starts with one. */
		int packetId() {
			return ((body[0] & 0xff) << 8) | (body[1] & 0xff);
		}
	}

	private final ServerSocket server;
	private Socket socket;
	private DataInputStream in;
	private OutputStream out;

	RawBroker() throws IOException {
		this(InetAddress.getLoopbackAddress());
	}

	RawBroker(InetAddress address) throws IOException {
		server = new ServerSocket();
		server.bind(new InetSocketAddress(address, 0), 5);
		server.setSoTimeout(15_000);
	}

	int port() {
		return server.getLocalPort();
	}

	URI uri() {
		return URI.create("tcp://127.0.0.1:" + port());
	}

	/** Accepts the next connection, closing the previous one. */
	void accept() throws IOException {
		if (socket != null) {
			socket.close();
		}
		socket = server.accept();
		socket.setSoTimeout(15_000);
		in = new DataInputStream(socket.getInputStream());
		out = socket.getOutputStream();
	}

	/** Closes the current connection, as a broker restart or a network failure would. */
	void drop() throws IOException {
		if (socket != null) {
			socket.close();
			socket = null;
		}
	}

	/** Accepts a connection, reads CONNECT and answers with a CONNACK with this return code. */
	void acceptAndConnack(int returnCode) throws IOException {
		accept();
		Packet connect = read();
		if (connect.type() != 1) {
			throw new IOException("expected CONNECT, got type " + connect.type());
		}
		write(0x20, 0x02, 0x00, returnCode);
	}

	Packet read() throws IOException {
		int first = in.readUnsignedByte();
		int length = 0;
		int multiplier = 1;
		int b;
		do {
			b = in.readUnsignedByte();
			length += (b & 0x7f) * multiplier;
			multiplier *= 128;
		} while ((b & 0x80) != 0);
		byte[] body = new byte[length];
		in.readFully(body);
		return new Packet(first, body);
	}

	/**
	 * Reads packets until one of the given type, answering PINGREQs on the way. A negative type
	 * returns the next packet that is not a PINGREQ, whatever it is.
	 */
	Packet readUntil(int type) throws IOException {
		while (true) {
			Packet p = read();
			if (p.type() == type || (type < 0 && p.type() != 12)) {
				return p;
			}
			if (p.type() == 12) {
				write(0xd0, 0x00);
			}
		}
	}

	void write(int... bytes) throws IOException {
		byte[] b = new byte[bytes.length];
		for (int i = 0; i < bytes.length; i++) {
			b[i] = (byte) bytes[i];
		}
		out.write(b);
		out.flush();
	}

	void write(byte[] bytes) throws IOException {
		out.write(bytes);
		out.flush();
	}

	/** Answers a SUBSCRIBE with a SUBACK granting QoS 1 for one filter. */
	void suback(Packet subscribe) throws IOException {
		write(0x90, 0x03, subscribe.body[0] & 0xff, subscribe.body[1] & 0xff, 0x01);
	}

	/** Writes a PUBLISH with a short topic and payload. */
	void publish(String topic, int qos, int packetId, String payload) throws IOException {
		byte[] t = topic.getBytes(StandardCharsets.UTF_8);
		byte[] p = payload.getBytes(StandardCharsets.UTF_8);
		int length = 2 + t.length + (qos > 0 ? 2 : 0) + p.length;
		java.io.ByteArrayOutputStream b = new java.io.ByteArrayOutputStream();
		b.write(0x30 | (qos << 1));
		b.write(length);
		b.write(t.length >> 8);
		b.write(t.length);
		b.write(t, 0, t.length);
		if (qos > 0) {
			b.write(packetId >> 8);
			b.write(packetId);
		}
		b.write(p, 0, p.length);
		write(b.toByteArray());
	}

	boolean isClosedByPeer() {
		try {
			socket.setSoTimeout(100);
			return in.read() == -1;
		} catch (java.net.SocketTimeoutException e) {
			return false;
		} catch (IOException e) {
			return true;
		}
	}

	@Override
	public void close() throws IOException {
		if (socket != null) {
			socket.close();
		}
		server.close();
	}
}
