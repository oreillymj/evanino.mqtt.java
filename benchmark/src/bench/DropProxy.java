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

package bench;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A TCP relay to the broker that can drop every connection at once, as a network failure or
 * broker restart would, for the soak test. Uses two blocking threads per connection, which is
 * fine for the few dozen connections the soak test makes.
 */
final class DropProxy implements AutoCloseable {

	private final ServerSocket server;
	private final int brokerPort;
	private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
	/** Connections accepted so far. */
	final AtomicInteger accepted = new AtomicInteger();

	/**
	 * Starts the relay on a free loopback port.
	 *
	 * @param brokerPort the broker's port on 127.0.0.1
	 * @throws IOException if the port cannot be opened
	 */
	DropProxy(int brokerPort) throws IOException {
		this.brokerPort = brokerPort;
		this.server = new ServerSocket(0, 200, InetAddress.getLoopbackAddress());
		daemon(this::acceptLoop, "drop-proxy-accept");
	}

	/**
	 * Returns the relay's URI.
	 *
	 * @return {@code tcp://127.0.0.1:port}
	 */
	String uri() {
		return "tcp://127.0.0.1:" + server.getLocalPort();
	}

	/**
	 * Closes every relayed connection.
	 */
	void dropAll() {
		for (Socket s : sockets) {
			closeQuietly(s);
		}
		sockets.clear();
	}

	@Override
	public void close() {
		closeQuietly(server);
		dropAll();
	}

	private void acceptLoop() {
		while (!server.isClosed()) {
			try {
				Socket client = server.accept();
				Socket broker = new Socket(InetAddress.getLoopbackAddress(), brokerPort);
				client.setTcpNoDelay(true);
				broker.setTcpNoDelay(true);
				sockets.add(client);
				sockets.add(broker);
				accepted.incrementAndGet();
				daemon(() -> pump(client, broker), "drop-proxy-up");
				daemon(() -> pump(broker, client), "drop-proxy-down");
			} catch (IOException e) {
				return;
			}
		}
	}

	private void pump(Socket from, Socket to) {
		byte[] buffer = new byte[16384];
		try {
			InputStream in = from.getInputStream();
			OutputStream out = to.getOutputStream();
			int n;
			while ((n = in.read(buffer)) != -1) {
				out.write(buffer, 0, n);
				out.flush();
			}
		} catch (IOException ignored) {
			// one side closed
		} finally {
			closeQuietly(from);
			closeQuietly(to);
			sockets.remove(from);
			sockets.remove(to);
		}
	}

	private static void closeQuietly(AutoCloseable c) {
		try {
			c.close();
		} catch (Exception ignored) {
			// closing anyway
		}
	}

	private static void daemon(Runnable r, String name) {
		Thread t = new Thread(r, name);
		t.setDaemon(true);
		t.start();
	}
}
