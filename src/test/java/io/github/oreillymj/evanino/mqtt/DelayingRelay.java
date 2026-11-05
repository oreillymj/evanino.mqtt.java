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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * A TCP relay to the broker that forwards the client's bytes at once and holds back each
 * chunk of the broker's replies for a fixed delay.
 */
final class DelayingRelay {
	private final ServerSocket serverSocket;
	private final String host;
	private final int port;
	private final long delayMillis;
	private final java.util.Set<Socket> sockets = java.util.concurrent.ConcurrentHashMap.newKeySet();

	/**
	 * Starts the relay on a free loopback port.
	 *
	 * @param host        the broker host
	 * @param port        the broker port
	 * @param delayMillis how long to hold back each reply
	 * @throws IOException if the port cannot be opened
	 */
	DelayingRelay(String host, int port, long delayMillis) throws IOException {
		this.host = host;
		this.port = port;
		this.delayMillis = delayMillis;
		this.serverSocket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
		daemon(this::acceptLoop);
	}

	/**
	 * Returns the port the relay listens on.
	 *
	 * @return the local port
	 */
	int getPort() {
		return serverSocket.getLocalPort();
	}

	/**
	 * Closes every relayed connection, as a broker or network failure would.
	 */
	void dropConnections() {
		for (Socket socket : sockets) {
			try {
				socket.close();
			} catch (IOException ignored) {
				// closing anyway
			}
		}
		sockets.clear();
	}

	/**
	 * Stops listening.
	 */
	void close() {
		try {
			serverSocket.close();
		} catch (IOException ignored) {
			// closing anyway
		}
	}

	/**
	 * Accepts connections and relays each to the broker.
	 */
	private void acceptLoop() {
		while (!serverSocket.isClosed()) {
			try {
				Socket client = serverSocket.accept();
				Socket broker = new Socket(host, port);
				sockets.add(client);
				sockets.add(broker);
				daemon(() -> pump(client, broker, 0));
				daemon(() -> pump(broker, client, delayMillis));
			} catch (IOException e) {
				return;
			}
		}
	}

	/**
	 * Copies bytes from one socket to the other, waiting before writing each chunk, and
	 * closes both when either side closes.
	 *
	 * @param from        the socket to read
	 * @param to          the socket to write
	 * @param delayMillis how long to hold each chunk back
	 */
	private static void pump(Socket from, Socket to, long delayMillis) {
		byte[] buffer = new byte[8192];
		try {
			InputStream in = from.getInputStream();
			OutputStream out = to.getOutputStream();
			int n;
			while ((n = in.read(buffer)) != -1) {
				if (delayMillis > 0) {
					Thread.sleep(delayMillis);
				}
				out.write(buffer, 0, n);
				out.flush();
			}
		} catch (IOException | InterruptedException ignored) {
			// one side closed
		} finally {
			try {
				from.close();
			} catch (IOException ignored) {
				// already closed
			}
			try {
				to.close();
			} catch (IOException ignored) {
				// already closed
			}
		}
	}

	/**
	 * Runs a task on a daemon thread.
	 *
	 * @param task the task
	 */
	private static void daemon(Runnable task) {
		Thread t = new Thread(task, "delaying-relay");
		t.setDaemon(true);
		t.start();
	}
}
