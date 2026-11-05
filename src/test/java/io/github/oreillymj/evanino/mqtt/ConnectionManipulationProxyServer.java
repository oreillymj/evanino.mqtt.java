/*******************************************************************************
 * Ported from Eclipse Paho's Java client tests,
 * org/eclipse/paho/client/mqttv3/test/utilities/ConnectionManipulationProxyServer.java,
 * https://github.com/eclipse-paho/paho.mqtt.java. This project uses the Paho code under
 * the Eclipse Distribution License v1.0 (see licenses/EDL-1.0.txt).
 *
 * The original file has no notice of its own; it is part of Eclipse Paho, copyright
 * IBM Corp. and the other Paho contributors, under the Eclipse Public License v2.0
 * and the Eclipse Distribution License v1.0.
 *
 * Changes copyright (c) 2026 Michael OReilly, under the Apache License 2.0 (see LICENSE).
 *******************************************************************************/

package io.github.oreillymj.evanino.mqtt;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * Port of Paho's {@code ConnectionManipulationProxyServer}: a TCP proxy between the client
 * and the broker that can be switched off, to simulate the network dropping.
 * <p>
 * While enabled it listens on a local port and relays each connection to the broker. Disabling
 * it closes the listening socket and every relayed connection, so clients see the connection
 * drop and new connections are refused; enabling it listens again on the same port. Unlike
 * Paho's version, which relays one connection at a time from a polling loop, each connection
 * is relayed on its own threads.
 */
public final class ConnectionManipulationProxyServer {

	private final String host;
	private final int remotePort;
	private volatile int localPort;
	private volatile boolean portSet = false;

	private final Object lock = new Object();
	private ServerSocket serverSocket;
	private final List<Socket> openSockets = new ArrayList<>();

	/**
	 * Creates a proxy to the given broker.
	 *
	 * @param host       the broker host
	 * @param remotePort the broker port
	 * @param localPort  the local port to listen on, or 0 to pick a free one
	 */
	public ConnectionManipulationProxyServer(String host, int remotePort, int localPort) {
		this.host = host;
		this.remotePort = remotePort;
		this.localPort = localPort;
	}

	/**
	 * Starts listening and relaying connections.
	 *
	 * @throws IOException if the local port cannot be opened
	 */
	public void startProxy() throws IOException {
		enableProxy();
	}

	/**
	 * Starts listening again, on the same local port, if not already listening.
	 *
	 * @throws IOException if the local port cannot be opened
	 */
	public void enableProxy() throws IOException {
		synchronized (lock) {
			if (serverSocket != null) {
				return;
			}
			ServerSocket ss = new ServerSocket();
			ss.setReuseAddress(true);
			ss.bind(new InetSocketAddress("127.0.0.1", localPort));
			localPort = ss.getLocalPort();
			portSet = true;
			serverSocket = ss;
			Thread acceptor = new Thread(() -> acceptLoop(ss), "proxy-accept-" + localPort);
			acceptor.setDaemon(true);
			acceptor.start();
		}
	}

	/**
	 * Stops listening and closes every relayed connection, then waits briefly for the
	 * closures to take effect, as Paho's proxy does.
	 */
	public void disableProxy() {
		killOpenSockets();
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	/**
	 * Stops the proxy for good.
	 */
	public void stopProxy() {
		killOpenSockets();
	}

	/**
	 * Returns the local port clients should connect to.
	 *
	 * @return the local port
	 */
	public int getLocalPort() {
		return localPort;
	}

	/**
	 * Returns whether the local port has been chosen.
	 *
	 * @return {@code true} once the proxy has started listening
	 */
	public boolean isPortSet() {
		return portSet;
	}

	/**
	 * Closes the listening socket and all relayed connections.
	 */
	private void killOpenSockets() {
		synchronized (lock) {
			closeQuietly(serverSocket);
			serverSocket = null;
			for (Socket s : openSockets) {
				closeQuietly(s);
			}
			openSockets.clear();
		}
	}

	/**
	 * Accepts connections until the listening socket is closed, relaying each to the broker.
	 *
	 * @param ss the listening socket
	 */
	private void acceptLoop(ServerSocket ss) {
		while (!ss.isClosed()) {
			Socket client;
			try {
				client = ss.accept();
			} catch (IOException closed) {
				return;
			}
			Socket server;
			try {
				server = new Socket(host, remotePort);
			} catch (IOException ex) {
				System.err.println("ConnectionManipulationProxyServer cannot connect to " + host + ":" + remotePort);
				closeQuietly(client);
				continue;
			}
			synchronized (lock) {
				if (serverSocket != ss) {   // disabled while connecting
					closeQuietly(client);
					closeQuietly(server);
					return;
				}
				openSockets.add(client);
				openSockets.add(server);
			}
			pipe(client, server, "proxy-c2s-" + localPort);
			pipe(server, client, "proxy-s2c-" + localPort);
		}
	}

	/**
	 * Copies bytes from one socket to another until either closes, then closes both.
	 *
	 * @param from the socket to read from
	 * @param to   the socket to write to
	 * @param name the thread name
	 */
	private static void pipe(Socket from, Socket to, String name) {
		Thread t = new Thread(() -> {
			byte[] buffer = new byte[4096];
			try {
				InputStream in = from.getInputStream();
				OutputStream out = to.getOutputStream();
				int n;
				while ((n = in.read(buffer)) != -1) {
					out.write(buffer, 0, n);
					out.flush();
				}
			} catch (IOException ignored) {
				// connection closed
			} finally {
				closeQuietly(from);
				closeQuietly(to);
			}
		}, name);
		t.setDaemon(true);
		t.start();
	}

	/**
	 * Closes a socket, ignoring any failure.
	 *
	 * @param c the socket to close, or {@code null}
	 */
	private static void closeQuietly(AutoCloseable c) {
		if (c != null) {
			try {
				c.close();
			} catch (Exception ignored) {
			}
		}
	}
}
