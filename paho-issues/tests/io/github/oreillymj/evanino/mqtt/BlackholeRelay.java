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
import java.net.URI;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A TCP relay to a broker that can stop forwarding without closing anything, as when a cable is
 * pulled or a firewall starts dropping packets: no FIN or RST reaches either side, data just stops.
 * While black-holed, new connections are accepted and get nowhere either.
 */
final class BlackholeRelay implements AutoCloseable {

	private final ServerSocket server;
	private final int brokerPort;
	private final List<Socket> sockets = new CopyOnWriteArrayList<>();
	private volatile boolean blackholed;

	BlackholeRelay(int brokerPort) throws IOException {
		this.brokerPort = brokerPort;
		server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
		Thread t = new Thread(this::accept, "blackhole-accept");
		t.setDaemon(true);
		t.start();
	}

	URI uri() {
		return URI.create("tcp://127.0.0.1:" + server.getLocalPort());
	}

	/** Stops (or resumes) forwarding, leaving every connection open. */
	void blackhole(boolean on) {
		blackholed = on;
	}

	/** Whether a connection the client opened has been closed by the client. */
	int openConnections() {
		int n = 0;
		for (Socket s : sockets) {
			if (!s.isClosed()) {
				n++;
			}
		}
		return n / 2;
	}

	private void accept() {
		try {
			while (true) {
				Socket client = server.accept();
				Socket broker = new Socket("127.0.0.1", brokerPort);
				sockets.add(client);
				sockets.add(broker);
				pump(client, broker);
				pump(broker, client);
			}
		} catch (IOException closed) {
			// done
		}
	}

	private void pump(Socket from, Socket to) {
		Thread t = new Thread(() -> {
			byte[] buf = new byte[8192];
			try {
				InputStream in = from.getInputStream();
				OutputStream out = to.getOutputStream();
				while (true) {
					while (blackholed) {
						Thread.sleep(20);   // read nothing, forward nothing
					}
					int n = in.read(buf);
					if (n < 0) {
						break;
					}
					while (blackholed) {
						Thread.sleep(20);   // drop nothing either: hold what was read
					}
					out.write(buf, 0, n);
					out.flush();
				}
			} catch (IOException | InterruptedException ignored) {
				// closed
			} finally {
				try {
					from.close();
					to.close();
				} catch (IOException ignored) {
					// closing
				}
			}
		}, "blackhole-pump");
		t.setDaemon(true);
		t.start();
	}

	@Override
	public void close() throws IOException {
		server.close();
		for (Socket s : sockets) {
			s.close();
		}
	}
}
