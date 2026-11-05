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

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * A minimal proxy for tests, speaking one of three protocols:
 * <ul>
 * <li>{@link Protocol#SOCKS4}: SOCKS4 and SOCKS4a {@code CONNECT}, optionally requiring a
 * user ID.</li>
 * <li>{@link Protocol#SOCKS5}: SOCKS5 {@code CONNECT} (RFC 1928) with IPv4, IPv6 and
 * domain-name addresses, optionally requiring username/password authentication (RFC 1929).</li>
 * <li>{@link Protocol#HTTP}: HTTP {@code CONNECT}, optionally requiring {@code Basic}
 * proxy authentication.</li>
 * </ul>
 * Each tunnelled connection is relayed on its own threads. The proxy records the destination of
 * every tunnel it opens, so a test can check that a connection really went through it, and the
 * user of every login it refuses. Connections that close before sending anything are ignored.
 */
public final class TestProxyServer implements AutoCloseable {

	/** The proxy protocol to speak. */
	public enum Protocol {
		/** SOCKS4 and SOCKS4a. */
		SOCKS4,
		/** SOCKS5. */
		SOCKS5,
		/** HTTP {@code CONNECT}. */
		HTTP
	}

	private final Protocol protocol;
	private final String username;
	private final String password;
	private final ServerSocket serverSocket;
	private final List<Socket> openSockets = Collections.synchronizedList(new ArrayList<>());
	private final List<String> tunnels = Collections.synchronizedList(new ArrayList<>());
	private final List<String> rejectedLogins = Collections.synchronizedList(new ArrayList<>());

	/**
	 * Starts a proxy that requires no authentication, on a free loopback port.
	 *
	 * @param protocol the protocol to speak
	 * @throws IOException if the port cannot be opened
	 */
	public TestProxyServer(Protocol protocol) throws IOException {
		this(protocol, null, null);
	}

	/**
	 * Starts a proxy on a free loopback port. If {@code username} is not {@code null}, clients
	 * must authenticate: with the user ID for SOCKS4, and with the username and password for
	 * SOCKS5 and HTTP.
	 *
	 * @param protocol the protocol to speak
	 * @param username the required username or user ID, or {@code null} for no authentication
	 * @param password the required password; ignored for SOCKS4
	 * @throws IOException if the port cannot be opened
	 */
	public TestProxyServer(Protocol protocol, String username, String password) throws IOException {
		this.protocol = protocol;
		this.username = username;
		this.password = password;
		this.serverSocket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
		Thread acceptor = new Thread(this::acceptLoop, "test-proxy-accept");
		acceptor.setDaemon(true);
		acceptor.start();
	}

	/**
	 * Returns the port the proxy listens on.
	 *
	 * @return the local port
	 */
	public int getPort() {
		return serverSocket.getLocalPort();
	}

	/**
	 * Returns the destination, in {@code host:port} form, of every tunnel opened so far.
	 *
	 * @return a copy of the tunnel destinations, in order
	 */
	public List<String> getTunnels() {
		synchronized (tunnels) {
			return new ArrayList<>(tunnels);
		}
	}

	/**
	 * Returns the usernames of logins that were refused.
	 *
	 * @return a copy of the refused usernames, in order
	 */
	public List<String> getRejectedLogins() {
		synchronized (rejectedLogins) {
			return new ArrayList<>(rejectedLogins);
		}
	}

	/**
	 * Stops listening and closes every relayed connection.
	 */
	@Override
	public void close() {
		closeQuietly(serverSocket);
		synchronized (openSockets) {
			for (Socket socket : openSockets) {
				closeQuietly(socket);
			}
			openSockets.clear();
		}
	}

	/**
	 * Accepts connections until the proxy is closed.
	 */
	private void acceptLoop() {
		while (!serverSocket.isClosed()) {
			try {
				Socket client = serverSocket.accept();
				openSockets.add(client);
				Thread t = new Thread(() -> handle(client), "test-proxy-client");
				t.setDaemon(true);
				t.start();
			} catch (IOException e) {
				return;   // closed
			}
		}
	}

	/**
	 * Runs the proxy handshake for one client connection, then relays it.
	 *
	 * @param client the client connection
	 */
	private void handle(Socket client) {
		try {
			DataInputStream in = new DataInputStream(client.getInputStream());
			OutputStream out = client.getOutputStream();
			Socket target;
			try {
				switch (protocol) {
					case SOCKS4:
						target = socks4(in, out);
						break;
					case SOCKS5:
						target = socks5(in, out);
						break;
					default:
						target = httpConnect(in, out);
						break;
				}
			} catch (EOFException closedEarly) {
				target = null;   // connected and closed without a request
			}
			if (target == null) {
				closeQuietly(client);
				return;
			}
			relay(client, target);
		} catch (IOException e) {
			closeQuietly(client);
		}
	}

	/**
	 * Handles a SOCKS4 or SOCKS4a {@code CONNECT} request.
	 *
	 * @param in  the client's input
	 * @param out the client's output
	 * @return the connection to the destination, or {@code null} if the request was refused
	 * @throws IOException if the connection fails
	 */
	private Socket socks4(DataInputStream in, OutputStream out) throws IOException {
		// VN, CD, DSTPORT, DSTIP, USERID, NUL [, DOMAIN, NUL for SOCKS4a]
		int version = in.readUnsignedByte();
		if (version != 4) {
			throw new IOException("not SOCKS4: version " + version);
		}
		int cmd = in.readUnsignedByte();
		int port = in.readUnsignedShort();
		byte[] ip = new byte[4];
		in.readFully(ip);
		String userId = readNulTerminated(in);
		String host;
		if (ip[0] == 0 && ip[1] == 0 && ip[2] == 0 && ip[3] != 0) {
			host = readNulTerminated(in);   // SOCKS4a: the proxy resolves the name
		} else {
			host = InetAddress.getByAddress(ip).getHostAddress();
		}
		byte[] refused = {0, 0x5B, 0, 0, 0, 0, 0, 0};
		if (username != null && !username.equals(userId)) {
			rejectedLogins.add(userId);
			out.write(refused);
			out.flush();
			return null;
		}
		if (cmd != 1) {
			out.write(refused);
			out.flush();
			return null;
		}
		Socket target = connect(host, port);
		if (target == null) {
			out.write(refused);
			out.flush();
			return null;
		}
		out.write(new byte[]{0, 0x5A, 0, 0, 0, 0, 0, 0});   // request granted
		out.flush();
		return target;
	}

	/**
	 * Handles the SOCKS5 greeting, optional username/password sub-negotiation and
	 * {@code CONNECT} request.
	 *
	 * @param in  the client's input
	 * @param out the client's output
	 * @return the connection to the destination, or {@code null} if the request was refused
	 * @throws IOException if the connection fails
	 */
	private Socket socks5(DataInputStream in, OutputStream out) throws IOException {
		// Greeting: VER, NMETHODS, METHODS
		int version = in.readUnsignedByte();
		if (version != 5) {
			throw new IOException("not SOCKS5: version " + version);
		}
		byte[] methods = new byte[in.readUnsignedByte()];
		in.readFully(methods);
		int wanted = (username == null) ? 0x00 : 0x02;
		if (!contains(methods, wanted)) {
			out.write(new byte[]{5, (byte) 0xFF});
			out.flush();
			return null;
		}
		out.write(new byte[]{5, (byte) wanted});
		out.flush();

		// Username/password sub-negotiation (RFC 1929)
		if (wanted == 0x02) {
			in.readUnsignedByte();   // sub-negotiation version
			byte[] user = new byte[in.readUnsignedByte()];
			in.readFully(user);
			byte[] pass = new byte[in.readUnsignedByte()];
			in.readFully(pass);
			String u = new String(user, StandardCharsets.UTF_8);
			String p = new String(pass, StandardCharsets.UTF_8);
			boolean ok = username.equals(u) && password.equals(p);
			out.write(new byte[]{1, (byte) (ok ? 0 : 1)});
			out.flush();
			if (!ok) {
				rejectedLogins.add(u);
				return null;
			}
		}

		// Request: VER, CMD, RSV, ATYP, DST.ADDR, DST.PORT
		in.readUnsignedByte();
		int cmd = in.readUnsignedByte();
		in.readUnsignedByte();
		int atyp = in.readUnsignedByte();
		String host;
		if (atyp == 1) {
			byte[] addr = new byte[4];
			in.readFully(addr);
			host = InetAddress.getByAddress(addr).getHostAddress();
		} else if (atyp == 4) {
			byte[] addr = new byte[16];
			in.readFully(addr);
			host = InetAddress.getByAddress(addr).getHostAddress();
		} else if (atyp == 3) {
			byte[] name = new byte[in.readUnsignedByte()];
			in.readFully(name);
			host = new String(name, StandardCharsets.US_ASCII);
		} else {
			throw new IOException("bad address type " + atyp);
		}
		int port = in.readUnsignedShort();
		if (cmd != 1) {
			out.write(new byte[]{5, 7, 0, 1, 0, 0, 0, 0, 0, 0});   // command not supported
			out.flush();
			return null;
		}
		Socket target = connect(host, port);
		if (target == null) {
			out.write(new byte[]{5, 5, 0, 1, 0, 0, 0, 0, 0, 0});   // connection refused
			out.flush();
			return null;
		}
		out.write(new byte[]{5, 0, 0, 1, 0, 0, 0, 0, 0, 0});        // succeeded
		out.flush();
		return target;
	}

	/**
	 * Handles an HTTP {@code CONNECT} request, checking {@code Proxy-Authorization} if
	 * authentication is required.
	 *
	 * @param in  the client's input
	 * @param out the client's output
	 * @return the connection to the destination, or {@code null} if the request was refused
	 * @throws IOException if the connection fails
	 */
	private Socket httpConnect(DataInputStream in, OutputStream out) throws IOException {
		String requestLine = readHttpLine(in);
		String[] parts = requestLine.split(" ");
		String authorization = null;
		for (String line = readHttpLine(in); !line.isEmpty(); line = readHttpLine(in)) {
			int colon = line.indexOf(':');
			if (colon > 0 && line.substring(0, colon).trim().equalsIgnoreCase("Proxy-Authorization")) {
				authorization = line.substring(colon + 1).trim();
			}
		}
		if (parts.length < 3 || !parts[0].equals("CONNECT")) {
			writeHttp(out, "405 Method Not Allowed");
			return null;
		}
		if (username != null) {
			String user = basicUser(authorization);
			String expected = "Basic " + Base64.getEncoder().encodeToString(
					(username + ":" + password).getBytes(StandardCharsets.UTF_8));
			if (!expected.equals(authorization)) {
				rejectedLogins.add(user);
				writeHttp(out, "407 Proxy Authentication Required\r\nProxy-Authenticate: Basic realm=\"test\"");
				return null;
			}
		}
		String authority = parts[1];
		int colon = authority.lastIndexOf(':');
		String host = authority.substring(0, colon);
		if (host.startsWith("[") && host.endsWith("]")) {
			host = host.substring(1, host.length() - 1);
		}
		int port = Integer.parseInt(authority.substring(colon + 1));
		Socket target = connect(host, port);
		if (target == null) {
			writeHttp(out, "502 Bad Gateway");
			return null;
		}
		writeHttp(out, "200 Connection established");
		return target;
	}

	/**
	 * Opens a connection to a tunnel destination and records it.
	 *
	 * @param host the destination host
	 * @param port the destination port
	 * @return the connection, or {@code null} if it could not be opened
	 */
	private Socket connect(String host, int port) {
		try {
			Socket target = new Socket(host, port);
			openSockets.add(target);
			tunnels.add(host + ":" + port);
			return target;
		} catch (IOException e) {
			return null;
		}
	}

	/**
	 * Copies bytes in both directions between two sockets until either side closes.
	 *
	 * @param client the client connection
	 * @param target the connection to the destination
	 * @throws IOException if a stream cannot be opened
	 */
	private void relay(Socket client, Socket target) throws IOException {
		InputStream fromClient = client.getInputStream();
		OutputStream toTarget = target.getOutputStream();
		Thread upstream = new Thread(() -> pump(fromClient, toTarget, client, target), "test-proxy-up");
		upstream.setDaemon(true);
		upstream.start();
		pump(target.getInputStream(), client.getOutputStream(), client, target);
	}

	/**
	 * Copies bytes from one stream to another, closing both sockets when the input ends.
	 *
	 * @param from   the stream to read
	 * @param to     the stream to write
	 * @param client the client connection
	 * @param target the connection to the destination
	 */
	private void pump(InputStream from, OutputStream to, Socket client, Socket target) {
		byte[] buffer = new byte[8192];
		try {
			int n;
			while ((n = from.read(buffer)) != -1) {
				to.write(buffer, 0, n);
				to.flush();
			}
		} catch (SocketException ignored) {
			// one side closed
		} catch (IOException ignored) {
			// one side failed
		} finally {
			closeQuietly(client);
			closeQuietly(target);
		}
	}

	/**
	 * Reads a NUL-terminated ASCII string, as used by SOCKS4.
	 *
	 * @param in the input
	 * @return the string, without the terminator
	 * @throws IOException if the input ends first
	 */
	private static String readNulTerminated(DataInputStream in) throws IOException {
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		for (int b = in.readUnsignedByte(); b != 0; b = in.readUnsignedByte()) {
			bytes.write(b);
		}
		return new String(bytes.toByteArray(), StandardCharsets.US_ASCII);
	}

	/**
	 * Reads one CRLF-terminated line of an HTTP request, one byte at a time so that nothing
	 * after the headers is consumed.
	 *
	 * @param in the input
	 * @return the line, without the terminator
	 * @throws IOException if the input ends first
	 */
	private static String readHttpLine(DataInputStream in) throws IOException {
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		int previous = -1;
		while (true) {
			int b = in.readUnsignedByte();
			if (previous == '\r' && b == '\n') {
				byte[] line = bytes.toByteArray();
				return new String(line, 0, line.length - 1, StandardCharsets.ISO_8859_1);
			}
			bytes.write(b);
			previous = b;
		}
	}

	/**
	 * Writes an HTTP response with no body.
	 *
	 * @param out              the output
	 * @param statusAndHeaders the status code and reason, optionally followed by CRLF-separated
	 *                         headers
	 * @throws IOException if the write fails
	 */
	private static void writeHttp(OutputStream out, String statusAndHeaders) throws IOException {
		String response = "HTTP/1.1 " + statusAndHeaders + "\r\nContent-Length: 0\r\n\r\n";
		out.write(response.getBytes(StandardCharsets.ISO_8859_1));
		out.flush();
	}

	/**
	 * Returns the username in a {@code Basic} {@code Proxy-Authorization} value.
	 *
	 * @param authorization the header value, or {@code null}
	 * @return the username, or {@code null} if there is none
	 */
	private static String basicUser(String authorization) {
		if (authorization == null || !authorization.toLowerCase(Locale.ROOT).startsWith("basic ")) {
			return null;
		}
		try {
			String decoded = new String(Base64.getDecoder().decode(authorization.substring(6).trim()),
					StandardCharsets.UTF_8);
			int colon = decoded.indexOf(':');
			return colon >= 0 ? decoded.substring(0, colon) : decoded;
		} catch (IllegalArgumentException e) {
			return null;
		}
	}

	/**
	 * Returns whether an array contains a byte value.
	 *
	 * @param values the values
	 * @param value  the value to look for
	 * @return {@code true} if present
	 */
	private static boolean contains(byte[] values, int value) {
		for (byte b : values) {
			if ((b & 0xFF) == value) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Closes a socket, ignoring errors.
	 *
	 * @param closeable the socket, or {@code null}
	 */
	private static void closeQuietly(AutoCloseable closeable) {
		if (closeable == null) {
			return;
		}
		try {
			closeable.close();
		} catch (Exception ignored) {
			// already closed
		}
	}
}
