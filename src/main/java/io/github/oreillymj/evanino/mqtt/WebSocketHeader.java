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
 * Well-known HTTP headers that can be sent in the WebSocket upgrade request.
 */
public enum WebSocketHeader {
    /** The {@code Authorization} header. */
    AUTHORIZATION("Authorization"),
    /** The {@code X-API-Key} header. */
    API_KEY("X-API-Key"),
    /** The {@code Cookie} header. */
    COOKIE("Cookie"),
    /** The {@code Host} header. */
    HOST("Host"),
    /** The {@code User-Agent} header. */
    USER_AGENT("User-Agent"),
    /** The {@code X-Request-Id} header. */
    REQUEST_ID("X-Request-Id"),
    /** The {@code X-Correlation-Id} header. */
    CORRELATION_ID("X-Correlation-Id");

    private final String headerName;

    /**
     * Creates a header constant.
     *
     * @param headerName the header name as sent on the wire
     */
    WebSocketHeader(String headerName) {
        this.headerName = headerName;
    }

    /**
     * Returns the header name as sent on the wire.
     *
     * @return the HTTP header name
     */
    public String headerName() {
        return headerName;
    }
}