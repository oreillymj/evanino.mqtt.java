/*******************************************************************************
 * From Eclipse Paho's Java client (org.eclipse.paho.client.mqttv3.ExceptionHelper), with changes.
 * The original notice follows. This project uses the Paho code under the Eclipse
 * Distribution License v1.0 (see licenses/EDL-1.0.txt).
 *
 *   Copyright (c) 2009, 2014 IBM Corp.
 *
 *   All rights reserved. This program and the accompanying materials
 *   are made available under the terms of the Eclipse Public License v2.0
 *   and Eclipse Distribution License v1.0 which accompany this distribution.
 *
 *   The Eclipse Public License is available at
 *      https://www.eclipse.org/legal/epl-2.0
 *   and the Eclipse Distribution License is available at
 *     https://www.eclipse.org/org/documents/edl-v10.php
 *
 *   Contributors:
 *      Dave Locke - initial API and implementation and/or initial documentation
 *
 * Changes copyright (c) 2026 Michael OReilly, under the Apache License 2.0 (see LICENSE).
 *******************************************************************************/

package io.github.oreillymj.evanino.mqtt;

/**
 * Utility class to help create exceptions of the correct type.
 */
public class ExceptionHelper {
    /**
     * Creates an exception for a reason code. Authentication and authorization failures
     * produce an {@link MqttSecurityException}; all other codes produce an {@link MqttException}.
     *
     * @param reasonCode the reason code
     * @return a new exception carrying the reason code
     */
    public static MqttException createMqttException(int reasonCode) {
        if ((reasonCode == MqttException.REASON_CODE_FAILED_AUTHENTICATION) ||
                (reasonCode == MqttException.REASON_CODE_NOT_AUTHORIZED)) {
            return new MqttSecurityException(reasonCode);
        }

        return new MqttException(reasonCode);
    }

    /**
     * Creates an exception wrapping a cause. A {@code java.security.GeneralSecurityException}
     * (exactly that class, not a subclass) produces an {@link MqttSecurityException}; anything
     * else produces an {@link MqttException}.
     *
     * @param cause the underlying failure
     * @return a new exception wrapping the cause
     */
    public static MqttException createMqttException(Throwable cause) {
        if (cause.getClass().getName().equals("java.security.GeneralSecurityException")) {
            return new MqttSecurityException(cause);
        }
        return new MqttException(cause);
    }


    /**
     * Returns whether or not the specified class is available to the current
     * class loader.  This is used to protect the code against using Java SE
     * APIs on Java ME.
     * @param className The name of the class
     * @return If true, the class is available
     */
    public static boolean isClassAvailable(String className) {
        boolean result = false;
        try {
            Class.forName(className);
            result = true;
        }
        catch (ClassNotFoundException ex) {
        }
        return result;
    }

    // Utility classes should not have a public or default constructor.
    /**
     * Prevents instantiation; this class only holds static helpers.
     */
    private ExceptionHelper() {
    }
}