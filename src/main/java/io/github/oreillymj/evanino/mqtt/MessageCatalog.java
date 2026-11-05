/*******************************************************************************
 * From Eclipse Paho's Java client (org.eclipse.paho.client.mqttv3.MessageCatalog), with changes.
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
 * Catalog of human readable error messages, carried over from Paho.
 * <p>
 * The concrete catalog is loaded reflectively from Paho's
 * {@code org.eclipse.paho.client.mqttv3.internal} package, so messages are only available
 * when the Paho client library is on the classpath.
 */
public abstract class MessageCatalog {
    private static MessageCatalog INSTANCE = null;

    /**
     * Returns the human readable message for an MQTT reason code, loading the catalog on
     * first use.
     *
     * @param id the reason code to look up
     * @return the localized message, or an empty string if the catalog cannot be loaded
     */
    public static final String getMessage(int id) {
        if (INSTANCE == null) {
            if (ExceptionHelper.isClassAvailable("java.util.ResourceBundle")) {
                try {
                    // Hide this class reference behind reflection so that the class does not need to
                    // be present when compiled on midp
                    INSTANCE = (MessageCatalog)Class.forName("org.eclipse.paho.client.mqttv3.internal.ResourceBundleCatalog").newInstance();
                } catch (Exception e) {
                    return MqttExceptionReasons.getMessage(id);
                }
            } else if (ExceptionHelper.isClassAvailable("org.eclipse.paho.client.mqttv3.internal.MIDPCatalog")){
                try {
                    INSTANCE = (MessageCatalog)Class.forName("org.eclipse.paho.client.mqttv3.internal.MIDPCatalog").newInstance();
                } catch (Exception e) {
                    return MqttExceptionReasons.getMessage(id);
                }
            }
        }
        if (INSTANCE == null) {
            return MqttExceptionReasons.getMessage(id);
        }
        return INSTANCE.getLocalizedMessage(id);
    }

    /**
     * Looks up the message for a reason code in this catalog.
     *
     * @param id the reason code to look up
     * @return the localized message
     */
    protected abstract String getLocalizedMessage(int id);
}