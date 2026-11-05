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

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Minimal logger that writes timestamped lines to {@code System.out}.
 */
public class SimpleLogger {
    private boolean mLoggingEnabled=true;
    private String mTAG="";

    /**
     * Creates an enabled logger with no tag.
     */
    SimpleLogger() {

    }

    /**
     * Creates a logger with no tag.
     *
     * @param LoggingEnabled whether output is written
     */
    SimpleLogger(boolean LoggingEnabled) {
        mLoggingEnabled=LoggingEnabled;
    }

    /**
     * Creates an enabled logger whose lines are prefixed with a tag.
     *
     * @param TAG the prefix written before each message
     */
    SimpleLogger(String TAG) {
        mTAG=TAG;
    }

    /**
     * Creates a logger whose lines are prefixed with a tag.
     *
     * @param TAG            the prefix written before each message
     * @param LoggingEnabled whether output is written
     */
    SimpleLogger(String TAG, boolean LoggingEnabled) {
        mTAG=TAG;
        mLoggingEnabled=LoggingEnabled;
    }

    /**
     * Turns output on or off.
     *
     * @param enable {@code true} to write log lines, {@code false} to discard them
     */
    public void enableLogging(boolean enable){
        mLoggingEnabled=enable;
    }


    /**
     * Returns the current time formatted as used in log lines.
     *
     * @return the timestamp in {@code yyyy-MM-dd HH:mm:ss:SSS - } form
     */
    public String getDate(){
        return dt();
    }

    /**
     * Formats the current time for a log line, followed by a {@code " - "} separator.
     *
     * @return the formatted timestamp, or the epoch if formatting fails
     */
    private  String dt(){
        String strDate ="";
        try {
            SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");//dd/MM/yyyy
            Date now = new Date();
            strDate = sdfDate.format(now);
        }catch (Exception e){
            strDate="1970-01-01 00:00:00:000";
        }finally {
            return strDate + " - ";
        }
    }

    /**
     * Writes a message, prefixed with the timestamp and this logger's tag.
     *
     * @param info the message to write
     */
    public void log(String info){
        if (!mLoggingEnabled){return;}
        System.out.println(dt() + mTAG + info);
    }

    /**
     * Writes a message, prefixed with the timestamp and the given tag instead of this
     * logger's own tag.
     *
     * @param TAG  the prefix written before the message
     * @param info the message to write
     */
    public void log(String TAG,String info){
        if (!mLoggingEnabled){return;}
        System.out.println(dt()+ TAG + "->" + info);
    }
}
