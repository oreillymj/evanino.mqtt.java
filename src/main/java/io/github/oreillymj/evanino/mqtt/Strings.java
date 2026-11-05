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
 * Small string helpers mirroring Apache Commons Lang's {@code StringUtils}, to avoid the
 * dependency.
 */
final class Strings {

    /**
     * Prevents instantiation; this class only holds static helpers.
     */
    private Strings() {}

    /**
     * Checks if the given CharSequence equals any of the search CharSequences,
     * case-sensitively. Mirrors Apache Commons Lang's StringUtils.equalsAny.
     *
     * @param string        the value to compare
     * @param searchStrings the candidates to compare against; {@code null} entries are skipped
     * @return {@code true} if {@code string} equals any candidate; {@code false} if either
     *         argument is {@code null}
     */
    public static boolean equalsAny(final CharSequence string, final CharSequence... searchStrings) {
        if (string == null || searchStrings == null) {
            return false;
        }
        for (CharSequence next : searchStrings) {
            if (next != null && next.toString().contentEquals(string)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Counts non-overlapping occurrences of a substring within a string.
     * Mirrors Apache Commons Lang's StringUtils.countMatches.
     *
     * @param str the string to search
     * @param sub the substring to count
     * @return the number of occurrences, or {@code 0} if either argument is {@code null} or empty
     */
    public static int countMatches(final CharSequence str, final CharSequence sub) {
        if (isEmpty(str) || isEmpty(sub)) {
            return 0;
        }
        int count = 0;
        int idx = 0;
        String s = str.toString();
        String target = sub.toString();
        while ((idx = s.indexOf(target, idx)) != -1) {
            count++;
            idx += target.length();
        }
        return count;
    }

    /**
     * Checks whether a CharSequence is {@code null} or has length zero.
     *
     * @param cs the value to check
     * @return {@code true} if {@code cs} is {@code null} or empty
     */
    private static boolean isEmpty(final CharSequence cs) {
        return cs == null || cs.length() == 0;
    }

    /**
     * Checks if the CharSequence contains any of the given search CharSequences,
     * case-sensitively. Mirrors Apache Commons Lang's StringUtils.containsAny.
     *
     * @param string        the value to search
     * @param searchStrings the substrings to look for; {@code null} and empty entries are skipped
     * @return {@code true} if {@code string} contains any of the substrings; {@code false} if
     *         either argument is {@code null}
     */
    public static boolean containsAny(final CharSequence string, final CharSequence... searchStrings) {
        if (string == null || searchStrings == null) {
            return false;
        }
        String s = string.toString();
        for (CharSequence next : searchStrings) {
            if (next != null && next.length() > 0 && s.contains(next)) {
                return true;
            }
        }
        return false;
    }
}