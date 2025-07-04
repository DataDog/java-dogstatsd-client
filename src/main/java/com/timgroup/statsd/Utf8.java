/*
 * Copyright (C) 2013 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.timgroup.statsd;

import static java.lang.Character.MAX_SURROGATE;
import static java.lang.Character.MIN_SURROGATE;

import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

/**
 * This class is a partial copy of the {@code com.google.common.base.Utf8}
 * <a href="https://github.com/google/guava/blob/v33.0.0/guava/src/com/google/common/base/Utf8.java">class</a>
 * from the Guava library.
 * It is copied here to avoid a dependency on Guava.
 */
final class Utf8 {

    private static final int UTF8_REPLACEMENT_LENGTH = StandardCharsets.UTF_8.newEncoder().replacement().length;

    private Utf8() {
    }

    /**
     * Returns the number of bytes in the UTF-8-encoded form of {@code sequence}. For a string, this
     * method is equivalent to {@code string.getBytes(UTF_8).length}, but is more efficient in both
     * time and space.
     *
     * @throws IllegalArgumentException if {@code sequence} contains ill-formed UTF-16 (unpaired
     *                                  surrogates)
     */
    public static int encodedLength(CharSequence sequence) {
        // Warning to maintainers: this implementation is highly optimized.
        int utf16Length = sequence.length();
        int utf8Length = utf16Length;
        int index = 0;

        // This loop optimizes for pure ASCII.
        while (index < utf16Length && sequence.charAt(index) < 0x80) {
            index++;
        }

        // This loop optimizes for chars less than 0x800.
        for (; index < utf16Length; index++) {
            char character = sequence.charAt(index);
            if (character < 0x800) {
                utf8Length += ((0x7f - character) >>> 31); // branch free!
            } else {
                utf8Length += encodedLengthGeneral(sequence, index);
                break;
            }
        }

        if (utf8Length < utf16Length) {
            // Necessary and sufficient condition for overflow because of maximum 3x expansion
            throw new IllegalArgumentException(
                    "UTF-8 length does not fit in int: " + (utf8Length + (1L << 32)));
        }
        return utf8Length;
    }

    private static int encodedLengthGeneral(CharSequence sequence, int start) {
        int utf16Length = sequence.length();
        int utf8Length = 0;
        for (int index = start; index < utf16Length; index++) {
            char character = sequence.charAt(index);
            if (character < 0x800) {
                utf8Length += (0x7f - character) >>> 31; // branch free!
            } else {
                utf8Length += 2;
                // jdk7+: if (Character.isSurrogate(character)) {
                if (MIN_SURROGATE <= character && character <= MAX_SURROGATE) {
                    // Check that we have a well-formed surrogate pair.
                    if (Character.codePointAt(sequence, index) == character) {
                        // Bad input so deduct char length and account for the replacement characters
                        utf8Length += -2 + UTF8_REPLACEMENT_LENGTH - 1;
                    } else {
                        index++;
                    }
                }
            }
        }
        return utf8Length;
    }

    static String sanitize(String str) {
        if (str == null) {
            return null;
        }
        return Pattern.compile("[|\\p{Cntrl}]").matcher(str).replaceAll("");
    }
}
