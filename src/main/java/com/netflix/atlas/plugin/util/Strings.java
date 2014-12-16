/*
 * Copyright 2014 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.atlas.plugin.util;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

/**
 * Utility class providing String related methods.
 */
public final class Strings {
    private static final int ASCII_LIMIT = 128;

    private static final String[] URI_ESCAPES = new String[ASCII_LIMIT];
    private static final int SHORT_LIST = 3;

    private Strings() {
    }

    private static String hex(int c) {
        return String.format("%%%02X", c);
    }
    static {
        for (int pos = 0; pos < URI_ESCAPES.length; ++pos) {
            URI_ESCAPES[pos] =
                    Character.isISOControl((char) pos) ? hex(pos) : String.valueOf((char) pos);
        }
        URI_ESCAPES[' '] = hex(' ');
        URI_ESCAPES['+'] = hex('+');
        URI_ESCAPES['#'] = hex('#');
        URI_ESCAPES['"'] = hex('"');
        URI_ESCAPES['%'] = hex('%');
        URI_ESCAPES['&'] = hex('&');
        URI_ESCAPES[';'] = hex(';');
        URI_ESCAPES['<'] = hex('<');
        URI_ESCAPES['='] = hex('=');
        URI_ESCAPES['>'] = hex('>');
        URI_ESCAPES['?'] = hex('?');
        URI_ESCAPES['['] = hex('[');
        URI_ESCAPES['\\'] = hex('\\');
        URI_ESCAPES[']'] = hex(']');
        URI_ESCAPES['^'] = hex('^');
        URI_ESCAPES['{'] = hex('{');
        URI_ESCAPES['|'] = hex('|');
        URI_ESCAPES['{'] = hex('{');
    }

    public static boolean isNotEmpty(String s) {
        return s != null && !s.isEmpty();
    }

    /**
     * Lenient url-encoder. The URLEncoder class provided in the jdk is eager to
     * percent encode making atlas expressions hard to read. This version assumes
     * the only escaping necessary for '%', '&amp;', '+', '?', '=', and ' '.
     */
    public static String urlEncode(String s) throws UnsupportedEncodingException {
        StringBuilder buf = new StringBuilder();
        int size = s.length();
        for (int pos = 0; pos < size; ++pos) {
            int c = s.charAt(pos);
            if (c < ASCII_LIMIT) {
                buf.append(URI_ESCAPES[c]);
            } else {
                buf.append(URLEncoder.encode(String.valueOf((char) c), "UTF-8"));
            }
        }
        return buf.toString();
    }

    /**
     * URL encode params.
     */
    public static String encodeParams(SortedMap<String, String> params) {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            try {
                builder.append(urlEncode(entry.getKey()));
                builder.append('=');
                builder.append(urlEncode(entry.getValue()));
                builder.append('&');
            } catch (UnsupportedEncodingException e) {
                throw Throwables.propagate(e);
            }
        }
        if (builder.length() > 0) {
            builder.deleteCharAt(builder.length() - 1);
        }
        return builder.toString();
    }

    /**
     * Create a string representation of a list. Lists longer than 3 entries are truncated.
     */
    public static String shortList(List<String> list) {
        StringBuilder buf = new StringBuilder();
        buf.append('[');

        if (list.size() > SHORT_LIST) {
            for (int i = 0; i < SHORT_LIST; ++i) {
                buf.append(list.get(i)).append(", ");
            }
            buf.append("and ");
            buf.append(list.size() - SHORT_LIST);
            buf.append(" others");
        } else {
            Joiner.on(", ").appendTo(buf, list);
        }
        buf.append(']');
        return buf.toString();
    }
}
