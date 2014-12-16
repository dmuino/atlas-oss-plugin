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

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This implements a simple templating mechanism:
 * $foo will get expanded to the value of the foo key,
 * or foo if one does not exist in the provided map
 * It also supports $(nf.fo/var) to replace the value of the nf.fo/var key.
 */
public final class Variables {
    private static final Pattern SIMPLE_VAR = Pattern.compile("\\$([-_.a-zA-Z0-9]+)");
    private static final Pattern PAREN_VAR = Pattern.compile("\\$\\(([^\\(\\)]+)\\)");
    private Variables() {
    }

    private static String replaceAllIn(Pattern pattern, String input, Map<String, String> vars) {
        final StringBuffer buf = new StringBuffer();
        final Matcher m = pattern.matcher(input);
        while (m.find()) {
            String var = m.group(1);
            String replacement = vars.get(var);
            if (replacement == null) {
                replacement = var;
            }
            m.appendReplacement(buf, replacement);
        }
        m.appendTail(buf);
        return buf.toString();
    }

    /**
     * Do a template expansion based on the input map 'vars'.
     */
    public static String substitute(String inputPattern, Map<String, String> vars) {
        String simple = replaceAllIn(SIMPLE_VAR, inputPattern, vars);
        return replaceAllIn(PAREN_VAR, simple, vars);
    }
}
