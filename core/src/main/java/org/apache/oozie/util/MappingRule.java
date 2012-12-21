package org.apache.oozie.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class for rule mapping
 */
public class MappingRule {

    private static Pattern variableNamePattern = Pattern.compile("\\$\\{[0-9]\\}");
    private Pattern fromPattern;
    private String fromString;
    private String toString;
    private boolean patternMatch;

    /**
     * Maps from source rule to destination rule
     * @param fromRule - Rule for which input needs to be matched
     * @param toRule - Rule for value to be returned
     */
    public MappingRule(String fromRule, String toRule) {
        if (fromRule.contains("$")) {
            patternMatch = true;
            fromRule = fromRule.replaceAll("\\.", "\\\\.");
            Matcher match = variableNamePattern.matcher(fromRule);
            fromRule = match.replaceAll("(.*)");
            fromPattern = Pattern.compile(fromRule);
        }
        else {
            fromString = fromRule;
        }
        toString = toRule;
    }

    /**
     * Gets the from rule
     * @return
     */
    public String getFromRule() {
        return fromString;
    }

    /**
     * Gets the to rule
     * @return
     */
    public String getToRule() {
        return toString;
    }

    /**
     * Applies rules based on the input
     * @param input
     * @return
     */
    public String applyRule(String input) {
        if (patternMatch) {
            Matcher match = fromPattern.matcher(input);
            if (match.matches()) {
                String result = toString;
                int count = match.groupCount();
                for (int i = 1; i <= count; i++) {
                    result = result.replace("${" + (i) + "}", match.group(i));
                }
                return result;
            }
        }
        else if (input.equals(fromString)) {
            return toString;
        }
        return null;
    }
}
