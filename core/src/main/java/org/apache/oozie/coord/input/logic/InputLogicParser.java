/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.oozie.coord.input.logic;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.jdom.Element;
import org.jdom.Namespace;

/**
 * Parses xml into jexl expression
 */
public class InputLogicParser {

    public final static String COORD_INPUT_EVENTS_DATA_IN = "data-in";

    public final static String AND = "and";

    public final static String OR = "or";

    public final static String COMBINE = "combine";

    /**
     * Parses the xml.
     *
     * @param root the root
     * @return the string
     */
    public String parse(Element root) {
        return parseWithName(root, null);

    }

    /**
     * Parses the xml with name.
     *
     * @param root the root
     * @param name the name
     * @return the string
     */
    @SuppressWarnings("unchecked")
    public String parseWithName(Element root, String name) {
        if (root == null) {
            return "";
        }
        StringBuffer parsedString = new StringBuffer();

        List<Element> childrens = root.getChildren();
        for (int i = 0; i < childrens.size(); i++) {
            String childName = childrens.get(i).getAttributeValue("name");
            String min = childrens.get(i).getAttributeValue("min");
            String wait = childrens.get(i).getAttributeValue("wait");

            if (name == null || name.equals(childName)) {
                parsedString.append(parse(childrens.get(i), getOpt(childrens.get(i).getName()), min, wait));
            }
            else {
                parsedString.append(parseWithName(childrens.get(i), name));
            }
        }
        return parsedString.toString();
    }

    public String parse(Element root, String opt, String min, String wait) {
        StringBuffer parsedString = new StringBuffer();

        Namespace ns = root.getNamespace();
        if (root.getName().equals(COMBINE)) {
            parsedString.append("(");
            parsedString.append(processCombinedNode(root, getOpt(root.getName()), getMin(root, min),
                    getWait(root, wait)));
            parsedString.append(")");
        }
        else if (root.getName().equals(AND) || root.getName().equals(OR)) {
            parsedString.append("(");
            parsedString.append(parseAllChildren(root, opt, getOpt(root.getName()), getMin(root, min),
                    getWait(root, wait)));
            parsedString.append(")");

        }
        else if (root.getChild(COORD_INPUT_EVENTS_DATA_IN, ns) != null) {
            parsedString.append("(");
            parsedString.append(processChildNode(root, getOpt(root.getName()), getMin(root, min), getWait(root, wait)));
            parsedString.append(")");
        }
        else if (root.getName().equals(COORD_INPUT_EVENTS_DATA_IN)) {
            parsedString.append(parseDataInNode(root, min, wait));

        }
        return parsedString.toString();

    }

    /**
     * Parses the all children.
     *
     * @param root the root
     * @param parentOpt the parent opt
     * @param opt the opt
     * @param min the min
     * @param wait the wait
     * @return the string
     */
    @SuppressWarnings("unchecked")
    private String parseAllChildren(Element root, String parentOpt, String opt, String min, String wait) {
        StringBuffer parsedString = new StringBuffer();

        List<Element> childrens = root.getChildren();
        for (int i = 0; i < childrens.size(); i++) {
            String currentMin = min;
            String currentWait = wait;
            String childMin = childrens.get(i).getAttributeValue("min");
            String childWait = childrens.get(i).getAttributeValue("wait");
            if (!StringUtils.isEmpty(childMin)) {
                currentMin = childMin;
            }
            if (!StringUtils.isEmpty(childWait)) {
                currentWait = childWait;
            }
            parsedString.append(parse(childrens.get(i), opt, currentMin, currentWait));
            if (i < childrens.size() - 1) {
                if (!StringUtils.isEmpty(opt))
                    parsedString.append(" " + opt + " ");
            }
        }
        return parsedString.toString();

    }

    /**
     * Parses the data in node.
     *
     * @param root the root
     * @param min the min
     * @param wait the wait
     * @return the string
     */
    private String parseDataInNode(Element root, String min, String wait) {
        StringBuffer parsedString = new StringBuffer();

        String nestedChildDataName = root.getAttributeValue("dataset");

        parsedString.append("dependencyBuilder.input(\"" + nestedChildDataName + "\")");
        appendMin(root, min, parsedString);
        appendWait(root, wait, parsedString);
        parsedString.append(".build()");
        return parsedString.toString();
    }

    /**
     * Process child node.
     *
     * @param root the root
     * @param opt the opt
     * @param min the min
     * @param wait the wait
     * @return the string
     */
    @SuppressWarnings("unchecked")
    private String processChildNode(final Element root, final String opt, final String min, final String wait) {
        StringBuffer parsedString = new StringBuffer();

        Namespace ns = root.getNamespace();

        List<Element> childrens = root.getChildren(COORD_INPUT_EVENTS_DATA_IN, ns);

        for (int i = 0; i < childrens.size(); i++) {
            parsedString.append(parseDataInNode(childrens.get(i), min, wait));

            if (i < childrens.size() - 1) {
                parsedString.append(" " + opt + " ");
            }
        }
        return parsedString.toString();
    }

    /**
     * Process combined node.
     *
     * @param root the root
     * @param opt the opt
     * @param min the min
     * @param wait the wait
     * @return the string
     */
    @SuppressWarnings("unchecked")
    private String processCombinedNode(final Element root, final String opt, final String min, final String wait) {
        StringBuffer parsedString = new StringBuffer();

        Namespace ns = root.getNamespace();

        List<Element> childrens = root.getChildren(COORD_INPUT_EVENTS_DATA_IN, ns);
        parsedString.append("dependencyBuilder.combine(");

        for (int i = 0; i < childrens.size(); i++) {
            String nestedChildDataName = childrens.get(i).getAttributeValue("dataset");
            parsedString.append("\"" + nestedChildDataName + "\"");
            if (i < childrens.size() - 1) {
                parsedString.append(",");
            }
        }
        parsedString.append(")");

        appendMin(root, min, parsedString);
        appendWait(root, wait, parsedString);
        parsedString.append(".build()");
        return parsedString.toString();

    }

    /**
     * Gets the opt.
     *
     * @param opt the opt
     * @return the opt
     */
    private String getOpt(String opt) {
        if (opt.equalsIgnoreCase("or")) {
            return "||";
        }

        if (opt.equalsIgnoreCase("and")) {
            return "&&";
        }

        return "";

    }

    /**
     * Gets the min.
     *
     * @param root the root
     * @param parentMin the parent min
     * @return the min
     */
    private String getMin(Element root, String parentMin) {
        String min = root.getAttributeValue("min");
        if (StringUtils.isEmpty(min)) {
            return parentMin;
        }
        return min;

    }

    /**
     * Gets the wait.
     *
     * @param root the root
     * @param parentWait the parent wait
     * @return the wait
     */
    private String getWait(Element root, String parentWait) {
        String wait = root.getAttributeValue("wait");
        if (StringUtils.isEmpty(parentWait)) {
            return parentWait;
        }
        return wait;

    }

    private void appendWait(final Element root, String wait, StringBuffer parsedString) {
        String childWait = root.getAttributeValue("wait");
        if (!StringUtils.isEmpty(childWait)) {
            parsedString.append(".inputWait(" + childWait + ")");

        }
        else {
            if (!StringUtils.isEmpty(wait)) {
                parsedString.append(".inputWait(" + wait + ")");

            }
        }

    }

    private void appendMin(final Element root, String min, StringBuffer parsedString) {
        String childMin = root.getAttributeValue("min");

        if (!StringUtils.isEmpty(childMin)) {
            parsedString.append(".min(" + childMin + ")");

        }
        else {
            if (!StringUtils.isEmpty(min)) {
                parsedString.append(".min(" + min + ")");

            }
        }
    }

}
