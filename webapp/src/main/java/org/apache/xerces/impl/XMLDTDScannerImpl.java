/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.xerces.impl;

import java.io.IOException;

import org.apache.xerces.util.SymbolTable;
import org.apache.xerces.util.XMLChar;
import org.apache.xerces.util.XMLStringBuffer;
import org.apache.xerces.util.XMLSymbols;
import org.apache.xerces.xni.Augmentations;
import org.apache.xerces.xni.XMLDTDContentModelHandler;
import org.apache.xerces.xni.XMLDTDHandler;
import org.apache.xerces.xni.XMLLocator;
import org.apache.xerces.xni.XMLResourceIdentifier;
import org.apache.xerces.xni.XMLString;
import org.apache.xerces.xni.XNIException;
import org.apache.xerces.xni.parser.XMLComponent;
import org.apache.xerces.xni.parser.XMLComponentManager;
import org.apache.xerces.xni.parser.XMLConfigurationException;
import org.apache.xerces.xni.parser.XMLDTDScanner;
import org.apache.xerces.xni.parser.XMLInputSource;

public class XMLDTDScannerImpl extends XMLScanner implements XMLDTDScanner, XMLComponent, XMLEntityHandler {
    private static final String DTD_UNSUPPORTED_ERROR_MESSAGE = "DOCTYPE is disallowed when the feature " +
            "http://apache.org/xml/features/disallow-doctype-decl set to true.";

    public XMLDTDScannerImpl() {
    }

    public XMLDTDScannerImpl(SymbolTable symbolTable, XMLErrorReporter errorReporter, XMLEntityManager entityManager) {
    }

    public void setInputSource(XMLInputSource inputSource) throws IOException {
    }

    public boolean scanDTDExternalSubset(boolean complete) throws IOException, XNIException {
        throw new UnsupportedOperationException(DTD_UNSUPPORTED_ERROR_MESSAGE);
    }

    public boolean scanDTDInternalSubset(boolean complete, boolean standalone, boolean hasExternalSubset) throws IOException, XNIException {
        throw new UnsupportedOperationException(DTD_UNSUPPORTED_ERROR_MESSAGE);
    }

    public void reset(XMLComponentManager componentManager) throws XMLConfigurationException {
    }

    public void reset() {
    }

    public String[] getRecognizedFeatures() {
        return null;
    }

    public String[] getRecognizedProperties() {
        return null;
    }

    public Boolean getFeatureDefault(String featureId) {
        return null;
    }

    public Object getPropertyDefault(String propertyId) {
        return null;
    }

    public void setDTDHandler(XMLDTDHandler dtdHandler) {
    }

    public XMLDTDHandler getDTDHandler() {
        return null;
    }

    public void setDTDContentModelHandler(XMLDTDContentModelHandler dtdContentModelHandler) {
    }

    public XMLDTDContentModelHandler getDTDContentModelHandler() {
        return null;
    }

    public void startEntity(String name, XMLResourceIdentifier identifier, String encoding, Augmentations augs) throws XNIException {
    }

    public void endEntity(String name, Augmentations augs) throws XNIException {
    }


}
