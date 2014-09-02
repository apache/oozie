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

package org.apache.oozie;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import org.apache.oozie.compression.CodecFactory;
import org.apache.oozie.compression.CompressionCodec;

/**
 * BinaryBlob to maintain compress and uncompressed data
 */
public class BinaryBlob {

    private byte[] rawBlob;
    private byte[] bytes;

    /**
     * Construct a binaryblob
     *
     * @param byteArray
     * @param isUncompressed - true if data is uncompressed
     */
    public BinaryBlob(byte[] byteArray, boolean isUncompressed) {
        if (isUncompressed) {
            this.bytes = byteArray;
            this.rawBlob = null;
        }
        else {
            this.rawBlob = byteArray;
        }
    }

    /**
     * Set bytes
     *
     * @param byteArray
     */
    public void setBytes(byte[] byteArray) {
        this.bytes = byteArray;
        this.rawBlob = null;
    }

    /**
     * Returns a decompressed byte array
     *
     * @return byte array
     */
    public byte[] getBytes() {
        if (bytes != null) {
            return bytes;
        }
        if (rawBlob == null) {
            return null;
        }
        try {
            DataInputStream dais = new DataInputStream(new ByteArrayInputStream(rawBlob));
            CompressionCodec codec = CodecFactory.getDeCompressionCodec(dais);
            if (codec != null) {
                bytes = codec.decompressToBytes(dais);
            }
            else {
                bytes = rawBlob;
            }
            dais.close();
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        rawBlob = null;
        return bytes;

    }

    /**
     * Returns a raw blob
     *
     * @return raw blob
     */
    public byte[] getRawBlob() {
        if (rawBlob != null) {
            return rawBlob;
        }
        if (bytes == null) {
            return null;
        }
        if (CodecFactory.isCompressionEnabled()) {
            byte[] headerBytes = CodecFactory.getHeaderBytes();
            try {
                rawBlob = CodecFactory.getCompressionCodec().compressBytes(headerBytes, bytes);
            }
            catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
        else {
            rawBlob = bytes;
        }
        return rawBlob;
    }

}
