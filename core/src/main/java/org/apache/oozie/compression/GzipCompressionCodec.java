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

package org.apache.oozie.compression;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.io.IOUtils;

/**
 * Class to compress and decompress data using Gzip codec
 *
 */
public class GzipCompressionCodec implements CompressionCodec {

    public static final String CODEC_NAME = "gz";

    public byte[] compressBytes(byte[] header, byte[] data) throws IOException {
        ByteArrayOutputStream byteOutput = new ByteArrayOutputStream(2000);
        byteOutput.write(header);
        GZIPOutputStream gzipOut = new GZIPOutputStream(byteOutput);
        gzipOut.write(data);
        gzipOut.close();
        return byteOutput.toByteArray();
    }

    public byte[] compressString(byte[] header, String data) throws IOException {
        ByteArrayOutputStream byteOutput = new ByteArrayOutputStream(1000);
        byteOutput.write(header);
        GZIPOutputStream gzipOut = new GZIPOutputStream(byteOutput);
        gzipOut.write(data.getBytes(CodecFactory.UTF_8_ENCODING));
        gzipOut.close();
        return byteOutput.toByteArray();
    }

    public String decompressToString(DataInputStream dais) throws IOException {
        GZIPInputStream gzipIn = new GZIPInputStream(dais);
        String decompress = IOUtils.toString(gzipIn, CodecFactory.UTF_8_ENCODING);
        gzipIn.close();
        return decompress;
    }

    public byte[] decompressToBytes(DataInputStream dais) throws IOException {
        GZIPInputStream gzipIn = new GZIPInputStream(dais);
        byte[] decompress = IOUtils.toByteArray(gzipIn);
        gzipIn.close();
        return decompress;
    }

}
