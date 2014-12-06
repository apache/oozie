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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.BinaryBlob;
import org.apache.oozie.StringBlob;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestCodecFactory extends XTestCase {

    private Services services;

    @Override
    @Before
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.getConf().set(CodecFactory.COMPRESSION_OUTPUT_CODEC, GzipCompressionCodec.CODEC_NAME);
        services.init();
    }

    @Override
    @After
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    @Test
    public void testCompressionCodecs() throws IOException {
        assertTrue(CodecFactory.isCompressionEnabled());
        assertEquals(GzipCompressionCodec.class, CodecFactory.getCompressionCodec().getClass());
        byte[] bytes = CodecFactory.getHeaderBytes();
        DataInputStream dais = new DataInputStream(new ByteArrayInputStream(bytes));
        assertEquals(GzipCompressionCodec.class, CodecFactory.getDeCompressionCodec(dais).getClass());
        dais.close();
    }

    @Test
    public void testCompression() throws IOException {
        byte[] headerBytes = CodecFactory.getHeaderBytes();
        byte[] compressedBytes = CodecFactory.getCompressionCodec().compressString(headerBytes, "dummmmyyyy");
        DataInputStream dais = new DataInputStream(new ByteArrayInputStream(compressedBytes));
        String uncompressed = CodecFactory.getDeCompressionCodec(dais).decompressToString(dais);
        assertEquals("dummmmyyyy", uncompressed);
        dais.close();
    }

    @Test
    public void testCodecFactoryConf() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream daos = new DataOutputStream(baos);
        // magic data
        daos.write(CodecFactory.COMPRESSION_MAGIC_DATA.getBytes(CodecFactory.UTF_8_ENCODING));
        // version
        daos.writeInt(1);
        // no of key value pairs
        daos.writeInt(1);
        daos.writeUTF(CodecFactory.COMPRESSION_KEY_HEADER);
        daos.writeUTF(GzipCompressionCodec.CODEC_NAME);
        daos.close();
        assertEquals(new String(baos.toByteArray()), new String(CodecFactory.getHeaderBytes()));
        Configuration conf = services.getConf();
        conf.set(CodecFactory.COMPRESSION_OUTPUT_CODEC, "none");
        CodecFactory.initialize(conf);
        assertTrue(!CodecFactory.isCompressionEnabled());
        conf.set(CodecFactory.COMPRESSION_OUTPUT_CODEC, "");
        CodecFactory.initialize(conf);
        assertTrue(!CodecFactory.isCompressionEnabled());
        conf.set(CodecFactory.COMPRESSION_CODECS, "gz=" + GzipCompressionCodec.class.getName());
        CodecFactory.initialize(conf);
        conf.set(CodecFactory.COMPRESSION_OUTPUT_CODEC, "abcd");
        try {
            CodecFactory.initialize(conf);
        }
        catch (Exception e) {
            // expected
        }
    }
}
