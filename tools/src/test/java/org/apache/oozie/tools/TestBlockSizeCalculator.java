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

package org.apache.oozie.tools;

import junit.framework.TestCase;
import org.junit.Assert;

public class TestBlockSizeCalculator extends TestCase{

    private long minBlockSize = 1048576;
    private long bytesPerChecksum = 512;

    public void testGetValidBlockSizeWhenFileLengthLowerThanMinBlockSize() {
        long fileLength = 615100;
        long validBlockSize = OozieSharelibCLI.BlockSizeCalculator.getValidBlockSize(fileLength, minBlockSize, bytesPerChecksum);
        Assert.assertEquals("The block size should be equal to the defined min block size", minBlockSize, validBlockSize);
    }

    public void testGetValidBlockSizeWhenBytesPerChecksumDoesNotDivideFileLength() {
        long fileLength = 1048577;
        long expectedBlockSize = (fileLength / bytesPerChecksum + 1) * bytesPerChecksum;
        long validBlockSize = OozieSharelibCLI.BlockSizeCalculator.getValidBlockSize(fileLength, minBlockSize, bytesPerChecksum);
        Assert.assertEquals("The block size should be the first greater value than the file size, dividable by bytes per checksum",
                expectedBlockSize, validBlockSize);
    }

    public void testGetValidBlockSizeWhenBytesPerChecksumDivideFileLength() {
        long fileLength = 1049088;
        long validBlockSize = OozieSharelibCLI.BlockSizeCalculator.getValidBlockSize(fileLength, minBlockSize, bytesPerChecksum);
        Assert.assertEquals("The block size should be equal with the file length", fileLength, validBlockSize);
    }

}
