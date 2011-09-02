package org.apache.maven.doxia;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.codehaus.plexus.PlexusTestCase;
import org.codehaus.plexus.util.WriterFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Writer;

/**
 * Provide some common convenience methods to test Doxia modules (parsers and sinks).
 */
public abstract class AbstractModuleTest
    extends PlexusTestCase
{
    /**
     * Set the system property <code>line.separator</code> to <code>\n</code> (Unix) to prevent
     * failure on windows.
     */
    static
    {
        // Safety
        System.setProperty( "line.separator", "\n" );
    }

    // ----------------------------------------------------------------------
    // Methods for creating test reader and writer
    // ----------------------------------------------------------------------

    /**
     * Returns a FileWriter to write to a file with the given name
     * in the test target output directory.
     *
     * @param baseName The name of the target file.
     * @param extension The file extension of the file to write.
     * @return A FileWriter.
     * @throws IOException If the FileWriter could not be generated.
     */
    protected Writer getTestWriter( String baseName, String extension )
        throws IOException
    {
        File outputDirectory =
            new File( getBasedirFile(), outputBaseDir() + getOutputDir() );

        if ( !outputDirectory.exists() )
        {
            outputDirectory.mkdirs();
        }

        return WriterFactory.newPlatformWriter(
            new File( outputDirectory, baseName + "." + extension ) );
    }

    /**
     * Returns an XML FileWriter to write to a file with the given name
     * in the test target output directory.
     *
     * @param baseName The name of the target file.
     * @param extension The file extension of the file to write.
     * @return An XML FileWriter.
     * @throws IOException If the FileWriter could not be generated.
     */
    protected Writer getXmlTestWriter( String baseName, String extension )
        throws IOException
    {
        File outputDirectory =
            new File( getBasedirFile(), outputBaseDir() + getOutputDir() );

        if ( !outputDirectory.exists() )
        {
            outputDirectory.mkdirs();
        }

        return WriterFactory.newXmlWriter(
            new File( outputDirectory, baseName + "." + extension ) );
    }

    /**
     * Returns a FileWriter to write to a file with the given name
     * in the test target output directory.
     *
     * @param baseName The name of the target file.
     * @return A FileWriter.
     * @throws IOException If the FileWriter could not be generated.
     */
    protected Writer getTestWriter( String baseName )
        throws IOException
    {
        return getTestWriter( baseName, outputExtension() );
    }

    /**
     * Returns an InputStreamReader to read a resource from a file
     * in the test target output directory.
     *
     * @param baseName The name of the resource file to read.
     * @param extension The file extension of the resource file to read.
     * @return An InputStreamReader.
     */
    protected Reader getTestReader( String baseName, String extension )
    {
        InputStream is =
            Thread.currentThread().getContextClassLoader().getResourceAsStream(
                baseName + "." + extension );

        assertNotNull( "Could not find resource: "
            + baseName + "." + extension, is );

        InputStreamReader reader = new InputStreamReader( is );

        return reader;
    }

    /**
     * Returns an InputStreamReader to read a resource from a file
     * in the test target output directory.
     *
     * @param baseName The name of the resource file to read.
     * @return An InputStreamReader.
     */
    protected Reader getTestReader( String baseName )
    {
        return getTestReader( baseName, outputExtension() );
    }


    // ----------------------------------------------------------------------
    // Utility methods
    // ----------------------------------------------------------------------

    /**
     * Returns the common base directory.
     *
     * @return The common base directory as a File.
     */
    protected File getBasedirFile()
    {
        return new File( getBasedir() );
    }

    /**
     * Returns the base directory where all test output will go.
     *
     * @return The test output directory.
     */
    protected final String outputBaseDir()
    {
        return "target/test-output/";
    }


    // ----------------------------------------------------------------------
    // Abstract methods the individual ModuleTests must provide
    // ----------------------------------------------------------------------

    /**
     * Determines the default file extension for the current module.
     *
     * @return The default file extension.
     */
    protected abstract String outputExtension();

    /**
     * Returns the directory where test output will go.
     * Should be relative to outputBaseDir().
     *
     * @return The test output directory, relative to outputBaseDir().
     */
    protected abstract String getOutputDir();

}
