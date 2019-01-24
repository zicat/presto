/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hive.s3;

import alluxio.AlluxioURI;
import alluxio.client.ReadType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.OpenFileOptions;
import io.airlift.log.Logger;
import org.apache.hadoop.fs.FSInputStream;

import java.io.IOException;
import java.net.URI;

public class PrestoS3AlluxioInputStream
        extends FSInputStream
{
    private static final Logger log = Logger.get(PrestoS3AlluxioInputStream.class);

    private FileInStream is;
    private URI uri;
    private volatile boolean close;
    private FileSystem fs;

    public PrestoS3AlluxioInputStream(FileSystem fs, URI uri)
    {
        this.uri = uri;
        this.fs = fs;
        openStream();
    }

    private void openStream()
    {
        if (is == null) {
            AlluxioURI alluxioURI = new AlluxioURI("/" + uri.getAuthority() + uri.getPath());
            try {
                is = fs.openFile(alluxioURI, OpenFileOptions.defaults().setReadType(ReadType.CACHE));
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void seek(long l) throws IOException
    {
        is.seek(l);
    }

    @Override
    public long getPos()
    {
        return is.getPos();
    }

    @Override
    public boolean seekToNewSource(long l)
    {
        return false;
    }

    @Override
    public int read() throws IOException
    {
        return is.read();
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException
    {
        return is.read(buffer, offset, length);
    }

    @Override
    public void close()
    {
        if (close) {
            return;
        }

        if (is != null) {
            try {
                is.close();
            }
            catch (IOException ignore) {
                log.warn(ignore, "close alluiox input stream error");
            }
            finally {
                is = null;
                close = true;
            }
        }
    }
}
