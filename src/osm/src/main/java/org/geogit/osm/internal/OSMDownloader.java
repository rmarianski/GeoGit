/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */

package org.geogit.osm.internal;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

import javax.annotation.Nullable;

import org.geogit.api.ProgressListener;

import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;

public class OSMDownloader {

    private final String osmAPIUrl;

    private ProgressListener progress;

    /**
     * @param osmAPIUrl api url, e.g. {@code http://api.openstreetmap.org/api/0.6},
     * @param downloadFolder where to download the data xml contents to
     */
    public OSMDownloader(String osmAPIUrl, ProgressListener progress) {
        checkNotNull(osmAPIUrl);
        checkNotNull(progress);
        this.osmAPIUrl = osmAPIUrl;
        this.progress = progress;
    }

    private class DownloadOSMData {

        private String filter;

        private String osmAPIUrl;

        private File downloadFile;

        public DownloadOSMData(String osmAPIUrl, String filter, @Nullable File downloadFile) {
            this.filter = filter;
            this.osmAPIUrl = osmAPIUrl;
            this.downloadFile = downloadFile;
        }

        public InputStream call() throws Exception {
            URL url = new URL(osmAPIUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setConnectTimeout(10000);
            conn.setReadTimeout(180000);
            conn.setDoInput(true);
            conn.setDoOutput(true);
            conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");

            DataOutputStream printout = new DataOutputStream(conn.getOutputStream());
            printout.writeBytes("data=" + URLEncoder.encode(filter, "utf-8"));
            printout.flush();
            printout.close();

            InputStream inputStream;
            if (downloadFile != null) {
                OutputStream out = new BufferedOutputStream(new FileOutputStream(downloadFile),
                        16 * 1024);
                inputStream = new TeeInputStream(new BufferedInputStream(conn.getInputStream(),
                        16 * 1024), out);
            } else {
                inputStream = new BufferedInputStream(conn.getInputStream(), 16 * 1024);
            }
            return inputStream;
        }
    }

    public InputStream download(String filter, @Nullable File destination) throws Exception {
        InputStream downloadedFile = new DownloadOSMData(osmAPIUrl, filter, destination).call();
        return downloadedFile;
    }

    private static class TeeInputStream extends FilterInputStream {

        private OutputStream out;

        protected TeeInputStream(InputStream in, OutputStream out) {
            super(in);
            this.out = out;
        }

        @Override
        public void close() throws IOException {
            try {
                super.close();
            } finally {
                Closeables.close(out, true);
            }
        }

        @Override
        public int read() throws IOException {
            int b = super.read();
            if (b > -1) {
                out.write(b);
            }
            return b;
        }

        @Override
        public int read(byte b[]) throws IOException {
            int c = super.read(b);
            if (c > -1) {
                out.write(b, 0, c);
            }
            return c;
        }

        @Override
        public int read(byte b[], int off, int len) throws IOException {
            int c = super.read(b, off, len);
            if (c > -1) {
                out.write(b, off, c);
            }
            return c;
        }

    }

    private static class ProgressInputStream extends FilterInputStream {

        private final ProgressListener listener;

        private int readCount;

        public ProgressInputStream(InputStream stream, ProgressListener listener) {
            super(stream);
            this.listener = listener;
        }

        @Override
        public int read() throws IOException {
            int read = super.read();
            if (read != -1) {
                progress(1);
            }
            return read;
        }

        @Override
        public int read(byte b[], int off, int len) throws IOException {
            int read = super.read(b, off, len);
            if (read != -1) {
                progress(read);
            }
            return read;
        }

        /**
         * @param read
         */
        private void progress(int read) {
            readCount += read;
            listener.setProgress(readCount);
        }
    }

    private static void copy(final InputStream from, final File to) {
        File tmp = new File(to.getAbsolutePath() + ".tmp");
        try {
            tmp.createNewFile();
            OutputStream output = new FileOutputStream(tmp);
            try {
                ByteStreams.copy(from, output);
                output.flush();
            } finally {
                output.close();
            }
            to.delete();
            tmp.renameTo(to);
        } catch (Exception e) {
            tmp.delete();
        }
    }
}
