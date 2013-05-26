/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.uva.vlet.vfs.cloud;

import java.io.*;
import java.net.URI;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.util.EntityUtils;
import org.jclouds.blobstore.AsyncBlobStore;
import org.jclouds.rest.RestContext;

/**
 *
 * @author alogo
 */
class ChunkUploader {

    private final File sourceFile;
    private final String container;
    private final String blobName;
    private final AsyncBlobStore asyncBlobStore;
    private final String key;
    private Header authToken;
    private BasicHttpParams params;
    private PoolingClientConnectionManager cm;
    private Header storageURLHeader;
    private String putURL;
    private int maxThreads;
    private ThreadPoolExecutor executorService;
    private int counter = 1;
    private static final long chunkSize = 1024 * 1024 * 1024;//
    private int chunkFileNum;

    ChunkUploader(File file, String container, String blobName, AsyncBlobStore asyncBlobStore, String key) throws IOException {
        this.sourceFile = file;
        this.container = container;
        this.blobName = blobName;
        this.asyncBlobStore = asyncBlobStore;
        this.key = key;

        int cpus = Runtime.getRuntime().availableProcessors();
        maxThreads = cpus * 2;
        maxThreads = (maxThreads > 0 ? maxThreads : 1);
        chunkFileNum = (int) (file.length() / chunkSize);
        if (file.length() % chunkSize == 0) {
            chunkFileNum++;
        }
        initHttpClient(this.key);
    }

    void upload() throws IOException, InterruptedException, ExecutionException {
        try {

            int read;
            FileInputStream fis = new FileInputStream(sourceFile);
            byte[] copyBuffer = new byte[1024 * 1024];
            File bufferFile = File.createTempFile(this.getClass().getSimpleName() + "part" + counter, null);
            FileOutputStream fos = new FileOutputStream(bufferFile);
            while ((read = fis.read(copyBuffer, 0, copyBuffer.length)) != -1) {
                fos.write(copyBuffer, 0, read);
                if (bufferFile.length() >= chunkSize || counter >= chunkFileNum) {
                    fos.close();
                    HttpPut put = new HttpPut(putURL + "/_part" + counter++);
                    put.setHeader(authToken);
                    put.setEntity(new FileEntity(bufferFile));
                    ChunkUploader.PutRunnable putTask = new ChunkUploader.PutRunnable(wrapClient(new DefaultHttpClient(cm, params)), bufferFile);
                    putTask.setPut(put);
                    executorService.submit(putTask);
                    bufferFile = File.createTempFile(this.getClass().getSimpleName() + "part" + counter, null);
                    fos = new FileOutputStream(bufferFile);
                }
            }

        } finally {
            setManifestFile();
            int count = executorService.getActiveCount();
            executorService.shutdown();
            long sleepTime = 50;
            while (!executorService.awaitTermination(2, TimeUnit.HOURS)) {
                count = executorService.getActiveCount();
                sleepTime = 25 * count;
                Thread.sleep(sleepTime);
            }
            this.cm.closeExpiredConnections();
            counter = 0;
        }
    }

    private void setManifestFile() throws IOException, InterruptedException, ExecutionException {
        if (authToken == null) {
            initHttpClient(this.key);
        }
        HttpPut put = new HttpPut(putURL);
        put.setHeader("X-Object-Manifest", container + "/" + blobName);
        put.setHeader(authToken);

        ChunkUploader.PutRunnable putTask = new ChunkUploader.PutRunnable(wrapClient(new DefaultHttpClient(cm, params)), null);
        putTask.setPut(put);
        executorService.submit(putTask);
    }

    private void initHttpClient(String key) throws IOException {
        params = new BasicHttpParams();
        org.apache.http.params.HttpConnectionParams.setSoTimeout(params, Constants.TIME_OUT);
        params.setParameter("http.socket.timeout", Constants.TIME_OUT);

        RestContext<Object, Object> ctx = asyncBlobStore.getContext().getProviderSpecificContext();
        URI endpoint = ctx.getEndpoint();

        HttpGet getMethod = new HttpGet(endpoint);
        getMethod.getParams().setIntParameter("http.socket.timeout", Constants.TIME_OUT);
        getMethod.setHeader("x-auth-user", ctx.getIdentity());
        getMethod.setHeader("x-auth-key", key);

        cm = new PoolingClientConnectionManager();
        cm.setMaxTotal(500);
        cm.setDefaultMaxPerRoute(500);
        DefaultHttpClient client = new DefaultHttpClient(cm, params);
        HttpClient wrapClient1 = wrapClient(client);

        HttpResponse resp = wrapClient1.execute(getMethod);
        if (resp.getStatusLine().getStatusCode() != 200) {
            throw new IOException(resp.getStatusLine().toString());
        }

        storageURLHeader = resp.getFirstHeader("X-Storage-Url");
        authToken = resp.getFirstHeader("X-Auth-Token");

        wrapClient1.getConnectionManager().closeExpiredConnections();

        putURL = storageURLHeader.getValue() + "/" + container + "/" + blobName;
        EntityUtils.consume(resp.getEntity());

        ArrayBlockingQueue<Runnable> queue = new ArrayBlockingQueue<Runnable>(maxThreads);
        executorService = new ThreadPoolExecutor(
                maxThreads, // core thread pool size
                maxThreads, // maximum thread pool size
                20, // time to wait before resizing pool
                TimeUnit.SECONDS,
                queue,
                new ThreadPoolExecutor.CallerRunsPolicy());

    }

    private org.apache.http.client.HttpClient wrapClient(org.apache.http.client.HttpClient base) {
        try {
            SSLSocketFactory ssf = getSSLSocketFactory();
            ClientConnectionManager ccm = base.getConnectionManager();
            SchemeRegistry sr = ccm.getSchemeRegistry();
            sr.register(new Scheme("https", ssf, 443));
            return new DefaultHttpClient(ccm, base.getParams());
        } catch (Exception ex) {
            return null;
        }
    }

    private SSLSocketFactory getSSLSocketFactory() throws KeyManagementException, NoSuchAlgorithmException {
        SSLContext ctx = getSSLContext();

        SSLSocketFactory ssf = new SSLSocketFactory(ctx);
        ssf.setHostnameVerifier(SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
        return ssf;
    }

    private SSLContext getSSLContext() throws KeyManagementException, NoSuchAlgorithmException {
        SSLContext ctx = SSLContext.getInstance("TLS");
        X509TrustManager tm = new X509TrustManager() {
            @Override
            public void checkClientTrusted(X509Certificate[] xcs, String string) throws CertificateException {
            }

            @Override
            public void checkServerTrusted(X509Certificate[] xcs, String string) throws CertificateException {
            }

            @Override
            public X509Certificate[] getAcceptedIssuers() {
                return null;
            }
        };
        ctx.init(null, new TrustManager[]{tm}, null);
        return ctx;
    }

    private static class PutRunnable implements Runnable {

        private final HttpClient client;
        private HttpPut put;
        private final File bufferFile;

        private PutRunnable(HttpClient client, File bufferFile) {
            this.client = client;
            this.bufferFile = bufferFile;
        }

        @Override
        public void run() {
            HttpResponse resp = null;
            try {
//                debug("Uploading " + put.getURI());
//                long start = System.currentTimeMillis();
                resp = client.execute(put);
//                long end = System.currentTimeMillis();
//                double time = (end - start) / 1000.0;
//                double len = put.getEntity().getContentLength() / 1024.0;
//                debug("len: " + len);
//                debug("time: " + time);
//                double speed = (len) / (time);
//                debug("Speed: " + speed + "kb/s");
//                count++;
//                avSpeed = (avSpeed + speed) / count;

                if (resp.getStatusLine().getStatusCode() != 201) {
                    throw new IOException(resp.toString());
                }
            } catch (IOException ex) {
                try {
                    client.execute(put);
                    Logger.getLogger(SwiftCloudOutputStream.class.getName()).log(Level.SEVERE, null, ex);
                } catch (IOException ex1) {
                    Logger.getLogger(SwiftCloudOutputStream.class.getName()).log(Level.SEVERE, null, ex1);
                }
            } finally {
                if (resp != null) {
                    try {
                        EntityUtils.consume(resp.getEntity());
                    } catch (IOException ex) {
                        Logger.getLogger(SwiftCloudOutputStream.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
                if (bufferFile != null) {
                    debug("Deleting " + bufferFile.getName());
                    bufferFile.delete();
                }
            }
        }

        private void setPut(HttpPut put) {
            this.put = put;
        }

        private void debug(String msg) {
            System.err.println(this.getClass().getName() + ": " + msg);
        }
    }
}
