/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.uva.vlet.vfs.cloud;

import java.io.*;
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
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.util.EntityUtils;
import org.jclouds.blobstore.BlobStore;

/**
 *
 * @author S. Koulouzis
 */
class ChunkUploader {

    private final File sourceFile;
    private final String container;
    private final String blobName;
    private final BlobStore blobStore;
    private final String key;
    private Header authToken;
    private BasicHttpParams params;
    private PoolingClientConnectionManager cm;
    private Header storageURLHeader;
    private String putURL;
    private int maxThreads;
    private ThreadPoolExecutor executorService;
    private int counter = 0;
    private long chunkSize = Long.valueOf(1024*1024);//500MB //Long.valueOf("1073741824");//1GB
    private int chunkFileNum;
    private final String endpoint;
    private final String username;

    ChunkUploader(File file, String container, String blobName, BlobStore blobStore, String key,String username,String endpoint) throws IOException {
        this.sourceFile = file;
        this.container = container;
        this.blobName = blobName;
        this.blobStore = blobStore;
        this.key = key;

        int cpus = Runtime.getRuntime().availableProcessors();
        maxThreads = cpus * 2;
        maxThreads = (maxThreads > 0 ? maxThreads : 1);

        if (file.length() < chunkSize) {
            chunkSize = file.length();
        }

        chunkFileNum = (int) (file.length() / chunkSize);

        if (file.length() % chunkFileNum != 0) {
            chunkFileNum++;
        }
       this.endpoint = endpoint;
       this.username= username;
        initHttpClient(this.key);
    }

    void upload() throws IOException, InterruptedException, ExecutionException {
        if (authToken == null) {
            initHttpClient(this.key);
        }
        FileInputStream fis = null;
        try {

            int read;
            fis = new FileInputStream(sourceFile);
            byte[] copyBuffer = new byte[1024 * 1024];
            File bufferFile = File.createTempFile(this.getClass().getSimpleName() + "part" + counter, null);
            FileOutputStream fos = new FileOutputStream(bufferFile);
            while ((read = fis.read(copyBuffer, 0, copyBuffer.length)) != -1) {
                fos.write(copyBuffer, 0, read);
                if (bufferFile.length() >= chunkSize) {
                    fos.flush();
                    fos.close();
                    debug("Counter: " + counter);
                    setUpload(bufferFile);
                    counter++;
                    bufferFile = File.createTempFile(this.getClass().getSimpleName() + "part" + counter, null);
                    fos = new FileOutputStream(bufferFile);
                }
            }
            if (counter < chunkFileNum) {
                fos.flush();
                fos.close();
                debug("Left over Counter: " + counter);
                setUpload(bufferFile);
                counter++;
            }
        } finally {
            if (fis != null) {
                fis.close();
            }
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
        }
    }

    private void setManifestFile() throws IOException, InterruptedException, ExecutionException {
        debug("Sending manifest file");
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

//        RestContext<Object, Object> ctx = blobStore.getContext().getProviderSpecificContext();
//        URI endpoint = ctx.getEndpoint();

        HttpGet getMethod = new HttpGet(endpoint);
        getMethod.getParams().setIntParameter("http.socket.timeout", Constants.TIME_OUT);
        getMethod.setHeader("x-auth-user", username);
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
                maxThreads + 2, // maximum thread pool size
                1, // time to wait before resizing pool
                TimeUnit.HOURS,
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

    private void debug(String msg) {
        System.err.println(this.getClass().getName() + ": " + msg);
    }

    private void setUpload(File bufferFile) {
        HttpPut put = new HttpPut(putURL + "/_part" + counter);
        put.setHeader(authToken);
        put.setEntity(new FileEntity(bufferFile));
        ChunkUploader.PutRunnable putTask = new ChunkUploader.PutRunnable(wrapClient(new DefaultHttpClient(cm, params)), bufferFile);
        putTask.setPut(put);
        executorService.submit(putTask);
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
//                debug("Uploading size: " +  bufferFile.length() / (1024.0 * 1024.0) );
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
                    Logger.getLogger(ChunkUploader.class.getName()).log(Level.SEVERE, null, ex);
                } catch (IOException ex1) {
                    Logger.getLogger(ChunkUploader.class.getName()).log(Level.SEVERE, null, ex1);
                }
            } finally {
                if (resp != null) {
                    try {
                        EntityUtils.consume(resp.getEntity());
                    } catch (IOException ex) {
                        Logger.getLogger(ChunkUploader.class.getName()).log(Level.SEVERE, null, ex);
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
