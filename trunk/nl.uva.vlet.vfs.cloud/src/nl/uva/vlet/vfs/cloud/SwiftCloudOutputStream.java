/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.uva.vlet.vfs.cloud;

import com.sun.management.OperatingSystemMXBean;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
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
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.util.EntityUtils;
import org.jclouds.blobstore.AsyncBlobStore;
import org.jclouds.rest.RestContext;


/**
 *
 * @author skoulouz
 */
class SwiftCloudOutputStream extends OutputStream {

    private int bytesWriten = 0;
    private final String container;
    private final String blobName;
    private final ByteArrayOutputStream out;
    private Header authToken;
    private final String key;
    private BasicHttpParams params;
    private DefaultHttpClient client;
    private HttpClient wrapClient1;
    private final AsyncBlobStore asyncBlobStore;
    private Header storageURLHeader;
    private String putURL;
    private int counter = 1;
    private ThreadPoolExecutor executorService;
    private static int limit;

    public SwiftCloudOutputStream(String container, String blobName, AsyncBlobStore asyncBlobStore, String key) {

        this.container = container;
        this.blobName = blobName;
        this.asyncBlobStore = asyncBlobStore;
        out = new ByteArrayOutputStream();
        this.key = key;
        
        OperatingSystemMXBean osMBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
//        System.out.println("Free physical memory:\t" + osMBean.getFreePhysicalMemorySize() / 1024 + " kB");

        limit = (int) (osMBean.getFreePhysicalMemorySize() / 20);  //20 * 1024 * 1024;//Constants.OUTPUT_STREAM_BUFFER_SIZE_IN_BYTES;
    }

    @Override
    public void write(final int b) throws IOException {
        out.write(b);
        bytesWriten++;
        if (bytesWriten >= limit) {
            uploadChunk();
        }
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
        out.write(b, off, len);
        bytesWriten += len;
        if (bytesWriten >= limit) {
            uploadChunk();
        }
    }

    @Override
    public void write(final byte[] b) throws IOException {
        out.write(b);
        bytesWriten += b.length;
        if (bytesWriten >= limit) {
            uploadChunk();
        }
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    @Override
    public void close() throws IOException {
        try {
            out.close();
            setManifestFile();
            //        blobContext.close();
        } catch (ExecutionException ex) {
            throw new IOException(ex);
        } catch (InterruptedException ex) {
            throw new IOException(ex);
        }
    }

    private void uploadChunk() throws FileNotFoundException, IOException {
        if (this.authToken == null) {
            initHttpClient(this.key);
        }
        try {
            HttpPut put = new HttpPut(putURL + "/_part" + counter);
            put.setHeader(authToken);
            put.setEntity(new ByteArrayEntity(out.toByteArray()));

            PutRunnable putTask = new PutRunnable(wrapClient1);
            putTask.setPut(put);
            executorService.submit(putTask);
        } finally {
            bytesWriten = 0;
            counter++;
            out.reset();
        }
    }

    private void setManifestFile() throws IOException, InterruptedException, ExecutionException {
        if (out.size() > 0 || out.toByteArray().length > 0) {
            uploadChunk();
        }
        if (authToken == null) {
            initHttpClient(this.key);
        }
        HttpResponse resp = null;
        try {
            HttpPut put = new HttpPut(putURL);
            put.setHeader("X-Object-Manifest", container + "/" + blobName);
            put.setHeader(authToken);

            PutRunnable putTask = new PutRunnable(wrapClient1);
            putTask.setPut(put);
            executorService.submit(putTask);

        } finally {
            int count = executorService.getActiveCount();
//            System.err.println("Before Still running: " + count);
            executorService.shutdown();
//            System.err.println("After Still running: " + count);
            long sleepTime = 50;
            while (!executorService.awaitTermination(2, TimeUnit.HOURS)) {
//            while (count >= 1) {
                count = executorService.getActiveCount();
                sleepTime = 25 * count;
//                System.err.println("Still running: " + count + " sleeping for :" + sleepTime);
                Thread.sleep(sleepTime);
            }
//            System.err.println("Still running: " + executorService.getActiveCount());
            if (resp != null) {
                EntityUtils.consume(resp.getEntity());
            }
            wrapClient1.getConnectionManager().closeExpiredConnections();
            counter = 0;
        }
    }

    private void initHttpClient(String key) throws IOException {
        params = new BasicHttpParams();
        org.apache.http.params.HttpConnectionParams.setSoTimeout(params, 30000);
        params.setParameter("http.socket.timeout", 30000);

        PoolingClientConnectionManager cm = new PoolingClientConnectionManager();
        cm.setMaxTotal(500);
        cm.setDefaultMaxPerRoute(500);

        client = new DefaultHttpClient(cm, params);
        wrapClient1 = wrapClient(client);

        RestContext<Object, Object> ctx = asyncBlobStore.getContext().getProviderSpecificContext();
        URI endpoint = ctx.getEndpoint();

        HttpGet getMethod = new HttpGet(endpoint);
        getMethod.getParams().setIntParameter("http.socket.timeout", 30000);
        getMethod.setHeader("x-auth-user", ctx.getIdentity());
        getMethod.setHeader("x-auth-key", key);
        HttpResponse resp = wrapClient1.execute(getMethod);
        if (resp.getStatusLine().getStatusCode() != 200) {
            throw new IOException(resp.getStatusLine().toString());
        }

        storageURLHeader = resp.getFirstHeader("X-Storage-Url");
        authToken = resp.getFirstHeader("X-Auth-Token");

        wrapClient1.getConnectionManager().closeExpiredConnections();

        putURL = storageURLHeader.getValue() + "/" + container + "/" + blobName;
        EntityUtils.consume(resp.getEntity());



//        int cpus = Runtime.getRuntime().availableProcessors();
        int maxThreads = 2;//cpus * 1;
        maxThreads = (maxThreads > 0 ? maxThreads : 1);
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

    private static SSLContext getSSLContext() throws KeyManagementException, NoSuchAlgorithmException {
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
        private PutRunnable(HttpClient client) {
            this.client = client;
        }

        @Override
        public void run() {
            HttpResponse resp = null;
            try {

//                long start = System.currentTimeMillis();
                resp = client.execute(put);
//                long end = System.currentTimeMillis();
//                double time = (end - start) / 1000.0;
//                double len = put.getEntity().getContentLength() / 1024.0;
//                System.out.println("len: " + len);
//                System.out.println("time: " + time);
//                double speed = (len) / (time);
//                System.out.println("Speed: " + speed + "kb/s");
//                count++;
//                avSpeed = (avSpeed + speed) / count;

                if (resp.getStatusLine().getStatusCode() != 201) {
                    throw new IOException(resp.toString());
                }
            } catch (IOException ex) {
                Logger.getLogger(SwiftCloudOutputStream.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                if (resp != null) {
                    try {
                        EntityUtils.consume(resp.getEntity());
                    } catch (IOException ex) {
                        Logger.getLogger(SwiftCloudOutputStream.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            }
        }

        private void setPut(HttpPut put) {
            this.put = put;
        }
    }
}
