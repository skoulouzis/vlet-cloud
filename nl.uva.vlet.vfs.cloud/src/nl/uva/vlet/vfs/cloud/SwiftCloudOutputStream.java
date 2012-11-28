/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.uva.vlet.vfs.cloud;

import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import java.io.*;
import java.net.URI;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.ExecutionException;
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
import org.apache.http.params.BasicHttpParams;
import org.apache.http.util.EntityUtils;
import org.jclouds.blobstore.AsyncBlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.options.PutOptions;
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
    private int counter;

    public SwiftCloudOutputStream(String container, String blobName, AsyncBlobStore asyncBlobStore, String key) {

        this.container = container;
        this.blobName = blobName;
        this.asyncBlobStore = asyncBlobStore;
        out = new ByteArrayOutputStream();

        this.key = key;
    }

    @Override
    public void write(final int b) throws IOException {
        out.write(b);
        bytesWriten++;
        if (bytesWriten >= Constants.OUTPUT_STREAM_BUFFER_SIZE_IN_BYTES) {
            uploadChunk();
        }
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
        out.write(b, off, len);
        bytesWriten += len;
        if (bytesWriten >= Constants.OUTPUT_STREAM_BUFFER_SIZE_IN_BYTES) {
            uploadChunk();
        }
    }

    @Override
    public void write(final byte[] b) throws IOException {
        out.write(b);
        bytesWriten += b.length;
        if (bytesWriten >= Constants.OUTPUT_STREAM_BUFFER_SIZE_IN_BYTES) {
            uploadChunk();
        }
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    @Override
    public void close() throws IOException {
        out.close();
        setManifestFile();
//        blobContext.close();
    }

    private void uploadChunk() throws FileNotFoundException, IOException {
        if (this.authToken == null) {
            initHttpClient(this.key);
        }
        HttpResponse resp = null;
        try {
            counter++;
            HttpPut put = new HttpPut(putURL + "/_part" + counter);
            put.setHeader(authToken);

            put.setEntity(new ByteArrayEntity(out.toByteArray()));

//            client = new DefaultHttpClient(params);
//            wrapClient1 = wrapClient1(client);
            resp = wrapClient1.execute(put);

            if (resp.getStatusLine().getStatusCode() != 201) {
                throw new IOException(resp.toString());
            }

        } finally {
            bytesWriten = 0;
            out.reset();
            if (resp != null) {
                EntityUtils.consume(resp.getEntity());
            }
//            wrapClient1.getConnectionManager().closeExpiredConnections();
        }
    }

    private void setManifestFile() throws IOException {

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

//            client = new DefaultHttpClient(params);
//            wrapClient1 = wrapClient1(client);
            resp = wrapClient1.execute(put);

            if (resp.getStatusLine().getStatusCode() != 201) {
                throw new IOException(resp.toString());
            }
        } finally {
            if (resp != null) {
                EntityUtils.consume(resp.getEntity());
            }
            wrapClient1.getConnectionManager().closeExpiredConnections();
            counter = 0;
        }
    }

    private void initHttpClient(String key) throws IOException {
        params = new BasicHttpParams();
        org.apache.http.params.HttpConnectionParams.setSoTimeout(params, 10000);
        params.setParameter("http.socket.timeout", 10000);
        client = new DefaultHttpClient(params);
        wrapClient1 = wrapClient1(client);

        RestContext<Object, Object> ctx = asyncBlobStore.getContext().getProviderSpecificContext();
        URI endpoint = ctx.getEndpoint();

        HttpGet getMethod = new HttpGet(endpoint);
        getMethod.getParams().setIntParameter("http.socket.timeout", 9000);
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

//        client = new DefaultHttpClient(params);
//        wrapClient1 = wrapClient1(client);
    }

    private org.apache.http.client.HttpClient wrapClient1(org.apache.http.client.HttpClient base) {
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
}
