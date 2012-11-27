/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.uva.vlet.vfs.test;

import com.google.common.util.concurrent.ListenableFuture;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;
import nl.uva.vlet.data.StringUtil;
import nl.uva.vlet.exception.ResourceAlreadyExistsException;
import nl.uva.vlet.exception.ResourceCreationFailedException;
import nl.uva.vlet.exception.ResourceException;
import nl.uva.vlet.exception.VRLSyntaxException;
import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.jclouds.blobstore.AsyncBlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.BlobStoreContextFactory;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.StorageType;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.filesystem.reference.FilesystemConstants;

import java.net.URL;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.security.SecureRandom;
import java.util.*;
import javax.net.ssl.*;
import org.apache.http.StatusLine;
import org.apache.http.entity.ByteArrayEntity;
import org.jclouds.domain.Credentials;
import org.jclouds.http.HttpRequest;
import org.jclouds.rest.HttpAsyncClient;
import org.jclouds.rest.HttpClient;
import org.jclouds.rest.RestContext;

/**
 *
 * @author skoulouz
 */
public class TestBlobStore {

    private static AsyncBlobStore asyncBlobStore;
    private static String endpoint;
    private static Properties props;

    public static void main(String args[]) {
        try {
            setup();
//        testToch();
//            touch(true);
//            mkdir(true);
//            rm();
//            writeData();
//            exists(StorageType.BLOB);
//            getOutPutStream();
//            login();

            put();
//            put2();

        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            asyncBlobStore.getContext().close();
        }

    }

    private static void writeData() throws InterruptedException, ExecutionException, IOException {
        File bufferFile = File.createTempFile("tmpFile", null);
        FileOutputStream fos = new FileOutputStream(bufferFile);
        byte[] data = new byte[1024 * 1024];
        Random r = new Random();
        r.nextBytes(data);
        for (int i = 0; i < 10; i++) {
            fos.write(data);
        }
        fos.flush();
        fos.close();
        long startTime;
        try {
            String[] containerAndPath = new String[]{"testBlobStoreVFS", "dirListTest2/file0"};

            startTime = System.currentTimeMillis();

            ListenableFuture<Blob> res = asyncBlobStore.getBlob(containerAndPath[0], containerAndPath[1]);

            Blob blob = res.get();
            if (blob == null) {
//                blob = asyncBlobStore.blobBuilder(containerAndPath[1]).build();
                blob = asyncBlobStore.blobBuilder(containerAndPath[1]).type(StorageType.BLOB).build();

            }
            blob.setPayload(bufferFile);
            if (bufferFile.length() > (800 * 1024 * 1024)) {
                asyncBlobStore.getContext().getBlobStore().putBlob(containerAndPath[0], blob, PutOptions.Builder.multipart());
            } else {
                asyncBlobStore.getContext().getBlobStore().putBlob(containerAndPath[0], blob);
            }
        } finally {
            bufferFile.delete();
        }

        long elapsedTime = System.currentTimeMillis() - startTime;
        debug("Elapsed time = " + elapsedTime);
    }

    protected static boolean exists(StorageType type)
            throws VRLSyntaxException, InterruptedException,
            ExecutionException, ResourceException {


        String[] containerAndPath = new String[]{"testBlobStoreVFS", "testDirB0/subFile1"};


        if (containerAndPath.length <= 1
                || StringUtil.isEmpty(containerAndPath[1])) {

            ListenableFuture<Boolean> resContainerExists = asyncBlobStore.containerExists(containerAndPath[0]);

            // This waits for request to complete
            return resContainerExists.get();

        } else if (containerAndPath.length > 1) {

            ListenableFuture<BlobMetadata> metaRes = asyncBlobStore.blobMetadata(containerAndPath[0], containerAndPath[1]);
            BlobMetadata meta = metaRes.get();

            if (meta != null && meta.getType() == type) {
                return true;
            }

        }
        return false;
    }

    public static boolean rm() throws VRLSyntaxException,
            InterruptedException, ExecutionException {
        String[] containerAndPath = new String[]{"testBlobStoreVFS", "dirListTest2/file0"};

        if (containerAndPath.length <= 1
                || StringUtil.isEmpty(containerAndPath[1])) {

            ListenableFuture<Void> res = asyncBlobStore.deleteContainer(containerAndPath[0]);
            // This waits for request to complete
            res.get();
            return true;
        } else if (containerAndPath.length > 1) {
            ListenableFuture<Void> resRemove = asyncBlobStore.removeBlob(containerAndPath[0], containerAndPath[1]);
            resRemove.get();
            return true;
        }
        return false;
    }

    private static void testToch() throws InterruptedException, ExecutionException, IOException {


        String[] containerAndPath = new String[]{"testBlobStoreVFS", "testReplicable.txt"};


        Blob blob = asyncBlobStore.blobBuilder(containerAndPath[1]).payload("data2").build();

        ListenableFuture<String> res = asyncBlobStore.putBlob(
                containerAndPath[0], blob);


        String get = res.get();
        ListenableFuture<Blob> theBlob = asyncBlobStore.getBlob(containerAndPath[0], containerAndPath[1]);
        blob = theBlob.get();
        InputStream ins = blob.getPayload().getInput();
        byte[] data = new byte[1024];
        ins.read(data);
        ins.close();

        debug("Content: " + new String(data));

//        blob = asyncBlobStore.blobBuilder(containerAndPath[1]).payload("").build();
//        get = res.get();

    }

    public static boolean touch(boolean ignoreExisting) throws InterruptedException, Exception {

//        String[] containerAndPath = new String[]{"testBlobStoreVFS", "testReplicable.txt"};
//        String[] containerAndPath = new String[]{"testBlobStoreVFS", "testDirB0"};
//        String[] containerAndPath = new String[]{"testBlobStoreVFS", "testDirH"};
//        String[] containerAndPath = new String[]{"testBlobStoreVFS", "dir"};
        String[] containerAndPath = new String[]{"testBlobStoreVFS", "nonExisting"};

        //Exists ?
        ListenableFuture<BlobMetadata> blobMeta = asyncBlobStore.blobMetadata(containerAndPath[0], containerAndPath[1]);
        BlobMetadata meta = blobMeta.get();
        if (meta == null) {
            //Ok non existing 
            Blob blob = asyncBlobStore.blobBuilder(containerAndPath[1]).type(StorageType.BLOB).build();
            blob.setPayload("");
            ListenableFuture<String> res = asyncBlobStore.putBlob(containerAndPath[0], blob);

            res.get();

            return true;
        }
        //What is it?
        if (meta.getType() != StorageType.BLOB) {
            throw new Exception(containerAndPath[1] + " already exists as a folder");
        } else if (meta.getType() == StorageType.BLOB && !ignoreExisting) {
            throw new Exception(containerAndPath[1] + " already exists");
        } else if (meta.getType() == StorageType.BLOB && ignoreExisting) {
            return true;
        }
        return false;
    }

    protected static void mkdir(boolean ignoreExisting) throws Exception {

        String[] containerAndPath = new String[]{"testBlobStoreVFS", "dir3"};
        //Create container
        if (containerAndPath.length <= 1
                || StringUtil.isEmpty(containerAndPath[1])) {
            createContainer(containerAndPath[0], ignoreExisting);
        } else {
            createFolder(containerAndPath, ignoreExisting);
        }
    }

    private static void setup() throws FileNotFoundException, IOException {
        String provider = "filesystem";//"swift"; // "in-memory" "filesystem";//
//        Properties props = new Properties();

        props = getCloudProperties();

        if (StringUtil.isEmpty(provider)) {
            throw new NullPointerException("Provider is null!");
        }
        endpoint = "https://149.156.10.131:8443/auth/v1.0/";
        props.setProperty(org.jclouds.Constants.PROPERTY_ENDPOINT, endpoint);
        props.setProperty(org.jclouds.Constants.PROPERTY_TRUST_ALL_CERTS,
                "true");
        props.setProperty(org.jclouds.Constants.PROPERTY_RELAX_HOSTNAME, "true");

        String path = System.getProperty("user.home") + "/.local/filesystemstorage";
        File localStorage = new File(path);
        if (!localStorage.exists()) {
            localStorage.mkdirs();

        }
        props.setProperty(FilesystemConstants.PROPERTY_BASEDIR, path);

        BlobStoreContext blobStoreContext = new BlobStoreContextFactory().createContext(provider, props);
        asyncBlobStore = blobStoreContext.getAsyncBlobStore();
    }

    private static void debug(String msg) {
        System.err.println(TestBlobStore.class.getName() + ": " + msg);
    }

    private static Properties getCloudProperties()
            throws FileNotFoundException, IOException {
        Properties properties = new Properties();
        String propPath = System.getProperty("user.home") + File.separator
                + "workspace" + File.separator + "nl.uva.vlet.vfs.cloud"
                + File.separator + "etc" + File.separator + "cloud.properties";
        File f = new File(propPath);
        properties.load(new FileInputStream(f));

        return properties;
    }

    private static void createContainer(String container, boolean ignoreExisting) throws ResourceAlreadyExistsException, InterruptedException, ExecutionException, ResourceCreationFailedException {
        ListenableFuture<Boolean> res = asyncBlobStore.containerExists(container);
        Boolean containerExists = res.get();
        if (containerExists && !ignoreExisting) {
            throw new nl.uva.vlet.exception.ResourceAlreadyExistsException(
                    container + "Exists");
        } else if (containerExists) {
            //return new CloudDir(this, vrl);
            return;
        }
        ListenableFuture<Boolean> createContainer = asyncBlobStore.createContainerInLocation(null,
                container);
        Boolean created = createContainer.get();

        if (!created) {
            throw new nl.uva.vlet.exception.ResourceCreationFailedException(
                    "Could not create " + container);
        }
        //return new CloudDir(this, vrl);
        return;
    }

    private static void createFolder(String[] containerAndPath, boolean ignoreExisting) throws ExecutionException, InterruptedException, ResourceAlreadyExistsException, ResourceException {

        //Exists ?
        ListenableFuture<BlobMetadata> blobMeta = asyncBlobStore.blobMetadata(containerAndPath[0], containerAndPath[1]);
        BlobMetadata meta = blobMeta.get();
        if (meta == null) {
            //Ok non existing
            try {
                ListenableFuture<Void> createdDirRes = asyncBlobStore.createDirectory(containerAndPath[0], containerAndPath[1]);
                createdDirRes.get();
                //return new CloudDir(this, vrl);
                return;
            } catch (Exception ex) {
                if (!ex.getMessage().contains("(Is a directory)")) {
                    if (ex instanceof ResourceAlreadyExistsException) {
                        throw ((ResourceAlreadyExistsException) ex);
                    }
                    throw new ResourceException(ex.getMessage());
                }
            }
        }
        //What is it?
        if (meta.getType() == StorageType.BLOB) {
            throw new nl.uva.vlet.exception.ResourceAlreadyExistsException(
                    containerAndPath[1] + " already exists as a file");
        }
        if (!ignoreExisting) {
            throw new nl.uva.vlet.exception.ResourceAlreadyExistsException(
                    containerAndPath[1] + "Exists");
        }
        //return new CloudDir(this, vrl);
    }

    private static void getOutPutStream() throws MalformedURLException, NoSuchAlgorithmException, KeyManagementException, IOException {
        final URL url = new URL(endpoint);

        // configure the SSLContext with a TrustManager
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(new KeyManager[0], new TrustManager[]{new DefaultTrustManager()}, new SecureRandom());
        SSLContext.setDefault(ctx);


        HttpsURLConnection authConnection = (HttpsURLConnection) url.openConnection();

        String uname = props.getProperty("jclouds.identity");
        String key = props.getProperty("jclouds.credential");
        authConnection.addRequestProperty("X-Storage-User", uname);
        authConnection.addRequestProperty("X-Storage-Pass", key);

        SSLContext ssl = SSLContext.getInstance("TLSv1");
        ssl.init(null, new TrustManager[]{new SimpleX509TrustManager()}, null);
        javax.net.ssl.SSLSocketFactory factory = ssl.getSocketFactory();
        authConnection.setSSLSocketFactory(factory);
        authConnection.setHostnameVerifier(new HostnameVerifier() {

            @Override
            public boolean verify(String hostname, SSLSession session) {
                System.out.println("hostname: " + hostname);
                return true;
            }
        });


        System.out.println("H0 " + authConnection.getHeaderField(0));
        System.out.println("H1 " + authConnection.getHeaderField(1));
        System.out.println("H2 " + authConnection.getHeaderField(2));
        System.out.println("H3 " + authConnection.getHeaderField(3));
        System.out.println("H4 " + authConnection.getHeaderField(4));
        System.out.println("H5 " + authConnection.getHeaderField(5));
        System.out.println("H6 " + authConnection.getHeaderField(6));
        authConnection.disconnect();



        URL storageUrl = new URL(authConnection.getHeaderField(1) + "/deleteMe/someFile");
        HttpsURLConnection storageConnection = (HttpsURLConnection) storageUrl.openConnection();
        storageConnection.setDoOutput(true);
        storageConnection.setDoInput(true);

        storageConnection.setRequestMethod("POST");
//        storageConnection.addRequestProperty("User-Agent", "curl/7.22.0 (i686-pc-linux-gnu) libcurl/7.22.0 OpenSSL/1.0.1 zlib/1.2.3.4 libidn/1.23 librtmp/2.3");
//        storageConnection.addRequestProperty("Host", "149.156.10.131:8443");
        storageConnection.addRequestProperty("X-Auth-Token", authConnection.getHeaderField(2));
        storageConnection.addRequestProperty("Accept", "*/*");
        storageConnection.setSSLSocketFactory(factory);
        storageConnection.setHostnameVerifier(new HostnameVerifier() {

            @Override
            public boolean verify(String hostname, SSLSession session) {
                System.out.println("hostname: " + hostname);
                return true;
            }
        });


        OutputStreamWriter wr = new OutputStreamWriter(storageConnection.getOutputStream());
        wr.write("DATAAAAsssssssssssssssssss");
        wr.flush();
        // Get the response 
        BufferedReader rd = new BufferedReader(new InputStreamReader(storageConnection.getInputStream()));
        String line;
        while ((line = rd.readLine()) != null) {
            System.out.println("line: " + line);
        }
        wr.close();
        rd.close();
    }

    private static void login() throws IOException, NoSuchAlgorithmException, KeyManagementException {
        String uname = props.getProperty("jclouds.identity");
        String key = props.getProperty("jclouds.credential");

        final URL url = new URL(endpoint);

        // configure the SSLContext with a TrustManager
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(new KeyManager[0], new TrustManager[]{new DefaultTrustManager()}, new SecureRandom());
        SSLContext.setDefault(ctx);


        HttpsURLConnection authConnection = (HttpsURLConnection) url.openConnection();

        authConnection.addRequestProperty("X-Storage-User", uname);
        authConnection.addRequestProperty("X-Storage-Pass", key);

        SSLContext ssl = SSLContext.getInstance("TLSv1");
        ssl.init(null, new TrustManager[]{new SimpleX509TrustManager()}, null);
        javax.net.ssl.SSLSocketFactory factory = ssl.getSocketFactory();
        authConnection.setSSLSocketFactory(factory);
        authConnection.setHostnameVerifier(new HostnameVerifier() {

            @Override
            public boolean verify(String hostname, SSLSession session) {
                System.out.println("hostname: " + hostname);
                return true;
            }
        });


        System.out.println("H0 " + authConnection.getHeaderField(0));
        System.out.println("H1 " + authConnection.getHeaderField(1));
        System.out.println("H2 " + authConnection.getHeaderField(2));
        System.out.println("H3 " + authConnection.getHeaderField(3));
        System.out.println("H4 " + authConnection.getHeaderField(4));
        System.out.println("H5 " + authConnection.getHeaderField(5));
        System.out.println("H6 " + authConnection.getHeaderField(6));
        authConnection.disconnect();

        URL storageUrl = new URL(authConnection.getHeaderField(1) + "/deleteMe/someFile");
        HttpsURLConnection storageConnection = (HttpsURLConnection) storageUrl.openConnection();
        storageConnection.setDoOutput(true);
        storageConnection.setDoInput(true);

//        storageConnection.addRequestProperty("PUT", "/v1/AUTH_047ec1a4-0362-43b6-9991-f9323c6853f5/LOBCDER-REPLICA-v2.0/DATAAAA HTTP/1.1");
//        storageConnection.addRequestProperty("User-Agent", "curl/7.22.0 (i686-pc-linux-gnu) libcurl/7.22.0 OpenSSL/1.0.1 zlib/1.2.3.4 libidn/1.23 librtmp/2.3");
//        storageConnection.addRequestProperty("Host", "149.156.10.131:8443");
        storageConnection.addRequestProperty("X-Auth-Token", authConnection.getHeaderField(2));
//        storageConnection.addRequestProperty("Content-Length", "10");
//        storageConnection.addRequestProperty("Content-Type", "application/x-www-form-urlencoded");
//        storageConnection.addRequestProperty("Accept", "*/*");
        storageConnection.setSSLSocketFactory(factory);
        storageConnection.setHostnameVerifier(new HostnameVerifier() {

            @Override
            public boolean verify(String hostname, SSLSession session) {
                System.out.println("hostname: " + hostname);
                return true;
            }
        });

        OutputStreamWriter wr = new OutputStreamWriter(storageConnection.getOutputStream());
        wr.write("DATAAAA");
        wr.flush();
        // Get the response 
        BufferedReader rd = new BufferedReader(new InputStreamReader(storageConnection.getInputStream()));
        String line;
        while ((line = rd.readLine()) != null) {
            System.out.println("line: " + line);
        }
        wr.close();
        rd.close();



    }

    private static org.apache.http.client.HttpClient wrapClient1(org.apache.http.client.HttpClient base) {
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

    private static void put() throws IOException, Exception {
        String uname = props.getProperty("jclouds.identity");
        String key = props.getProperty("jclouds.credential");

        HttpGet getMethod = new HttpGet(endpoint);
        getMethod.getParams().setIntParameter("http.socket.timeout", 10000);
        getMethod.setHeader("x-auth-user", uname);
        getMethod.setHeader("x-auth-key", key);
        BasicHttpParams params = new BasicHttpParams();
        org.apache.http.params.HttpConnectionParams.setSoTimeout(params, 10000);
        params.setParameter("http.socket.timeout", 10000);

        org.apache.http.client.HttpClient client = new DefaultHttpClient(params);
        org.apache.http.client.HttpClient wrapClient1 = wrapClient1(client);

        HttpResponse resp = wrapClient1.execute(getMethod);
        StatusLine status = resp.getStatusLine();
        System.out.println("Status: " + status.getReasonPhrase() + " " + status.getStatusCode());

        Header[] allHeaders = resp.getAllHeaders();
        for (Header h : allHeaders) {
            System.out.println(h.getName() + " : " + h.getValue());
            HeaderElement[] elem = h.getElements();
            for (HeaderElement e : elem) {
                System.out.println("\t" + e.getName() + " : " + e.getValue());
            }
        }

        wrapClient1.getConnectionManager().closeExpiredConnections();
//        wrapClient1.getConnectionManager().shutdown();




        if (resp.getEntity() != null) {
            InputStream instream = resp.getEntity().getContent();
            try {

                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(instream));
                // do something useful with the response
                System.out.println(reader.readLine());

            } catch (IOException ex) {

                // In case of an IOException the connection will be released
                // back to the connection manager automatically
                throw ex;

            } catch (RuntimeException ex) {

                // In case of an unexpected exception you may want to abort
                // the HTTP request in order to shut down the underlying
                // connection and release it back to the connection manager.
                getMethod.abort();
                throw ex;

            } finally {
                // Closing the input stream will trigger connection release
                instream.close();

                // When HttpClient instance is no longer needed,
                // shut down the connection manager to ensure
                // immediate deallocation of all system resources
                wrapClient1.getConnectionManager().shutdown();
            }
        }

        Header storageURLHeader = resp.getFirstHeader("X-Storage-Url");
        Header authToken = resp.getFirstHeader("X-Auth-Token");
        String storageURL = storageURLHeader.getValue();
        System.out.println("storageURL; " + storageURL);
        System.out.println("authToken; " + authToken);


        //Add part 1
        String container = "deleteMe";
        String putURL = storageURL + "/" + container + "/someFile/_part1";
        HttpPut put = new HttpPut(putURL);
        put.getParams().setIntParameter("http.socket.timeout", 10000);
        put.setHeader(authToken);

        byte[] data1 = new byte[10];
        for (int i = 0; i < data1.length; i++) {
            data1[i] = (byte) i;
        }
        for (int i = 0; i < data1.length; i++) {
            System.out.println("Set2: " + data1[i]);
        }

        ByteArrayEntity byteEntry = new ByteArrayEntity(data1);
        put.setEntity(byteEntry);
//        put.setHeader(byteEntry.getContentType());

        client = new DefaultHttpClient(params);
        wrapClient1 = wrapClient1(client);
        resp = wrapClient1.execute(put);
        InputStream instream = resp.getEntity().getContent();
        BufferedReader reader = new BufferedReader(new InputStreamReader(instream));
        // do something useful with the response
        System.out.println("Responce PUT " + put.getURI() + " " + reader.readLine());
        wrapClient1.getConnectionManager().closeExpiredConnections();


        //Add part 2
        putURL = storageURL + "/" + container + "/someFile/_part2";
        put = new HttpPut(putURL);
        put.getParams().setIntParameter("http.socket.timeout", 10000);
        put.setHeader(authToken);

        byte[] data2 = new byte[10];
        for (int i = 0; i < data2.length; i++) {
            data2[i] = (byte) (i + data2.length);
        }

        for (int i = 0; i < data2.length; i++) {
            System.out.println("Set2: " + data2[i]);
        }
        byteEntry = new ByteArrayEntity(data2);
        put.setEntity(byteEntry);
//        put.setHeader(byteEntry.getContentType());

        client = new DefaultHttpClient(params);
        wrapClient1 = wrapClient1(client);
        resp = wrapClient1.execute(put);
        instream = resp.getEntity().getContent();
        reader = new BufferedReader(new InputStreamReader(instream));
        // do something useful with the response
        System.out.println("Responce PUT " + put.getURI() + " " + reader.readLine());
        wrapClient1.getConnectionManager().closeExpiredConnections();

        //Get BACK the file 
        putURL = storageURL + "/" + container + "/someFile";
        put = new HttpPut(putURL);
        put.setHeader("X-Object-Manifest", container + "/someFile");
        put.setHeader(authToken);

        client = new DefaultHttpClient(params);
        wrapClient1 = wrapClient1(client);

        resp = wrapClient1.execute(put);
        instream = resp.getEntity().getContent();
        reader = new BufferedReader(new InputStreamReader(instream));
        // do something useful with the response
        System.out.println(" Manifest Responce:---- " + reader.readLine());
        wrapClient1.getConnectionManager().closeExpiredConnections();

        getMethod = new HttpGet(putURL);
        getMethod.setHeader(authToken);
        client = new DefaultHttpClient(params);
        wrapClient1 = wrapClient1(client);
        resp = wrapClient1.execute(getMethod);
        instream = resp.getEntity().getContent();
        int len = 0;
        byte[] buffer = new byte[10];
        while (len != -1) {
            len = instream.read(buffer);
            for (int i = 0; i < len; i++) {
                System.out.println("Responce " + getMethod.getURI() + " " + buffer[i]);
            }
        }

        getMethod = new HttpGet(putURL + "/_part2");
        getMethod.setHeader(authToken);

        client = new DefaultHttpClient(params);
        wrapClient1 = wrapClient1(client);

        resp = wrapClient1.execute(getMethod);
        instream = resp.getEntity().getContent();
        len = 0;
        buffer = new byte[10];
        while (len != -1) {
            len = instream.read(buffer);
            for (int i = 0; i < len; i++) {
                System.out.println("Responce " + getMethod.getURI() + " " + buffer[i]);
            }
        }


        wrapClient1.getConnectionManager().closeExpiredConnections();

    }

    private static void put2() throws IOException, Exception {
        RestContext<Object, Object> ctx = asyncBlobStore.getContext().getProviderSpecificContext();
        URI ctxEndpoint = ctx.getEndpoint();
        System.out.println("ctxEndpoint: " + ctxEndpoint);

        Map<String, Credentials> cred = ctx.getCredentialStore();
        Iterator<String> keysIter = cred.keySet().iterator();

        while (keysIter.hasNext()) {
            String key = keysIter.next();
            System.out.println(key + " : " + cred.get(key));
        }
        System.out.println(ctx.getDescription());
        System.out.println(ctx.getId());

        System.out.println(ctx.getIdentity());



    }

    private static SSLSocketFactory getSSLSocketFactory() throws KeyManagementException, NoSuchAlgorithmException {


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

    private static javax.net.ssl.SSLSocketFactory getJAvaXSSLSocketFactory() throws KeyManagementException, NoSuchAlgorithmException {
        SSLContext ctx = getSSLContext();
        return ctx.getSocketFactory();


    }

    static class SimpleX509TrustManager implements X509TrustManager {

        @Override
        public void checkClientTrusted(
                X509Certificate[] cert, String s)
                throws CertificateException {
        }

        @Override
        public void checkServerTrusted(
                X509Certificate[] cert, String s)
                throws CertificateException {
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            // TODO Auto-generated method stub
            return null;
        }
    }

    private static class DefaultTrustManager implements X509TrustManager {

        @Override
        public void checkClientTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return null;
        }
    }
}
