/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.uva.vlet.vfs.test;

import com.google.common.util.concurrent.ListenableFuture;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import nl.uva.vlet.data.StringUtil;
import nl.uva.vlet.exception.ResourceAlreadyExistsException;
import nl.uva.vlet.exception.ResourceCreationFailedException;
import nl.uva.vlet.exception.ResourceException;
import nl.uva.vlet.exception.VRLSyntaxException;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.SingleClientConnManager;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.BasicHttpParams;
import org.jclouds.blobstore.AsyncBlobStore;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.BlobStoreContextFactory;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.StorageType;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.filesystem.reference.FilesystemConstants;
import org.jclouds.http.HttpRequest;
import org.jclouds.http.config.JavaUrlHttpCommandExecutorServiceModule;
import org.jclouds.io.Payload;
import org.jclouds.io.WriteTo;
import org.jclouds.io.payloads.BasePayload;
import org.jclouds.io.payloads.FilePayload;
import org.jclouds.io.payloads.StreamingPayload;
import org.jclouds.rest.HttpClient;

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
        String provider = "swift"; // "in-memory" "filesystem";//
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

    private static void getOutPutStream() {
        try {
            String[] containerAndPath = new String[]{"testBlobStoreVFS", "nonExisting"};
            //            ByteArrayInputStream ins = new ByteArrayInputStream("DATA".getBytes());
            String filePath1 = "/home/" + System.getProperty("user.home") + "/Documents/mails/thunderbird.mitsosl.uva.nl.tar.gz.gpg";
            String filePath2 = "/etc/passwd";
            File aLargeFile = new File(filePath2);
            InputStream ins = new FileInputStream(aLargeFile);
            BlobStoreContext cont = asyncBlobStore.getContext();

        } catch (Exception ex) {
            Logger.getLogger(TestBlobStore.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private static void login() throws IOException {
        HttpGet method = new HttpGet(endpoint);
        method.getParams().setIntParameter("http.socket.timeout", 10000);
        String uname = props.getProperty("jclouds.identity");
        String key = props.getProperty("jclouds.credential");
        method.setHeader("x-auth-user", uname);
        method.setHeader("x-auth-key", key);
        BasicHttpParams params = new BasicHttpParams();
        org.apache.http.params.HttpConnectionParams.setSoTimeout(params, 10000);
        params.setParameter("http.socket.timeout", 10000);

        org.apache.http.client.HttpClient client = new DefaultHttpClient(params);
        org.apache.http.client.HttpClient wrapClient1 = wrapClient1(client);

        HttpResponse resp = wrapClient1.execute(method);
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
        Header storageURLHeader = resp.getFirstHeader("X-Storage-Url");
        String storageURL = storageURLHeader.getValue();
        System.out.println("storageURL; " + storageURL);

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
        HttpGet get = new HttpGet(endpoint);
        get.getParams().setIntParameter("http.socket.timeout", 10000);
        String uname = props.getProperty("jclouds.identity");
        String key = props.getProperty("jclouds.credential");
        get.setHeader("x-auth-user", uname);
        get.setHeader("x-auth-key", key);
        BasicHttpParams params = new BasicHttpParams();
        org.apache.http.params.HttpConnectionParams.setSoTimeout(params, 10000);
        params.setParameter("http.socket.timeout", 10000);

        org.apache.http.client.HttpClient client = new DefaultHttpClient(params);
        org.apache.http.client.HttpClient wrapClient1 = wrapClient1(client);
        HttpResponse resp = wrapClient1.execute(get);

        Header[] allHeaders = resp.getAllHeaders();
        for (Header h : allHeaders) {
            System.out.println(h.getName() + " : " + h.getValue());
            HeaderElement[] elem = h.getElements();
            for (HeaderElement e : elem) {
                System.out.println("\t" + e.getName() + " : " + e.getValue());
            }
        }

        Header storageURLHeader = resp.getFirstHeader("X-Storage-Url");
        String storageURL = storageURLHeader.getValue();
        Header authTokenHeader = resp.getFirstHeader("X-Auth-Token");
        String authToken = authTokenHeader.getValue();


        String container = "deleteMe";
        String putURL = storageURL + "/" + container + "/someFile";
        HttpPut put = new HttpPut(putURL);
        put.getParams().setIntParameter("http.socket.timeout", 10000);
        put.setHeader("X-Auth-Token", authToken);

        HttpEntity entity = new FileEntity(new File("/etc/passwd"), "text/plain");
        put.setEntity(entity);
        put.setHeader(entity.getContentType());


        System.out.println("---------------PUT HEADERS---------------------");
        allHeaders = put.getAllHeaders();
        for (Header h : allHeaders) {
            System.out.println(h.getName() + " : " + h.getValue());
            HeaderElement[] elem = h.getElements();
            for (HeaderElement e : elem) {
                System.out.println("\t" + e.getName() + " : " + e.getValue());
            }
        }

        URL url = new URL(putURL);
        HttpsURLConnection con = (HttpsURLConnection) url.openConnection();
        SSLContext sc = getSSLContext();
        con.setSSLSocketFactory(sc.getSocketFactory());
        
        con.setHostnameVerifier(new HostnameVerifier()
        {      
            @Override
            public boolean verify(String hostname, SSLSession session)
            {
                System.out.println("Host name: "+hostname);
                System.out.println("session: "+session.getCipherSuite());
                System.out.println("session: "+session.getPeerHost());
                System.out.println("session: "+session.getProtocol());
                return true;
            }
        });
        
        con.setRequestProperty("X-Auth-Token",authToken);
        con.setDoOutput(true);
        OutputStream out = con.getOutputStream();
        out.write("DATA".getBytes());
        out.flush();
        out.close();
        con.disconnect();

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
}