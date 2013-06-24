package nl.uva.vlet.vfs.cloud;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import nl.uva.vlet.ClassLogger;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;

import com.google.common.util.concurrent.ListenableFuture;
import org.jclouds.blobstore.options.PutOptions;

public class CloudOutputStream extends OutputStream {

    private String provider;
    private Properties props;
    private String container;
    private String blobName;
//    private BlobStoreContext blobContext;
    private File bufferFile;
    private FileOutputStream fos;
    static ClassLogger logger;

    static {
        logger = ClassLogger.getLogger(CloudOutputStream.class);
        logger.setLevelToDebug();
    }
    private final BlobStore blobStore;

//    public CloudOutputStream(String container, String blobName,
//            String provider, Properties props) throws IOException {
//        this.provider = provider;
//        this.props = props;
//        this.container = container;
//        this.blobName = blobName;
//
//        bufferFile = File.createTempFile(this.getClass().getSimpleName(), null);
//
//        fos = new FileOutputStream(bufferFile);
//    }
    CloudOutputStream(String container, String blobName, BlobStore blobStore) throws IOException {
        this.container = container;
        this.blobName = blobName;

        this.blobStore = blobStore;

        bufferFile = File.createTempFile(this.getClass().getSimpleName(), null);
        fos = new FileOutputStream(bufferFile);
    }

    @Override
    public void write(final int b) throws IOException {
        fos.write(b);
    }

    private void writeData() throws InterruptedException, ExecutionException {

        try {

            Blob blob = blobStore.getBlob(container, blobName);
            if (blob == null) {
                blob = blobStore.blobBuilder(blobName).build();
            }

            blob.setPayload(bufferFile);
            if (bufferFile.length() > (800 * 1024 * 1024)) {
                blobStore.getContext().getBlobStore().putBlob(container, blob, PutOptions.Builder.multipart());
            } else {
                blobStore.getContext().getBlobStore().putBlob(container, blob);
            }
        } finally {
            bufferFile.delete();
        }
    }

    @Override
    public void write(final byte[] b, final int off, final int len)
            throws IOException {
        fos.write(b, off, len);
    }

    @Override
    public void write(final byte[] b) throws IOException {
        fos.write(b);
    }

    @Override
    public void flush() throws IOException {
        fos.flush();
    }

    @Override
    public void close() throws IOException {

        fos.close();

        try {
            writeData();
        } catch (InterruptedException e) {
            throw new IOException(e);
        } catch (ExecutionException e) {
            throw new IOException(e);
        }

//        blobContext.close();
    }
//    private BlobStoreContext getBlobStoreContext() {
//        return new BlobStoreContextFactory().createContext(provider, props);
//    }
}
