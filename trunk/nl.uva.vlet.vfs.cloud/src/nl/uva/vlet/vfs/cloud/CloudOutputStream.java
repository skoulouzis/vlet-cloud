package nl.uva.vlet.vfs.cloud;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import nl.uva.vlet.ClassLogger;

import org.jclouds.blobstore.AsyncBlobStore;
import org.jclouds.blobstore.domain.Blob;

import com.google.common.util.concurrent.ListenableFuture;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import org.jclouds.blobstore.options.PutOptions;

public class CloudOutputStream extends OutputStream {
    
    private String provider;
    private Properties props;
    private String container;
    private String blobName;
//    private BlobStoreContext blobContext;
    private File bufferFile;
    private OutputStream out;
    static ClassLogger logger;
    
    static {
        logger = ClassLogger.getLogger(CloudOutputStream.class);
        logger.setLevelToDebug();
    }
    private final AsyncBlobStore asyncBlobStore;
    
    CloudOutputStream(String container, String blobName, AsyncBlobStore asyncBlobStore) throws IOException {
        this.container = container;
        this.blobName = blobName;
        
        this.asyncBlobStore = asyncBlobStore;
        
        out = new ByteArrayOutputStream();
        //        out = new FileOutputStream(bufferFile);
        bufferFile = File.createTempFile(this.getClass().getSimpleName(), null);
    }
    
    @Override
    public void write(final int b) throws IOException {
        out.write(b);
    }
    
    private void writeData() throws InterruptedException, ExecutionException {
        try {
            ListenableFuture<Blob> res = asyncBlobStore.getBlob(container, blobName);
            
            Blob blob = res.get();
            if (blob == null) {
                blob = asyncBlobStore.blobBuilder(blobName).build();
            }
            if (out instanceof ByteArrayOutputStream) {
                blob.setPayload(((ByteArrayOutputStream) out).toByteArray());                
            } else if (out instanceof FileOutputStream) {
                blob.setPayload(bufferFile);                
            }
            if (bufferFile.length() > (800 * 1024 * 1024)) {
                asyncBlobStore.getContext().getBlobStore().putBlob(container, blob, PutOptions.Builder.multipart());
            } else {
                asyncBlobStore.getContext().getBlobStore().putBlob(container, blob);
            }
        } finally {
            bufferFile.delete();
        }
    }
    
    @Override
    public void write(final byte[] b, final int off, final int len)
            throws IOException {
        out.write(b, off, len);
    }
    
    @Override
    public void write(final byte[] b) throws IOException {
        out.write(b);
    }
    
    @Override
    public void flush() throws IOException {
        out.flush();
    }
    
    @Override
    public void close() throws IOException {
        
        out.close();
        
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
