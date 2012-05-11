/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.uva.vlet.vfs.cloud;

import org.jclouds.blobstore.domain.StorageMetadata;

/**
 *
 * @author skoulouz
 */
class CloudMetadataWrapper{
    private final StorageMetadata storageMetadata;
    private long numOfNodes;
    
    public CloudMetadataWrapper(StorageMetadata storageMetadata){
        this.storageMetadata = storageMetadata;
    }

    void setNumOfNodes(long numOfNodes) {
        this.numOfNodes = numOfNodes;
    }

    StorageMetadata getStorageMetadata() {
        return this.storageMetadata;
    }
    
}
