package nl.uva.vlet.vfs.cloud;

public class Constants {

//	public static final String[] SCHEMES = { "swift", "s3", "eucalyptus",
//			"synaptic", "azureblob", "cloudonestorage", "cloudfiles-us",
//			"cloudfiles-uk", "ninefold-storage", "aws-s3", "googlestorage",
//			"scaleup-storage", "hosteurope-storage", "tiscali-storage",
//			"in-memory" };
    public static final String FILESYSTEM_SCHME = "filesystem";
    public static final String[] SCHEMES = {"atmos", "cloudfiles-uk", "eucalyptus",
        "partnercloud", "synaptic-storage", "transient", "ninefold-storage",
        "swift", "swiftssl", "hpcloud-objectstorage-lvs", "cloudonestorage", "cloudfiles-us",
        "azureblob", "walrus", "aws-s3", "cloudfiles", FILESYSTEM_SCHME};
    public static final int TIME_OUT = 600000;
}
