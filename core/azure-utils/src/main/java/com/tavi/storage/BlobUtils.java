package com.tavi.storage;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.*;
import com.tavi.storage.exceptions.BlobNotFoundException;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.InvalidKeyException;
import java.util.*;

/**
 * Helper class for Azure Blob Storage related operations.
 */
// TODO: Custom StorageException wrapper exceptions or use StorageExceptions?
public class BlobUtils
{
    /** The CloudStorageAccount used for operations. */
    private final CloudStorageAccount account;

    /** The CloudBlobClient used for all the blob operations. */
    private final CloudBlobClient client;

    /**
     * Initializes a BlobUtils from a given connection string.
     * @param connectionString the given connection string.
     * @throws URISyntaxException
     * @throws InvalidKeyException
     */
    private BlobUtils(String connectionString) throws URISyntaxException, InvalidKeyException {
        this(CloudStorageAccount.parse(connectionString));
    }

    /**
     * Initializes a BlobUtils from a given CloudStorageAccount.
     * @param account the storage account to use.
     */
    private BlobUtils(CloudStorageAccount account) {
        this.account = account;
        this.client = account.createCloudBlobClient();
    }


    /**
     * Initializes a BlobUtils from a given connection string.
     * @param connectionString the given connection string.
     * @throws URISyntaxException
     * @throws InvalidKeyException
     */
    public static BlobUtils from(String connectionString) throws URISyntaxException, InvalidKeyException {
        return new BlobUtils(connectionString);
    }

    /**
     * Initializes a BlobUtils from a given CloudStorageAccount.
     * @param account the storage account to use.
     */
    public static BlobUtils from(CloudStorageAccount account) {
        return new BlobUtils(account);
    }

    /**
     * Initializes a BlobUtils using the default development account.
     */
    public static BlobUtils fromDevelopmentAccount() {
        return new BlobUtils(CloudStorageAccount.getDevelopmentStorageAccount());
    }

    /**
     * Uploads a blob from a byte array.
     * @param containerName the container name to upload the blob to.
     * @param blobName the blob name.
     * @param bytes the data to upload.
     * @param offset the offset in the data to upload.
     * @param length the number of bytes in the data to upload.
     * @param deleteIfExists delete the blob if it exists or not.
     * @throws URISyntaxException
     * @throws StorageException
     * @throws IOException
     */
    public void uploadBlockBlobFromByteArray(String containerName, String blobName, byte[] bytes, int offset, int length, boolean deleteIfExists)
            throws URISyntaxException, StorageException, IOException {
        CloudBlobContainer container = client.getContainerReference(containerName);
        CloudBlockBlob blob = container.getBlockBlobReference(blobName);

        if(deleteIfExists)
            blob.deleteIfExists();

        blob.uploadFromByteArray(bytes, offset, length);
    }

    /**
     * Uploads a blob from an entire byte array.
     * @param containerName the container name to upload the blob to.
     * @param blobName the blob name.
     * @param bytes the data to upload.
     * @param deleteIfExists delete the blob if it exists or not.
     * @throws URISyntaxException
     * @throws StorageException
     * @throws IOException
     */
    public void uploadBlockBlobFromByteArray(String containerName, String blobName, byte[] bytes, boolean deleteIfExists)
            throws URISyntaxException, StorageException, IOException {
        uploadBlockBlobFromByteArray(containerName, blobName, bytes, 0, bytes.length, deleteIfExists);
    }

    /**
     * Downloads a blob to a file.
     * @param containerName the container name to download this blob form.
     * @param blobName the blob name.
     * @param file the file to download this blob to.
     * @throws StorageException
     * @throws URISyntaxException
     * @throws IOException
     */
    public void downloadToFile(String containerName, String blobName, File file)
            throws URISyntaxException, StorageException, IOException {
        getBlobReferenceFromServer(containerName, blobName).downloadToFile(file.getAbsolutePath());
    }

    /**
     * Downloads a block blob as text.
     * @param containerName the container name to download this blob form.
     * @param blockBlobName the blob name.
     * @return a String containig the blob's text.
     * @throws BlobNotFoundException
     * @throws StorageException
     * @throws URISyntaxException
     * @throws IOException
     */
    public String downloadText(String containerName, String blockBlobName) throws URISyntaxException, StorageException, IOException, BlobNotFoundException {
        CloudBlockBlob blob = getBlockBlobReference(containerName, blockBlobName);
        if(blob.exists()) {
            return blob.downloadText();
        } else {
            throw new BlobNotFoundException();
        }
    }

    /**
     * Creates a sharable URL for a blob.
     * @param containerName the container name this blob resides in.
     * @param blobName the blob name.
     * @param groupPolicyIdentifier the group policy identifier.
     * @param expireTimeSeconds the amount of time this URL is available for - in seconds.
     * @param permissions permissions to grant.
     * @return
     * @throws URISyntaxException
     * @throws StorageException
     * @throws InvalidKeyException
     * @throws MalformedURLException
     */
    public URL createSharedBlobURL(String containerName, String blobName, String groupPolicyIdentifier, int expireTimeSeconds, EnumSet<SharedAccessBlobPermissions> permissions)
            throws URISyntaxException, StorageException, InvalidKeyException, MalformedURLException {
        CloudBlobContainer container = client.getContainerReference(containerName);
        CloudBlob blob = container.getBlobReferenceFromServer(blobName);

        Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        cal.setTime(new Date());
        cal.add(Calendar.SECOND, expireTimeSeconds);
        SharedAccessBlobPolicy policy = new SharedAccessBlobPolicy();
        policy.setPermissions(permissions);
        policy.setSharedAccessExpiryTime(cal.getTime());
        String sas = container.generateSharedAccessSignature(policy, groupPolicyIdentifier);

        return new URL(blob.getUri() + "?" + sas);
    }

    /**
     * Creates a sharable URL for a blob.
     * @param containerName the container name this blob resides in.
     * @param blobName the blob name.
     * @param groupPolicyIdentifier the group policy identifier.
     * @param expireTimeSeconds the amount of time this URL is available for - in seconds.
     * @param permissions permissions to grant.
     * @return
     * @throws URISyntaxException
     * @throws StorageException
     * @throws InvalidKeyException
     * @throws MalformedURLException
     */
    public URL createSharedBlobURL(String containerName, String blobName, String groupPolicyIdentifier, int expireTimeSeconds, SharedAccessBlobPermissions ... permissions)
            throws URISyntaxException, StorageException, InvalidKeyException, MalformedURLException {
        EnumSet<SharedAccessBlobPermissions> sap = (permissions != null && permissions.length > 0) ? EnumSet.of(permissions[0], permissions) : EnumSet.noneOf(SharedAccessBlobPermissions.class);
        return createSharedBlobURL(containerName, blobName, groupPolicyIdentifier, expireTimeSeconds, sap);
    }

    /**
     * Creates a sharable URL for a blob.
     * @param containerName the container name this blob resides in.
     * @param blobName the blob name.
     * @param expireTimeSeconds the amount of time this URL is available for - in seconds.
     * @param permissions permissions to grant.
     * @return
     * @throws URISyntaxException
     * @throws StorageException
     * @throws InvalidKeyException
     * @throws MalformedURLException
     */
    public URL createSharedBlobURL(String containerName, String blobName, int expireTimeSeconds, EnumSet<SharedAccessBlobPermissions> permissions)
            throws URISyntaxException, StorageException, InvalidKeyException, MalformedURLException {
        return createSharedBlobURL(containerName, blobName, null, expireTimeSeconds, permissions);
    }

    /**
     * Creates a sharable URL for a blob.
     * @param containerName the container name this blob resides in.
     * @param blobName the blob name.
     * @param expireTimeSeconds the amount of time this URL is available for - in seconds.
     * @param permissions permissions to grant.
     * @return
     * @throws URISyntaxException
     * @throws StorageException
     * @throws InvalidKeyException
     * @throws MalformedURLException
     */
    public URL createSharedBlobURL(String containerName, String blobName, int expireTimeSeconds, SharedAccessBlobPermissions ... permissions)
            throws URISyntaxException, StorageException, InvalidKeyException, MalformedURLException {
        return createSharedBlobURL(containerName, blobName, null, expireTimeSeconds, permissions);
    }

    /**
     * Get properties of a blob.
     * @param containerName the container name this blob resides in.
     * @param blobName the blob name.
     * @return a {@link BlobProperties} object.
     * @throws URISyntaxException
     * @throws StorageException
     */
    public BlobProperties getProperties(String containerName, String blobName) throws URISyntaxException, StorageException {
        return getBlobReferenceFromServer(containerName, blobName).getProperties();
    }

    /**
     * Get metadata of a blob.
     * @param containerName the container name this blob resides in.
     * @param blobName the blob name.
     * @return a Map<String, String> of key - value pairs.
     * @throws URISyntaxException
     * @throws StorageException
     */
    public Map<String, String> getMetadata(String containerName, String blobName) throws URISyntaxException, StorageException {
        return getBlobReferenceFromServer(containerName, blobName).getMetadata();
    }

    /**
     * Get a block blob reference.
     * @param containerName the container name this blob resides in.
     * @param blobName the blob name.
     * @return a blob reference.
     * @throws URISyntaxException
     * @throws StorageException
     */
    public CloudBlockBlob getBlockBlobReference(String containerName, String blobName) throws URISyntaxException, StorageException {
        CloudBlobContainer container = client.getContainerReference(containerName);
        return container.getBlockBlobReference(blobName);
    }

    /**
     * Get an append blob reference.
     * @param containerName the container name this blob resides in.
     * @param blobName the blob name.
     * @return a blob reference.
     * @throws URISyntaxException
     * @throws StorageException
     */
    public CloudAppendBlob getAppendBlobReference(String containerName, String blobName) throws URISyntaxException, StorageException {
        CloudBlobContainer container = client.getContainerReference(containerName);
        return container.getAppendBlobReference(blobName);
    }

    /**
     * Get a page blob reference.
     * @param containerName the container name this blob resides in.
     * @param blobName the blob name.
     * @return a blob reference.
     * @throws URISyntaxException
     * @throws StorageException
     */
    public CloudPageBlob getPageBlobReference(String containerName, String blobName) throws URISyntaxException, StorageException {
        CloudBlobContainer container = client.getContainerReference(containerName);
        return container.getPageBlobReference(blobName);
    }

    /**
     * Get a blob reference. This method does a service request to retrieve the blob's metadata and properties.
     * @param containerName the container name this blob resides in.
     * @param blobName the blob name.
     * @return a blob reference.
     * @throws URISyntaxException
     * @throws StorageException
     */
    public CloudBlob getBlobReferenceFromServer(String containerName, String blobName) throws URISyntaxException, StorageException {
        CloudBlobContainer container = client.getContainerReference(containerName);
        // No need to check if exists - will throw if it doesn't.
        return container.getBlobReferenceFromServer(blobName);
    }
}
