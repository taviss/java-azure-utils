package com.tavi.storage;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.*;
import com.tavi.storage.exceptions.BlobNotFoundException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.InvalidKeyException;

import static junit.framework.TestCase.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({CloudStorageAccount.class, CloudBlobClient.class, CloudBlobContainer.class,
                    CloudBlockBlob.class, CloudAppendBlob.class, CloudPageBlob.class})
public class BlobUtilsTest
{
    private static final String CONNECTION_STRING = "test";

    private static final String DUMMY_TEXT = "test text";
    private static final String CONTAINER_NAME = "test-container";
    private static final String BLOCK_BOB_NAME = "block-blob";

    private static final String DUMMY_URI_PATH = "https://some.domain/some/path";
    private static final String DUMMY_SAS = "abcdfe";

    private CloudBlobContainer container;

    @Before
    public void setUp() throws URISyntaxException, InvalidKeyException, StorageException {
        final CloudStorageAccount account = mock(CloudStorageAccount.class);
        PowerMockito.mockStatic(CloudStorageAccount.class);
        when(CloudStorageAccount.parse(anyString())).thenReturn(account);

        final CloudBlobClient client = mock(CloudBlobClient.class);
        container = mock(CloudBlobContainer.class);
        when(container.generateSharedAccessSignature(any(SharedAccessBlobPolicy.class), anyString())).thenCallRealMethod();

        when(client.getContainerReference(anyString())).thenReturn(container);
        when(account.createCloudBlobClient()).thenReturn(client);
    }

    public void setUpBlockBlob(boolean exists) throws URISyntaxException, StorageException, InvalidKeyException, IOException {
        final CloudBlockBlob cloudBlockBlob = mock(CloudBlockBlob.class);
        when(cloudBlockBlob.downloadText()).thenReturn(DUMMY_TEXT);
        when(cloudBlockBlob.exists()).thenReturn(exists);

        when(container.getBlockBlobReference(anyString())).thenReturn(cloudBlockBlob);
    }

    public void setUpAppendBlob(boolean exists) throws URISyntaxException, StorageException, InvalidKeyException, IOException {
        final CloudAppendBlob cloudAppendBlob = mock(CloudAppendBlob.class);
        when(cloudAppendBlob.exists()).thenReturn(exists);

        when(container.getAppendBlobReference(anyString())).thenReturn(cloudAppendBlob);
    }

    public void setUpPageBlob(boolean exists) throws URISyntaxException, StorageException, InvalidKeyException, IOException {
        final CloudPageBlob cloudPageBlob = mock(CloudPageBlob.class);
        when(cloudPageBlob.exists()).thenReturn(exists);

        when(container.getPageBlobReference(anyString())).thenReturn(cloudPageBlob);
    }

    public void setUpServerBlob() throws URISyntaxException, StorageException, InvalidKeyException, IOException {
        final CloudBlockBlob cloudBlockBlob = mock(CloudBlockBlob.class);
        when(cloudBlockBlob.downloadText()).thenReturn(DUMMY_TEXT);
        when(cloudBlockBlob.getUri()).thenReturn(new URI(DUMMY_URI_PATH));

        when(container.getBlobReferenceFromServer(any())).thenReturn(cloudBlockBlob);
    }

    @Test
    public void testCreation() throws URISyntaxException, InvalidKeyException {
        final CloudStorageAccount account = mock(CloudStorageAccount.class);
        PowerMockito.mockStatic(CloudStorageAccount.class);
        when(CloudStorageAccount.parse(anyString())).thenReturn(account);
        when(CloudStorageAccount.getDevelopmentStorageAccount()).thenReturn(account);

        BlobUtils bu1 = BlobUtils.from(CONNECTION_STRING);
        assertNotNull(bu1);
        BlobUtils bu2 = BlobUtils.from(CloudStorageAccount.parse(CONNECTION_STRING));
        assertNotNull(bu2);
        BlobUtils bu3 = BlobUtils.fromDevelopmentAccount();
        assertNotNull(bu3);
    }

    @Test
    public void testUploadBlockBlobFromByteArray() throws URISyntaxException, InvalidKeyException, IOException, StorageException {
        BlobUtils bu = BlobUtils.from(CONNECTION_STRING);
        setUpBlockBlob(true);
        byte[] data =  { 0xF, 0xE };
        {
            bu.uploadBlockBlobFromByteArray(CONTAINER_NAME, BLOCK_BOB_NAME, data, 0, data.length, true);
        }
        {
            bu.uploadBlockBlobFromByteArray(CONTAINER_NAME, BLOCK_BOB_NAME, data, 0, data.length, false);
        }
        {
            bu.uploadBlockBlobFromByteArray(CONTAINER_NAME, BLOCK_BOB_NAME, data, true);
        }
        {
            bu.uploadBlockBlobFromByteArray(CONTAINER_NAME, BLOCK_BOB_NAME, data,false);
        }
    }

    @Test
    public void testDownloadToFile() throws URISyntaxException, InvalidKeyException, IOException, StorageException {
        BlobUtils bu = BlobUtils.from(CONNECTION_STRING);
        setUpServerBlob();
        bu.downloadToFile(CONTAINER_NAME, BLOCK_BOB_NAME, new File("test"));
    }

    @Test
    public void testDownloadText() throws URISyntaxException, InvalidKeyException, StorageException, IOException, BlobNotFoundException {
        BlobUtils bu = BlobUtils.from(CONNECTION_STRING);
        {
            setUpBlockBlob(true);
            String text = bu.downloadText(CONTAINER_NAME, BLOCK_BOB_NAME);
            assertNotNull(text);
            assertEquals(DUMMY_TEXT, text);
        }
        {
            try {
                setUpBlockBlob(false);
                String text = bu.downloadText(CONTAINER_NAME, BLOCK_BOB_NAME);
                fail("Expected BlobNotFoundException not thrown!");
            } catch (BlobNotFoundException e) {
                ; // NO-OP; expected
            }
        }
    }

    @Test
    public void testCreateSharedBlobURL() throws URISyntaxException, InvalidKeyException, IOException, StorageException {
        BlobUtils bu = BlobUtils.from(CONNECTION_STRING);
        setUpServerBlob();
        URL url = bu.createSharedBlobURL(CONTAINER_NAME, BLOCK_BOB_NAME, 10, SharedAccessBlobPermissions.READ);
        assertNotNull(url);
        assertEquals(DUMMY_URI_PATH + "?" + DUMMY_SAS, url.toString());

    }
}
