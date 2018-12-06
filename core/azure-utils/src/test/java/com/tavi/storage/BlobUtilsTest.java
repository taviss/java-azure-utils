package com.tavi.storage;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.*;
import com.tavi.storage.exceptions.BlobNotFoundException;
import com.tavi.storage.exceptions.ContainerNotFoundException;
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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({CloudStorageAccount.class, CloudBlobClient.class, CloudBlobContainer.class,
                    CloudBlockBlob.class, CloudAppendBlob.class, CloudPageBlob.class, BlobProperties.class})
public class BlobUtilsTest
{
    private static final String CONNECTION_STRING = "test";

    private static final String DUMMY_TEXT = "test text";
    private static final String CONTAINER_NAME = "test-container";
    private static final String BLOCK_BOB_NAME = "block-blob";

    private static final String DUMMY_URI_PATH = "https://some.domain/some/path";
    private static final String DUMMY_SAS = "abcdfe";

    private static final HashMap<String, String> METADATA = new HashMap<>(2);
    private static final String M_K1 = "key1";
    private static final String M_K2 = "key2";
    private static final String M_V1 = "value1";
    private static final String M_V2 = "value2";

    static
    {
        METADATA.put(M_K1, M_V1);
        METADATA.put(M_K2, M_V2);
    }

    private CloudBlobContainer container;

    @Before
    public void setUp() throws URISyntaxException, InvalidKeyException, StorageException {
        final CloudStorageAccount account = mock(CloudStorageAccount.class);
        PowerMockito.mockStatic(CloudStorageAccount.class);
        when(CloudStorageAccount.parse(anyString())).thenReturn(account);

        final CloudBlobClient client = mock(CloudBlobClient.class);
        container = mock(CloudBlobContainer.class);
        when(container.generateSharedAccessSignature(any(SharedAccessBlobPolicy.class), any())).thenReturn(DUMMY_SAS);
        when(container.exists()).thenReturn(true);

        when(client.getContainerReference(anyString())).thenReturn(container);
        when(account.createCloudBlobClient()).thenReturn(client);
    }

    public void setUpBlockBlob(boolean exists) throws URISyntaxException, StorageException, IOException {
        final CloudBlockBlob cloudBlockBlob = mock(CloudBlockBlob.class);
        when(cloudBlockBlob.downloadText()).thenReturn(DUMMY_TEXT);
        when(cloudBlockBlob.exists()).thenReturn(exists);

        when(container.getBlockBlobReference(anyString())).thenReturn(cloudBlockBlob);
    }

    public void setUpAppendBlob(boolean exists) throws URISyntaxException, StorageException {
        final CloudAppendBlob cloudAppendBlob = mock(CloudAppendBlob.class);
        when(cloudAppendBlob.exists()).thenReturn(exists);

        when(container.getAppendBlobReference(anyString())).thenReturn(cloudAppendBlob);
    }

    public void setUpPageBlob(boolean exists) throws URISyntaxException, StorageException {
        final CloudPageBlob cloudPageBlob = mock(CloudPageBlob.class);
        when(cloudPageBlob.exists()).thenReturn(exists);

        when(container.getPageBlobReference(anyString())).thenReturn(cloudPageBlob);
    }

    public void setUpServerBlob() throws URISyntaxException, StorageException, IOException {
        final CloudBlockBlob cloudBlockBlob = mock(CloudBlockBlob.class);
        when(cloudBlockBlob.downloadText()).thenReturn(DUMMY_TEXT);
        when(cloudBlockBlob.getUri()).thenReturn(new URI(DUMMY_URI_PATH));
        when(cloudBlockBlob.exists()).thenReturn(true);

        final BlobProperties blobProperties = mock(BlobProperties.class);
        when(blobProperties.getBlobType()).thenReturn(BlobType.BLOCK_BLOB);
        when(cloudBlockBlob.getProperties()).thenReturn(blobProperties);

        when(cloudBlockBlob.getMetadata()).thenReturn(METADATA);

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
    public void testCreateSharedBlobURL() throws URISyntaxException, InvalidKeyException, IOException, StorageException, ContainerNotFoundException {
        BlobUtils bu = BlobUtils.from(CONNECTION_STRING);
        setUpServerBlob();
        {
            URL url = bu.createSharedBlobURL(CONTAINER_NAME, BLOCK_BOB_NAME, 10, SharedAccessBlobPermissions.READ);
            assertNotNull(url);
            assertEquals(DUMMY_URI_PATH + "?" + DUMMY_SAS, url.toString());
        }
        {
            when(container.exists()).thenReturn(false);

            try {
                URL url = bu.createSharedBlobURL(CONTAINER_NAME, BLOCK_BOB_NAME, 10, SharedAccessBlobPermissions.READ);
                fail("Expected ContainerNotFoundException not thrown!");
            } catch (ContainerNotFoundException e) {
                ; // NO-OP; expected
            }

            when(container.exists()).thenReturn(true);
        }
        {
            URL url = bu.createSharedBlobURL(CONTAINER_NAME, BLOCK_BOB_NAME, 10, EnumSet.of(SharedAccessBlobPermissions.READ));
            assertNotNull(url);
            assertEquals(DUMMY_URI_PATH + "?" + DUMMY_SAS, url.toString());
        }
        {
            URL url = bu.createSharedBlobURL(CONTAINER_NAME, BLOCK_BOB_NAME, "test", 10, EnumSet.of(SharedAccessBlobPermissions.READ));
            assertNotNull(url);
            assertEquals(DUMMY_URI_PATH + "?" + DUMMY_SAS, url.toString());
        }
        {
            URL url = bu.createSharedBlobURL(CONTAINER_NAME, BLOCK_BOB_NAME, "test", 10, SharedAccessBlobPermissions.READ);
            assertNotNull(url);
            assertEquals(DUMMY_URI_PATH + "?" + DUMMY_SAS, url.toString());
        }
    }

    @Test
    public void testGetProperties() throws StorageException, IOException, URISyntaxException, InvalidKeyException {
        BlobUtils bu = BlobUtils.from(CONNECTION_STRING);
        setUpServerBlob();
        BlobProperties blobProperties = bu.getProperties(CONTAINER_NAME, BLOCK_BOB_NAME);
        assertNotNull(blobProperties);
        assertEquals(BlobType.BLOCK_BLOB, blobProperties.getBlobType());
    }

    @Test
    public void testGetMetadata() throws StorageException, IOException, URISyntaxException, InvalidKeyException {
        BlobUtils bu = BlobUtils.from(CONNECTION_STRING);
        setUpServerBlob();
        Map<String, String> metadata = bu.getMetadata(CONTAINER_NAME, BLOCK_BOB_NAME);
        assertNotNull(metadata);
        assertEquals(M_V1, metadata.get(M_K1));
        assertEquals(M_V2, metadata.get(M_K2));
    }

    @Test
    public void testGetBlobReference() throws URISyntaxException, InvalidKeyException, IOException, StorageException {
        BlobUtils bu = BlobUtils.from(CONNECTION_STRING);
        {
            {
                setUpBlockBlob(true);
                CloudBlob cloudBlob = bu.getBlockBlobReference(CONTAINER_NAME, BLOCK_BOB_NAME);
                assertNotNull(cloudBlob);
                assertTrue(cloudBlob.exists());
            }
            {
                setUpBlockBlob(false);
                CloudBlob cloudBlob = bu.getBlockBlobReference(CONTAINER_NAME, BLOCK_BOB_NAME);
                assertNotNull(cloudBlob);
                assertTrue(!cloudBlob.exists());
            }
        }
        {
            setUpServerBlob();
            CloudBlob cloudBlob = bu.getBlobReferenceFromServer(CONTAINER_NAME, BLOCK_BOB_NAME);
            assertNotNull(cloudBlob);
            assertTrue(cloudBlob.exists());
        }
        {
            {
                setUpAppendBlob(true);
                CloudBlob cloudBlob = bu.getAppendBlobReference(CONTAINER_NAME, BLOCK_BOB_NAME);
                assertNotNull(cloudBlob);
                assertTrue(cloudBlob.exists());
            }
            {
                setUpAppendBlob(false);
                CloudBlob cloudBlob = bu.getAppendBlobReference(CONTAINER_NAME, BLOCK_BOB_NAME);
                assertNotNull(cloudBlob);
                assertTrue(!cloudBlob.exists());
            }
        }
        {
            {
                setUpPageBlob(true);
                CloudBlob cloudBlob = bu.getPageBlobReference(CONTAINER_NAME, BLOCK_BOB_NAME);
                assertNotNull(cloudBlob);
                assertTrue(cloudBlob.exists());
            }
            {
                setUpPageBlob(false);
                CloudBlob cloudBlob = bu.getPageBlobReference(CONTAINER_NAME, BLOCK_BOB_NAME);
                assertNotNull(cloudBlob);
                assertTrue(!cloudBlob.exists());
            }
        }
    }
}
