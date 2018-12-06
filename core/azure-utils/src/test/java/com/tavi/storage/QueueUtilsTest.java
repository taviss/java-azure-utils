package com.tavi.storage;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.queue.CloudQueueClient;
import com.microsoft.azure.storage.queue.CloudQueueMessage;
import com.tavi.storage.exceptions.QueueNotFoundException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import static junit.framework.TestCase.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({CloudStorageAccount.class, CloudQueue.class, CloudQueueClient.class})
public class QueueUtilsTest
{
    private static final String CONNECTION_STRING = "test";

    private static final String QUEUE_NAME = "test-queue";
    private static final String MESSAGE_TEXT = "test message";
    private static final CloudQueueMessage MESSAGE = new CloudQueueMessage(MESSAGE_TEXT);

    private CloudQueue queue;

    @Before
    public void setUp() throws URISyntaxException, InvalidKeyException, StorageException {
        final CloudStorageAccount account = mock(CloudStorageAccount.class);
        PowerMockito.mockStatic(CloudStorageAccount.class);
        Mockito.when(CloudStorageAccount.parse(anyString())).thenReturn(account);
        Mockito.when(CloudStorageAccount.getDevelopmentStorageAccount()).thenReturn(account);

        queue = mock(CloudQueue.class);
        when(queue.exists()).thenReturn(true);

        final CloudQueueClient cloudQueueClient = mock(CloudQueueClient.class);
        when(cloudQueueClient.getQueueReference(any())).thenReturn(queue);
        when(account.createCloudQueueClient()).thenReturn(cloudQueueClient);
    }

    @Test
    public void testCreation() throws URISyntaxException, InvalidKeyException {
        final CloudStorageAccount account = mock(CloudStorageAccount.class);
        PowerMockito.mockStatic(CloudStorageAccount.class);
        Mockito.when(CloudStorageAccount.parse(anyString())).thenReturn(account);
        Mockito.when(CloudStorageAccount.getDevelopmentStorageAccount()).thenReturn(account);

        final QueueUtils qu1 = QueueUtils.from(CONNECTION_STRING);
        assertNotNull(qu1);
        final QueueUtils qu2 = QueueUtils.from(CloudStorageAccount.parse(CONNECTION_STRING));
        assertNotNull(qu2);
        final QueueUtils qu3 = QueueUtils.fromDevelopmentAccount();
        assertNotNull(qu3);
    }

    @Test
    public void testAddMessageToQueue() throws URISyntaxException, InvalidKeyException, QueueNotFoundException, StorageException {
        final QueueUtils queueUtils = QueueUtils.from(CONNECTION_STRING);
        {
            {
                queueUtils.addMessageToQueue(QUEUE_NAME, MESSAGE_TEXT);
            }
            {
                when(queue.exists()).thenReturn(false);
                try {
                    queueUtils.addMessageToQueue(QUEUE_NAME, MESSAGE_TEXT);
                    fail("Expected QueueNotFoundException not thrown!");
                } catch (QueueNotFoundException e) {
                    ; // NO-OP; expected
                }
                when(queue.exists()).thenReturn(true);
            }
        }
        {
            {

                queueUtils.addMessageToQueue(QUEUE_NAME, MESSAGE);
            }
            {
                when(queue.exists()).thenReturn(false);
                try {
                    queueUtils.addMessageToQueue(QUEUE_NAME, MESSAGE);
                    fail("Expected QueueNotFoundException not thrown!");
                } catch (QueueNotFoundException e) {
                    ; // NO-OP; expected
                }
                when(queue.exists()).thenReturn(true);
            }
        }
    }

    @Test
    public void testGetQueueReference() throws URISyntaxException, InvalidKeyException, StorageException {
        final QueueUtils queueUtils = QueueUtils.from(CONNECTION_STRING);
        {
            CloudQueue cloudQueue = queueUtils.getQueueReference(QUEUE_NAME);
            assertNotNull(cloudQueue);
            assertTrue(cloudQueue.exists());
        }
    }
}
