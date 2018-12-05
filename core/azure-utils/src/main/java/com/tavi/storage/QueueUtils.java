package com.tavi.storage;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.queue.CloudQueueClient;
import com.microsoft.azure.storage.queue.CloudQueueMessage;
import com.tavi.storage.exceptions.QueueNotFoundException;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;

/**
 * Helper class for Azure Queue related operations.
 *
 * WARNING: Not thread safe - assumes instances are not shared between threads.
 */
// TODO: Custom StorageException wrapper exceptions or use StorageExceptions?
public class QueueUtils
{
    /** The CloudStorageAccount used for operations. */
    private final CloudStorageAccount account;

    /** The CloudQueueClient used for all the queue operations. */
    private final CloudQueueClient client;

    /**
     * Initializes a QueueUtils from a given connection string.
     * @param connectionString the given connection string.
     * @throws URISyntaxException
     * @throws InvalidKeyException
     */
    private QueueUtils(String connectionString) throws URISyntaxException, InvalidKeyException {
        this(CloudStorageAccount.parse(connectionString));
    }

    /**
     * Initializes a QueueUtils from a given CloudStorageAccount.
     * @param account the storage account to use.
     */
    private QueueUtils(CloudStorageAccount account) {
        this.account = account;
        this.client = account.createCloudQueueClient();
    }


    /**
     * Initializes a QueueUtils from a given connection string.
     * @param connectionString the given connection string.
     * @throws URISyntaxException
     * @throws InvalidKeyException
     */
    public static QueueUtils from(String connectionString) throws URISyntaxException, InvalidKeyException {
        return new QueueUtils(connectionString);
    }

    /**
     * Initializes a QueueUtils from a given CloudStorageAccount.
     * @param account the storage account to use.
     */
    public static QueueUtils from(CloudStorageAccount account) {
        return new QueueUtils(account);
    }

    /**
     * Initializes a QueueUtils using the default development account.
     */
    public static QueueUtils fromDevelopmentAccount() {
        return new QueueUtils(CloudStorageAccount.getDevelopmentStorageAccount());
    }

    /**
     * Adds a message to a queue.
     * @param queueName the queue name.
     * @param content the content (as string).
     * @throws URISyntaxException
     * @throws StorageException
     * @throws QueueNotFoundException
     */
    public void addMessageToQueue(String queueName, String content) throws URISyntaxException, StorageException, QueueNotFoundException {
        CloudQueueMessage message = new CloudQueueMessage(content);
        addMessageToQueue(queueName, message);
    }

    /**
     * Adds a message to a queue.
     * @param queueName the queue name.
     * @param content the message.
     * @throws URISyntaxException
     * @throws StorageException
     * @throws QueueNotFoundException
     */
    public void addMessageToQueue(String queueName, CloudQueueMessage content) throws URISyntaxException, StorageException, QueueNotFoundException {
        CloudQueue queue = getQueueReference(queueName);
        if(queue.exists()) {
            queue.addMessage(content);
        } else {
            throw new QueueNotFoundException("Unable to locate queue " + queueName);
        }
    }

    /**
     * Gets a reference to a queue.
     * @param queueName the queue name.
     * @return a reference to the queue.
     * @throws URISyntaxException
     * @throws StorageException
     */
    public CloudQueue getQueueReference(String queueName) throws URISyntaxException, StorageException {
        return client.getQueueReference(queueName);
    }
}
