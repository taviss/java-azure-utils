package com.tavi.storage;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.*;
import com.tavi.storage.exceptions.TableNotFoundException;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;

/**
 * Helper class for Azure Table related operations.
 *
 * WARNING: Not thread safe - assumes instances are not shared between threads.
 */
// TODO: Custom StorageException wrapper exceptions or use StorageExceptions?
public class TableUtils
{
    /** The CloudStorageAccount used for operations. */
    private final CloudStorageAccount account;

    /** The CloudTableClient used for all the table operations. */
    private final CloudTableClient client;

    /**
     * Initializes a TableUtils from a given connection string.
     * @param connectionString the given connection string.
     * @throws URISyntaxException
     * @throws InvalidKeyException
     */
    private TableUtils(String connectionString) throws URISyntaxException, InvalidKeyException {
        this(CloudStorageAccount.parse(connectionString));
    }

    /**
     * Initializes a TableUtils from a given CloudStorageAccount.
     * @param account the storage account to use.
     */
    private TableUtils(CloudStorageAccount account) {
        this.account = account;
        this.client = account.createCloudTableClient();
    }


    /**
     * Initializes a TableUtils from a given connection string.
     * @param connectionString the given connection string.
     * @throws URISyntaxException
     * @throws InvalidKeyException
     */
    public static TableUtils from(String connectionString) throws URISyntaxException, InvalidKeyException {
        return new TableUtils(connectionString);
    }

    /**
     * Initializes a TableUtils from a given CloudStorageAccount.
     * @param account the storage account to use.
     */
    public static TableUtils from(CloudStorageAccount account) {
        return new TableUtils(account);
    }

    /**
     * Initializes a TableUtils using the default development account.
     */
    public static TableUtils fromDevelopmentAccount() {
        return new TableUtils(CloudStorageAccount.getDevelopmentStorageAccount());
    }

    /**
     * Executes an operation on a given table.
     * @param tableName the table name.
     * @param createIfNotExists create the table if it doesn't exist?
     * @param operation the operation to execute.
     * @return the result.
     * @throws URISyntaxException
     * @throws StorageException
     * @throws TableNotFoundException
     */
    public TableResult execute(String tableName, boolean createIfNotExists, TableOperation operation) throws URISyntaxException, StorageException, TableNotFoundException {
        CloudTable table = getTableReference(tableName);

        if(createIfNotExists)
            table.createIfNotExists();

        if(table.exists()) {
            return table.execute(operation);
        } else {
            throw new TableNotFoundException("Unable to locate table " + tableName);
        }
    }

    /**
     * Executes an operation on a given table. Assumes table already exists and throws if it doesn't.
     * @param tableName the table name.
     * @param operation the operation to execute.
     * @return the result.
     * @throws URISyntaxException
     * @throws StorageException
     * @throws TableNotFoundException
     */
    public TableResult execute(String tableName, TableOperation operation) throws URISyntaxException, StorageException, TableNotFoundException {
        return execute(tableName, false, operation);
    }

    /**
     * Query a table.
     * @param tableName the table name.
     * @param query the query to execute.
     * @param <T> type to be returned.
     * @return the result of the query.
     * @throws URISyntaxException
     * @throws StorageException
     * @throws TableNotFoundException
     */
    public <T extends TableEntity> Iterable<T> query(String tableName, TableQuery<T> query) throws URISyntaxException, StorageException, TableNotFoundException {
        CloudTable table = getTableReference(tableName);
        if(table.exists()) {
            return table.execute(query);
        } else {
            throw new TableNotFoundException("Unable to locate table " + tableName);
        }
    }

    /**
     * Gets a reference to a table.
     * @param tableName the table name.
     * @return a reference to a table.
     * @throws URISyntaxException
     * @throws StorageException
     */
    public CloudTable getTableReference(String tableName) throws URISyntaxException, StorageException {
        return client.getTableReference(tableName);
    }
}
