package com.tavi.storage;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.*;
import com.tavi.storage.exceptions.TableNotFoundException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Arrays;
import java.util.List;

import static junit.framework.TestCase.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({CloudStorageAccount.class, CloudTableClient.class, CloudTable.class})
public class TableUtilsTest
{
    private static final String CONNECTION_STRING = "test";

    private static final String TABLE_NAME = "test-table";
    private static final TableResult OK_RESPONSE = new TableResult(200);
    private static final List<TableServiceEntity> QUERY_RESPONSE = Arrays.asList(
            new TableServiceEntity("1", "0xCAFEBABE"),
            new TableServiceEntity("1", "0xFF"),
            new TableServiceEntity("1", "0xFE")
    );

    private CloudTable table;

    @Before
    public void setUp() throws URISyntaxException, InvalidKeyException, StorageException {
        final CloudStorageAccount account = mock(CloudStorageAccount.class);
        PowerMockito.mockStatic(CloudStorageAccount.class);
        Mockito.when(CloudStorageAccount.parse(anyString())).thenReturn(account);
        Mockito.when(CloudStorageAccount.getDevelopmentStorageAccount()).thenReturn(account);

        final CloudTableClient client = mock(CloudTableClient.class);

        table = mock(CloudTable.class);
        when(table.execute(any(TableOperation.class))).thenReturn(OK_RESPONSE);
        when(table.execute(any(TableQuery.class))).thenReturn(QUERY_RESPONSE);
        when(table.exists()).thenReturn(true);
        when(table.createIfNotExists()).then(new Answer<Boolean>() {
            @Override
            public Boolean answer(InvocationOnMock invocationOnMock) throws Throwable {
                when(table.exists()).thenReturn(true);
                return true;
            }
        });

        when(client.getTableReference(any())).thenReturn(table);

        when(account.createCloudTableClient()).thenReturn(client);
    }

    @Test
    public void testCreation() throws URISyntaxException, InvalidKeyException {
        final CloudStorageAccount account = mock(CloudStorageAccount.class);
        PowerMockito.mockStatic(CloudStorageAccount.class);
        Mockito.when(CloudStorageAccount.parse(anyString())).thenReturn(account);
        Mockito.when(CloudStorageAccount.getDevelopmentStorageAccount()).thenReturn(account);

        TableUtils tu1 = TableUtils.from(CONNECTION_STRING);
        assertNotNull(tu1);
        TableUtils tu2 = TableUtils.from(CloudStorageAccount.parse(CONNECTION_STRING));
        assertNotNull(tu2);
        TableUtils tu3 = TableUtils.fromDevelopmentAccount();
        assertNotNull(tu3);
    }

    @Test
    public void testExecute() throws URISyntaxException, InvalidKeyException, TableNotFoundException, StorageException {
        final TableUtils tableUtils = TableUtils.from(CONNECTION_STRING);
        {
            {
                TableOperation tableOperation = TableOperation.insert(new TableServiceEntity());
                TableResult tableResult = tableUtils.execute(TABLE_NAME, tableOperation);
                assertNotNull(tableResult);
                assertEquals(200, tableResult.getHttpStatusCode());
            }
            {
                when(table.exists()).thenReturn(false);
                try {
                    TableOperation tableOperation = TableOperation.insert(new TableServiceEntity());
                    TableResult tableResult = tableUtils.execute(TABLE_NAME, tableOperation);
                    fail("Expected TableNotFoundException not thrown!");
                } catch (TableNotFoundException e) {
                    ; // NO-OP; expected
                }
                when(table.exists()).thenReturn(true);
            }
        }
        {
            {
                TableOperation tableOperation = TableOperation.insert(new TableServiceEntity());
                TableResult tableResult = tableUtils.execute(TABLE_NAME, true, tableOperation);
                assertNotNull(tableResult);
                assertEquals(200, tableResult.getHttpStatusCode());
            }
            {
                when(table.exists()).thenReturn(false);
                TableOperation tableOperation = TableOperation.insert(new TableServiceEntity());
                TableResult tableResult = tableUtils.execute(TABLE_NAME, true, tableOperation);
                assertNotNull(tableResult);
                assertEquals(200, tableResult.getHttpStatusCode());
                when(table.exists()).thenReturn(true);
            }
        }
    }

    @Test
    public void testQuery() throws URISyntaxException, InvalidKeyException, TableNotFoundException, StorageException {
        final TableUtils tableUtils = TableUtils.from(CONNECTION_STRING);
        {
            TableQuery<TableServiceEntity> query = TableQuery.from(TableServiceEntity.class);
            Iterable<TableServiceEntity> result = tableUtils.query(TABLE_NAME, query);
            assertNotNull(result);
            assertEquals(QUERY_RESPONSE, result);
        }
        {
            when(table.exists()).thenReturn(false);
            try {
                TableQuery<TableServiceEntity> query = TableQuery.from(TableServiceEntity.class);
                Iterable<TableServiceEntity> result = tableUtils.query(TABLE_NAME, query);
                fail("Expected TableNotFoundException not thrown!");
            } catch (TableNotFoundException e) {
                ; // NO-OP; expected
            }
            when(table.exists()).thenReturn(true);
        }
    }

    @Test
    public void testGetTableReference() throws URISyntaxException, InvalidKeyException, StorageException {
        final TableUtils tableUtils = TableUtils.from(CONNECTION_STRING);
        final CloudTable table = tableUtils.getTableReference(TABLE_NAME);
        assertNotNull(table);
        assertTrue(table.exists());
    }
}
