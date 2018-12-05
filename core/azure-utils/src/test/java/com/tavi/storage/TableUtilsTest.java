package com.tavi.storage;

import com.microsoft.azure.storage.CloudStorageAccount;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import static junit.framework.TestCase.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.powermock.api.mockito.PowerMockito.mock;

@RunWith(PowerMockRunner.class)
@PrepareForTest(CloudStorageAccount.class)
public class TableUtilsTest
{
    private static final String CONNECTION_STRING = "test";

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
}
