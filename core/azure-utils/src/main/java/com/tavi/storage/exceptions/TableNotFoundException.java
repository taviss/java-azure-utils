package com.tavi.storage.exceptions;

/**
 * Table not found exception.
 */
public class TableNotFoundException extends QueueException
{
    public TableNotFoundException() {}

    public TableNotFoundException(String message)
    {
        super(message);
    }
}
