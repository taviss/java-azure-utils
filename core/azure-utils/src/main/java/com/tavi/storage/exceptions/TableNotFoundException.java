package com.tavi.storage.exceptions;

/**
 * Table not found exception.
 */
public class TableNotFoundException extends TableException
{
    public TableNotFoundException() {}

    public TableNotFoundException(String message)
    {
        super(message);
    }
}
