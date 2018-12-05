package com.tavi.storage.exceptions;

/**
 * Base Table related exception.
 */
public class TableException extends Exception
{
    public TableException() {}

    public TableException(String message)
    {
        super(message);
    }

    public TableException(Throwable cause)
    {
        super(cause);
    }

    public TableException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
