package com.tavi.storage.exceptions;

/**
 * Base Blob related exception.
 */
public class BlobException extends Exception
{
    public BlobException() {}

    public BlobException(String message)
    {
        super(message);
    }

    public BlobException(Throwable cause)
    {
        super(cause);
    }

    public BlobException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
