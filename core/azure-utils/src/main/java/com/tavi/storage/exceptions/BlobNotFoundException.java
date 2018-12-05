package com.tavi.storage.exceptions;

/**
 * Blob not found exception.
 */
public class BlobNotFoundException extends BlobException
{
    public BlobNotFoundException() {}

    public BlobNotFoundException(String message)
    {
        super(message);
    }
}
