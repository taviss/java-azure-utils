package com.tavi.storage.exceptions;

/**
 * Container not found exception.
 */
public class ContainerNotFoundException extends BlobException
{
    public ContainerNotFoundException() {}

    public ContainerNotFoundException(String message)
    {
        super(message);
    }
}