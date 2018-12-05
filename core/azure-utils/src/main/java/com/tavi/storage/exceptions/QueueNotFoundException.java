package com.tavi.storage.exceptions;

/**
 * Queue not found exception.
 */
public class QueueNotFoundException extends QueueException
{
    public QueueNotFoundException() {}

    public QueueNotFoundException(String message)
    {
        super(message);
    }
}
