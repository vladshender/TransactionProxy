package com.example.demo.exception;

public class TransactionFetchingException extends RuntimeException {
    public TransactionFetchingException(String message) {
        super(message);
    }
}
