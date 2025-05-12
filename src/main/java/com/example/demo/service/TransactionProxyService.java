package com.example.demo.service;

import java.io.IOException;
import java.io.InputStream;

public interface TransactionProxyService {
    InputStream processTransactions(InputStream inputStream) throws IOException, InterruptedException;
}
