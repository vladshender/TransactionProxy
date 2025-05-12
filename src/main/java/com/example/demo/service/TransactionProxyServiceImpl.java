package com.example.demo.service;

import com.example.demo.model.TransactionDto;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.stereotype.Service;

@Service
public class TransactionProxyServiceImpl implements TransactionProxyService {
    private static final int THREAD_POOL_SIZE = Math.min(Runtime.getRuntime().availableProcessors(), 4);
    private static final int BATCH_SIZE = 5000;
    private static final int PIPE_BUFFER_SIZE = 65536;
    private final ThreadPoolExecutor executorService;

    public TransactionProxyServiceImpl() {
        this.executorService = new ThreadPoolExecutor(
                THREAD_POOL_SIZE,
                THREAD_POOL_SIZE,
                60L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(THREAD_POOL_SIZE * 2),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }

    public InputStream processTransactions(InputStream inputStream) throws IOException {
        PipedOutputStream pipedOutputStream = new PipedOutputStream();
        PipedInputStream pipedInputStream = new PipedInputStream(pipedOutputStream, PIPE_BUFFER_SIZE);
        AtomicBoolean hasErrors = new AtomicBoolean(false);
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        CompletableFuture.runAsync(() -> {
            try (CSVReader csvReader
                         = new CSVReaderBuilder(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
                    .withCSVParser(new CSVParserBuilder().withSeparator(',').build())
                    .build();
                 BufferedWriter writer
                         = new BufferedWriter(new OutputStreamWriter(pipedOutputStream, StandardCharsets.UTF_8))) {

                // Читання заголовка
                String[] header = csvReader.readNext();
                if (header != null) {
                    if (header.length < 38) {
                        throw new IOException("Invalid CSV header: expected at least 38 columns, got "
                                + header.length);
                    }
                    writer.write(String.join(",", header) + ",hash\n");
                } else {
                    throw new IOException("Empty CSV file");
                }

                List<String[]> batch = new ArrayList<>(BATCH_SIZE);
                String[] line;
                while ((line = csvReader.readNext()) != null) {
                    batch.add(line);
                    if (batch.size() >= BATCH_SIZE) {
                        futures.add(processBatchAsync(batch, writer, hasErrors));
                        batch = new ArrayList<>(BATCH_SIZE);
                    }
                }

                if (!batch.isEmpty()) {
                    futures.add(processBatchAsync(batch, writer, hasErrors));
                }

                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(30,
                        TimeUnit.SECONDS);

                if (hasErrors.get()) {
                    throw new IOException("Errors occurred during batch processing");
                }
            } catch (IOException e) {
                hasErrors.set(true);
                throw new RuntimeException("Error processing transactions", e);
            } catch (Exception e) {
                hasErrors.set(true);
                throw new RuntimeException("Unexpected error during transaction processing", e);
            } finally {
                try {
                    pipedOutputStream.close();
                } catch (IOException e) {
                    throw new RuntimeException("Error closing output stream", e);
                }
            }
        }, executorService);

        return pipedInputStream;
    }

    private CompletableFuture<Void> processBatchAsync(List<String[]> batch,
                                                      BufferedWriter writer,
                                                      AtomicBoolean hasErrors) {
        return CompletableFuture.runAsync(() -> {
            try {
                for (String[] columns : batch) {
                    String processedLine = processTransactionLine(columns);
                    synchronized (writer) {
                        writer.write(processedLine + "\n");
                    }
                }
                synchronized (writer) {
                    writer.flush();
                }
            } catch (IOException e) {
                hasErrors.set(true);
                throw new RuntimeException("Error processing batch", e);
            }
        }, executorService);
    }

    private String processTransactionLine(String[] columns) {
        try {
            if (columns.length < 38) {
                throw new IllegalArgumentException("Expected 38 columns, got "
                        + columns.length);
            }

            TransactionDto transaction = new TransactionDto(columns);
            transaction.generateHash();
            return transaction.toCsv();
        } catch (Exception e) {
            String csvLine = String.join(",", columns);
            String hash = calculateSHA256(csvLine);
            return csvLine + "," + hash;
        }
    }

    private String calculateSHA256(String data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(data.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : hashBytes) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Error calculating SHA-256 hash", e);
        }
    }

    public void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
