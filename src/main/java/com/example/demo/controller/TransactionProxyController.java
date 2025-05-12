package com.example.demo.controller;

import com.example.demo.exception.TransactionFetchingException;
import com.example.demo.service.TransactionProxyService;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.stream.Collectors;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/transactions")
@RequiredArgsConstructor
public class TransactionProxyController {
    private final TransactionProxyService transactionProxyService;

    @GetMapping(value = "/proxy", produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public InputStream getTransactions(
            @RequestParam @Pattern(regexp = "\\d{4}-\\d{2}-\\d{2}") String startDate,
            @RequestParam @Pattern(regexp = "\\d{4}-\\d{2}-\\d{2}") String endDate,
            @RequestParam(required = false) @Size(min = 1) List<@Pattern(regexp = "\\d{8}") String> reciptEdrpous) {
        try {
            InputStream inputStream = fetchTransactionsFromAPI(startDate, endDate, reciptEdrpous);
            return transactionProxyService.processTransactions(inputStream);
        } catch (Exception e) {
            throw new TransactionFetchingException("Failed to fetch transactions from API");
        }
    }

    private InputStream fetchTransactionsFromAPI(String startDate,
                                                 String endDate,
                                                 List<String> reciptEdrpous) throws IOException {
        StringBuilder urlBuilder = new StringBuilder("https://api.spending.gov.ua/api/v2/"
                + "api/transactions/?startdate=")
                .append(startDate)
                .append("&enddate=")
                .append(endDate);

        if (reciptEdrpous != null && !reciptEdrpous.isEmpty()) {
            String edrpousParam = reciptEdrpous.stream()
                    .map(edrpou -> "recipt_edrpous=" + edrpou)
                    .collect(Collectors.joining("&"));
            urlBuilder.append("&").append(edrpousParam);
        }

        URL url = new URL(urlBuilder.toString());
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        try {
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Accept", MediaType.APPLICATION_OCTET_STREAM_VALUE);
            int responseCode = connection.getResponseCode();
            if (responseCode != HttpURLConnection.HTTP_OK) {
                throw new IOException("Unexpected response code: " + responseCode);
            }
            return connection.getInputStream();
        } finally {
            connection.disconnect();
        }
    }
}
