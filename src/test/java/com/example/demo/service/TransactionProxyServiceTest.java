package com.example.demo.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

@SpringBootTest
public class TransactionProxyServiceTest {

    @Autowired
    private TransactionProxyService transactionProxyService;

    private static final Logger logger = LoggerFactory.getLogger(TransactionProxyServiceTest.class);

    private String inputCsv;

    @BeforeEach
    public void setUp() {
        inputCsv = "id,doc_vob,doc_vob_name,doc_number,doc_date,doc_v_date,trans_date,amount,amount_cop,currency,payer_edrpou,payer_name,payer_account,payer_mfo,payer_bank,payer_edrpou_fact,payer_name_fact,recipt_edrpou,recipt_name,recipt_account,recipt_mfo,recipt_bank,recipt_edrpou_fact,recipt_name_fact,payment_details,doc_add_attr,region_id,payment_type,payment_data,source_id,source_name,kekv,kpk,contractId,contractNumber,budgetCode,system_key,system_key_ff\n" +
                "292071798,6,,18/2/326,2024-10-30,2024-10-30,2024-10-30,4251932925.5,425193292550,UAH,37567646,\"Виділення асигнувань по ЗФ\",UA618201720000042314000000000,,,,,00013480,Мінфін,UA608201720343340023000000141,,,,,\"Процентний. Розп.МФУ від 10.10.xxxx №xxxx\",,28,nsep,,2,ДКСУ,2410,3511350,,,9900000000,176819180,\n" +
                "291944385,6,,140/944,2024-10-29,2024-10-29,2024-10-29,894.27,89427,UAH,37567646,\"Виділення асигнувань по ЗФ\",UA618201720000042314000000000,,,,,00013480,Мінфін,UA608201720343340023000000141,,,,,\"Комысыя ыною.Рах-фак.НБУ выд 15.10.24 И117\",,28,nsep,,2,ДКСУ,2420,3511350,,,9900000000,176682599,\n";
    }

    @Test
    public void testProcessTransactions() throws Exception {
        InputStream inputStream = new ByteArrayInputStream(inputCsv.getBytes(StandardCharsets.UTF_8));

        InputStream processedStream = transactionProxyService.processTransactions(inputStream);

        List<String> outputRows;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(processedStream, StandardCharsets.UTF_8))) {
            outputRows = reader.lines().collect(Collectors.toList());
        }

        // Перевірка результату
        assertEquals(3, outputRows.size(), "Expected 3 rows: header and two transactions");

        // Перевірка заголовка
        String header = outputRows.get(0);
        assertTrue(header.endsWith(",hash"), "Header should end with ',hash'");
        assertEquals(39, header.split(",").length, "Header should have 39 columns (38 + hash)");

        // Перевірка першої транзакції
        String firstRow = outputRows.get(1);
        String[] firstRowColumns = parseCsvRow(firstRow);
        assertEquals(39, firstRowColumns.length, "First transaction row should have 39 columns");
        assertEquals("292071798", firstRowColumns[0], "ID of first transaction should match");
        assertFalse(firstRowColumns[38].isEmpty(), "Hash should not be empty");
        assertEquals(64, firstRowColumns[38].length(), "Hash should be 64 characters (SHA-256)");

        // Перевірка другої транзакції
        String secondRow = outputRows.get(2);
        String[] secondRowColumns = parseCsvRow(secondRow);
        assertEquals(39, secondRowColumns.length, "Second transaction row should have 39 columns");
        assertEquals("291944385", secondRowColumns[0], "ID of second transaction should match");
        assertFalse(secondRowColumns[38].isEmpty(), "Hash should not be empty");
        assertEquals(64, secondRowColumns[38].length(), "Hash should be 64 characters (SHA-256)");
    }

    private String[] parseCsvRow(String row) throws Exception {
        try (CSVReader csvReader = new CSVReaderBuilder(new StringReader(row))
                .withCSVParser(new CSVParserBuilder().withSeparator(',').withQuoteChar('"').build())
                .build()) {
            String[] columns = csvReader.readNext();
            logger.debug("Parsed row with {} columns: {}", columns.length, row);
            return columns;
        }
    }
}
