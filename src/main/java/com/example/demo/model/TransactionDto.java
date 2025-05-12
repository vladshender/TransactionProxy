package com.example.demo.model;

import com.opencsv.CSVWriter;
import lombok.Data;
import java.io.IOException;
import java.io.Serializable;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.Objects;

@Data
public class TransactionDto implements Serializable {
    public TransactionDto() {
    }

    public TransactionDto(String[] columns) {
        if (columns == null || columns.length < 38) {
            throw new IllegalArgumentException("Expected 38 columns, got "
                    + (columns == null ? 0 : columns.length));
        }
        try {
            this.id = parseLong(columns[0]);
            this.docVob = parseString(columns[1]);
            this.docVobName = parseString(columns[2]);
            this.docNumber = parseString(columns[3]);
            this.docDate = parseDate(columns[4]);
            this.docVDate = parseDate(columns[5]);
            this.transDate = parseDate(columns[6]);
            this.amount = parseBigDecimal(columns[7]);
            this.amountCop = parseBigDecimal(columns[8]);
            this.currency = parseString(columns[9]);
            this.payerEdrpou = parseString(columns[10]);
            this.payerName = parseString(columns[11]);
            this.payerAccount = parseString(columns[12]);
            this.payerMfo = parseString(columns[13]);
            this.payerBank = parseString(columns[14]);
            this.payerEdrpouFact = parseString(columns[15]);
            this.payerNameFact = parseString(columns[16]);
            this.reciptEdrpou = parseString(columns[17]);
            this.reciptName = parseString(columns[18]);
            this.reciptAccount = parseString(columns[19]);
            this.reciptMfo = parseString(columns[20]);
            this.reciptBank = parseString(columns[21]);
            this.reciptEdrpouFact = parseString(columns[22]);
            this.reciptNameFact = parseString(columns[23]);
            this.paymentDetails = parseString(columns[24]);
            this.docAddAttr = parseString(columns[25]);
            this.regionId = parseInteger(columns[26]);
            this.paymentType = parseString(columns[27]);
            this.paymentData = parseString(columns[28]);
            this.sourceId = parseInteger(columns[29]);
            this.sourceName = parseString(columns[30]);
            this.kekv = parseInteger(columns[31]);
            this.kpk = parseString(columns[32]);
            this.contractId = parseString(columns[33]);
            this.contractNumber = parseString(columns[34]);
            this.budgetCode = parseString(columns[35]);
            this.systemKey = parseString(columns[36]);
            this.systemKeyFf = parseString(columns[37]);
        } catch (NumberFormatException | DateTimeParseException e) {
            throw new IllegalArgumentException("Invalid data format in columns", e);
        }
    }

    private Long parseLong(String value) {
        return value == null || value.trim().isEmpty() ? null : Long.parseLong(value.trim());
    }

    private Integer parseInteger(String value) {
        return value == null || value.trim().isEmpty() ? null : Integer.parseInt(value.trim());
    }

    private BigDecimal parseBigDecimal(String value) {
        return value == null || value.trim().isEmpty() ? null : new BigDecimal(value.trim());
    }

    private LocalDate parseDate(String value) {
        return value == null || value.trim().isEmpty() ? null : LocalDate.parse(value.trim());
    }

    private String parseString(String value) {
        return value == null || value.trim().isEmpty() ? null : value.trim();
    }

    private Long id;
    private String docVob;
    private String docVobName;
    private String docNumber;
    private LocalDate docDate;
    private LocalDate docVDate;
    private LocalDate transDate;
    private BigDecimal amount;
    private BigDecimal amountCop;
    private String currency;
    private String payerEdrpou;
    private String payerName;
    private String payerAccount;
    private String payerMfo;
    private String payerBank;
    private String payerEdrpouFact;
    private String payerNameFact;
    private String reciptEdrpou;
    private String reciptName;
    private String reciptAccount;
    private String reciptMfo;
    private String reciptBank;
    private String reciptEdrpouFact;
    private String reciptNameFact;
    private String paymentDetails;
    private String docAddAttr;
    private Integer regionId;
    private String paymentType;
    private String paymentData;
    private Integer sourceId;
    private String sourceName;
    private Integer kekv;
    private String kpk;
    private String contractId;
    private String contractNumber;
    private String budgetCode;
    private String systemKey;
    private String systemKeyFf;
    private String hash;

    public void generateHash() {
        String data = String.join(",",
                Objects.toString(id, ""),
                Objects.toString(docVob, ""),
                Objects.toString(docVobName, ""),
                Objects.toString(docNumber, ""),
                Objects.toString(docDate, ""),
                Objects.toString(docVDate, ""),
                Objects.toString(transDate, ""),
                Objects.toString(amount, ""),
                Objects.toString(amountCop, ""),
                Objects.toString(currency, ""),
                Objects.toString(payerEdrpou, ""),
                Objects.toString(payerName, ""),
                Objects.toString(payerAccount, ""),
                Objects.toString(payerMfo, ""),
                Objects.toString(payerBank, ""),
                Objects.toString(payerEdrpouFact, ""),
                Objects.toString(payerNameFact, ""),
                Objects.toString(reciptEdrpou, ""),
                Objects.toString(reciptName, ""),
                Objects.toString(reciptAccount, ""),
                Objects.toString(reciptMfo, ""),
                Objects.toString(reciptBank, ""),
                Objects.toString(reciptEdrpouFact, ""),
                Objects.toString(reciptNameFact, ""),
                Objects.toString(paymentDetails, ""),
                Objects.toString(docAddAttr, ""),
                Objects.toString(regionId, ""),
                Objects.toString(paymentType, ""),
                Objects.toString(paymentData, ""),
                Objects.toString(sourceId, ""),
                Objects.toString(sourceName, ""),
                Objects.toString(kekv, ""),
                Objects.toString(kpk, ""),
                Objects.toString(contractId, ""),
                Objects.toString(contractNumber, ""),
                Objects.toString(budgetCode, ""),
                Objects.toString(systemKey, ""),
                Objects.toString(systemKeyFf, "")
        );
        this.hash = calculateSHA256(data);
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

    public String toCsv() {
        try (StringWriter stringWriter = new StringWriter();
             CSVWriter csvWriter = new CSVWriter(stringWriter, ',',
                     CSVWriter.DEFAULT_QUOTE_CHARACTER,
                     CSVWriter.DEFAULT_ESCAPE_CHARACTER, "")) {
            String[] row = new String[]{
                    Objects.toString(id, ""),
                    Objects.toString(docVob, ""),
                    Objects.toString(docVobName, ""),
                    Objects.toString(docNumber, ""),
                    Objects.toString(docDate, ""),
                    Objects.toString(docVDate, ""),
                    Objects.toString(transDate, ""),
                    Objects.toString(amount, ""),
                    Objects.toString(amountCop, ""),
                    Objects.toString(currency, ""),
                    Objects.toString(payerEdrpou, ""),
                    Objects.toString(payerName, ""),
                    Objects.toString(payerAccount, ""),
                    Objects.toString(payerMfo, ""),
                    Objects.toString(payerBank, ""),
                    Objects.toString(payerEdrpouFact, ""),
                    Objects.toString(payerNameFact, ""),
                    Objects.toString(reciptEdrpou, ""),
                    Objects.toString(reciptName, ""),
                    Objects.toString(reciptAccount, ""),
                    Objects.toString(reciptMfo, ""),
                    Objects.toString(reciptBank, ""),
                    Objects.toString(reciptEdrpouFact, ""),
                    Objects.toString(reciptNameFact, ""),
                    Objects.toString(paymentDetails, ""),
                    Objects.toString(docAddAttr, ""),
                    Objects.toString(regionId, ""),
                    Objects.toString(paymentType, ""),
                    Objects.toString(paymentData, ""),
                    Objects.toString(sourceId, ""),
                    Objects.toString(sourceName, ""),
                    Objects.toString(kekv, ""),
                    Objects.toString(kpk, ""),
                    Objects.toString(contractId, ""),
                    Objects.toString(contractNumber, ""),
                    Objects.toString(budgetCode, ""),
                    Objects.toString(systemKey, ""),
                    Objects.toString(systemKeyFf, ""),
                    Objects.toString(hash, "")
            };
            csvWriter.writeNext(row);
            return stringWriter.toString().trim();
        } catch (IOException e) {
            throw new RuntimeException("Error generating CSV", e);
        }
    }
}