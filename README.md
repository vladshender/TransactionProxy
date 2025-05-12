# Transaction Proxy Service


This Spring Boot project is designed to process large CSV files with transaction data. It appends a SHA-256 hash to each transaction line.

## Features
- Processes CSV files with 1,000,000+ transaction rows.
- Adds a SHA-256 hash to each transaction row.
- Supports parallel processing with configurable batch sizes and thread pools.
- Includes robust unit tests using JUnit.
- Uses OpenCSV for reliable CSV parsing and generation.

## Prerequisites
- **Java**: 17 
- **Maven**: 3.6.0 or higher
- **IDE**: IntelliJ IDEA 
