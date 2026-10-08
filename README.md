# Pakistan Investment & Macroeconomic Intelligence Platform

This project builds a simple data pipeline and BI system for investment companies in Pakistan. It collects public economic data and turns it into clean information that analysts can use when studying investment conditions.

-- Test Line

## Project Goal

The system will combine important economic indicators in one place. It will help analysts study inflation, exchange rates, reserves, trade, remittances, interest rates, foreign investment, and fuel prices.

The dashboard will support research and analysis. It will not give direct buy or sell recommendations.

## Data Sources

The main sources are:

- Pakistan Bureau of Statistics (PBS)
- State Bank of Pakistan (SBP)
- Oil and Gas Regulatory Authority (OGRA)

The project will use historical data for the first full load. After that, it will load only new or changed daily, weekly, or monthly records.

## Medallion Architecture

### Bronze
Stores raw files from the original sources with load metadata.

### Silver
Cleans the data, fixes data types, standardizes dates and units, removes duplicates, and handles missing values.

### Gold
Creates simple fact and dimension tables and summary tables for Power BI.

## Business Intelligence

The Power BI dashboard will help answer questions such as:

- Is inflation rising or falling?
- Is the Pakistani rupee becoming stronger or weaker?
- Are foreign exchange reserves improving?
- Which sectors are receiving more foreign investment?
- How are imports, exports, and remittances changing?
- Are fuel costs creating pressure on businesses?

Main visuals will include economic trend charts, sector investment charts, and KPI cards.

## Security

The selected sources are public government datasets and are not expected to contain personal information. If any unexpected personal field appears, it will be removed before the Silver layer.

## Technology

- Python
- Apache Spark / PySpark
- Parquet
- Databricks Free Edition or another free/student cloud environment
- Power BI
- Git and GitHub

## Cost Control

The historical data will be loaded once. Later runs will process only new or changed data. Small samples will be used during development and testing. Compressed file formats such as Parquet will help reduce storage and compute usage.
