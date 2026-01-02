# 📈 Polygon Stock Data Extractor

## 📖 Overview

This project is a Python-based pipeline that **extracts stock market data from the Polygon API**. It is designed to reliably fetch stock ticker information and prepare it for loading into Snowflake or other data storage solutions.

The pipeline ensures:

* ⚡ Efficient extraction of stock market data
* ⏱️ Safe handling of API rate limits
* 🔄 Automated retries when requests exceed allowed limits

---

## 🔍 Features

* **Polygon API Integration**: Fetches real-time or historical stock ticker data
* **Rate Limit Handling**: Automatically waits and retries when maximum requests per minute are exceeded
* **Data Preparation**: Cleans and structures the extracted data for easy storage or analysis
* **Snowflake Compatibility**: Can be extended to load extracted data into Snowflake tables

---

## 🛠️ Key Benefits

* ✅ Reliable and automated data extraction from Polygon API
* ✅ Handles API throttling gracefully
* ✅ Ready for integration with data warehouses or analytics pipelines
* ✅ Suitable for daily stock market monitoring or historical analysis

---

## ⚠️ Notes

* API requests may be limited:

> `"You've exceeded the maximum requests per minute, please wait or upgrade your subscription to continue"`

* The pipeline includes logic to pause and retry requests automatically to ensure uninterrupted data collection.
* Works best with an active Polygon API key and a valid subscription plan if high-frequency data extraction is required.

---

## 💡 Use Cases

* Daily extraction of stock prices for portfolio analysis
* Historical stock data collection for machine learning or trading models
* Integration with business intelligence tools or data warehouses

---
