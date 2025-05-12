# Huel-data-task
# Junior Data Engineer Task 

This project is a solution to a data engineering task involving ingestion of nested e-commerce event JSON data into a relational SQL database. The ETL pipeline was implemented using Python 3 and SQLite.

## Task Overview

You are given a schemaless JSON file (compressed) containing nested Kafka-style events related to orders from a digital store. The goal is to:

- Design a relational database schema.
- Parse and normalise the nested data.
- Populate a SQL-based database with the flattened structure.
- Include documentation and basic data validation.

## What This Project Includes

- `huel.py`: A Python script to:
  - Create a normalised schema in SQLite.
  - Parse and flatten the nested order data.
  - Populate multiple relational tables including `orders`, `line_items`, `charges`, `addresses`, and more.
- An **Entity Relationship Diagram (ERD)** illustrating the final schema and how tables relate.


## Prerequisites

- Python 3
- SQLite3

> No external Python packages are required.

## Setup Instructions

1. **Unzip the compressed JSON file**  
   The input `orders.json` file is located in a ZIP archive (`SQlite_Data.zip`). Unzip it to extract `orders.json`:

   ```bash
   unzip SQlite_Data.zip
   ```

2. **Run the ETL script**
   Execute the Python script to build the database:
   ```bash
   python huel.py
   ```

   This will:
    - Parse and normalise orders.json
    - Generate an SQLite database called orders_database.db

3. **Explore the database**
   Open orders_database.db using a tool like DB Browser for SQLite to explore the ingested data.

## Database Schema
The following tables are created and populated:

- **events**: Event metadata
- **stores**: Store information
- **orders**: Core order details
- **line_items**: Items per order
- **charges**: Payment transactions
- **addresses**: Billing and shipping addresses
- **customer_details**: Customer info
- **shipping_lines**: Shipping-related charges
- **tax_lines**: Tax breakdown for items and shipping
- **discount_codes**: Applied discount codes
- **applied_discounts**: Detailed discount data
- **applied_discount_targets**: Targets of applied discounts

## Entity Relationship Diagram (ERD)
An ERD image (erd.png) is included in this repository. It visually outlines how the above tables relate to one another and supports the database design decisions made during the task.
