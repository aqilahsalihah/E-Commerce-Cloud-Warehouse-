# E-Commerce Cloud Warehouse ETL & Feature Engineering

## Aim of the Project
This project aims to build a simple ETL (Extract, Transform, Load) pipeline and perform automated feature engineering for an e-commerce dataset. The goal is to prepare and enrich data for downstream analytics and machine learning tasks, and to demonstrate the use of cloud data warehousing with Snowflake.

## Techniques Used
1. **Data Loading**  
   Loads customer, product, seller, and order data from CSV files.

2. **Data Exploration**  
   Displays the first few rows of each dataset and checks for duplicates.

3. **Data Transformation**  
   - Filters products to those present in orders.
   - Merges product and seller information into the orders.
   - Calculates order totals.

4. **Entity Set Creation**  
   Uses Featuretools to create an entity set representing relationships between orders, customers, products, and sellers.

5. **Automated Feature Engineering**  
   Applies Deep Feature Synthesis (DFS) to generate new features for orders, customers, sellers, and products.

6. **Feature Selection & Merging**  
   Selects relevant features and merges them back into the main dataframes for further analysis.

7. **Exporting Data**  
   Saves the transformed data to CSV files for use in analytics or machine learning.

## Dataset Source
This is the list of exported tables, with their corresponding row count and file names:

    Customers: 20 rows => Customers.csv
    Orders: 100 rows => Orders.csv
    Products: 100 rows => Products.csv
    Seller: 10 rows => Seller.csv

Generated on 2024-11-15 14:38:47.562339526 UTC by Fabricate v1.1.0

## Tools and Libraries Used
- **Python**: Main programming language.
- **Pandas**: Data manipulation and analysis.
- **Featuretools**: Automated feature engineering (Deep Feature Synthesis).
- **Snowflake Connector**: For uploading processed data to a Snowflake cloud data warehouse.