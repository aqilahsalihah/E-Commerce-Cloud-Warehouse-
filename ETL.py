import pandas as pd
import json
import featuretools as ft # type: ignore
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# Snowflake Connection
def get_snowflake_connection():
    
    #get config values from config.json
    with open('config.json') as file:
        config = json.load(file)
        
    snowflake_config = config["snowflake"]
    
    conn = snowflake.connector.connect(
        user=snowflake_config["user"],
        password=snowflake_config["password"],
        account=snowflake_config["account"],
        warehouse=snowflake_config["warehouse"],
        database=snowflake_config["database"],
        schema=snowflake_config["schema"]
    )
    print("Connection Established")
    return conn

# Upload DataFrames to Snowflake
def upload_to_snowflake(conn, df, table_name):
    
    # Delete table content if it already exists
    try:
        conn.cursor().execute(f"DELETE FROM {table_name}")
    except Exception as e:
        print(f"Failed to delete table {table_name}: {e}")
    
    # Create table
    success, nchunks, nrows, _ = write_pandas(conn, df, table_name)
    if success:
        print(f"Uploaded {nrows} rows to {table_name}")
    else:
        print(f"Failed to upload data to {table_name}")

# Extract Data
customers = pd.read_csv('Ecommerce/Customers.csv')
products = pd.read_csv('Ecommerce/Products.csv')
sellers = pd.read_csv('Ecommerce/Seller.csv')
orders = pd.read_csv('Ecommerce/Orders.csv')

# Transform Data
products = products[products['ProductID'].isin(orders['ProductID'])]
orders = orders.merge(products[['ProductID', 'SellerID','ProductPrice']], on='ProductID', how='left')
orders['OrderTotal'] = orders['ProductPrice'] * orders['OrderQuantity']

print(orders.info())
print(products.info())

## Create Entity sets
es = ft.EntitySet('orders')

es.add_dataframe(dataframe_name="orders",
                 dataframe=orders,
                 index="OrderID",
                 time_index="OrderDate")

es.add_dataframe(dataframe_name="customer",
                 dataframe=customers,
                 index="CustomerID",
                 time_index="CustomerSignupDate")

es.add_dataframe(dataframe_name= 'products',
                 dataframe= products,
                 index= 'ProductID')

es.add_dataframe(dataframe_name= 'sellers',
                 dataframe= sellers,
                 index= 'SellerID')   

es.add_relationship("products", "ProductID", "orders", "ProductID")
es.add_relationship("customer", "CustomerID", "orders", "CustomerID")
es.add_relationship("sellers", "SellerID", "orders", "SellerID")
print(es)

## Deep Feature Synthesis
features, feature_defs = ft.dfs(entityset=es,
                                target_dataframe_name="orders",
                                agg_primitives=["sum", "mean", "mode",'count'])

## merge new features (orders)
selected_features = ["WEEKDAY(OrderDate)", "YEAR(OrderDate)", "MONTH(OrderDate)", 
                     "WEEKDAY(ShipDate)", "MONTH(ShipDate)", "YEAR(ShipDate)"]
merge_features = features[selected_features]
merge_features.rename(columns={"WEEKDAY(OrderDate)":"OrderWeekday", "YEAR(OrderDate)":"OrderYear", 
                               "MONTH(OrderDate)":"OrderMonth", "WEEKDAY(ShipDate)":"ShipWeekday", 
                               "MONTH(ShipDate)":"ShipMonth", "YEAR(ShipDate)":"ShipYear"}, inplace=True)
orders = orders.merge(merge_features, left_on="OrderID", right_index=True)
orders.sort_values("OrderDate", inplace=True)

## merge new features (customer)
selected_features = ["CustomerID","customer.COUNT(orders)","customer.SUM(orders.OrderQuantity)", 
                     "customer.MEAN(orders.OrderTotal)"]
merge_features = features[selected_features]
merge_features = merge_features.groupby("CustomerID").mean()
merge_features.rename(columns={"customer.COUNT(orders)":"CustomerOrderCount", 
                               "customer.SUM(orders.OrderQuantity)":"TotalItemsPurchased", 
                               "customer.MEAN(orders.OrderTotal)":"AverageSpent"}, inplace=True)
customers = customers.merge(merge_features, left_on="CustomerID", right_index=True)
customers.sort_values("CustomerID", inplace=True)

## merge new features (seller)
selected_features = ["SellerID", "sellers.COUNT(orders)", "sellers.SUM(orders.OrderQuantity)", 
                     "sellers.SUM(orders.OrderTotal)"]
merge_features = features[selected_features]
merge_features = merge_features.groupby("SellerID").mean()
merge_features.rename(columns={"sellers.COUNT(orders)":"SellerOrderCount", 
                               "sellers.SUM(orders.OrderQuantity)":"TotalItemsSold", 
                               "sellers.SUM(orders.OrderTotal)":"TotalRevenue"}, inplace=True)
sellers = sellers.merge(merge_features, left_on="SellerID", right_index=True)
sellers.sort_values("SellerID", inplace=True)

## merge new features (products)
selected_features = ["ProductID","products.SUM(orders.OrderQuantity)","products.SUM(orders.OrderTotal)"]
merge_features = features[selected_features]
merge_features = merge_features.groupby("ProductID").mean()
merge_features.rename(columns={"products.SUM(orders.OrderQuantity)":"QuantitySold", 
                               "products.SUM(orders.OrderTotal)":"TotalRevenue"}, inplace=True)
products = products.merge(merge_features, left_on="ProductID", right_index=True)
products.sort_values("ProductID", inplace=True)

#change column names to uppercase
orders.columns = orders.columns.str.upper()
customers.columns = customers.columns.str.upper()
sellers.columns = sellers.columns.str.upper()
products.columns = products.columns.str.upper()



#Load Upload DataFrames to Snowflake
print(orders.info())
print(customers.info())
print(sellers.info())
print(products.info())


conn = get_snowflake_connection()

orders.to_csv('Data/orders_transformed.csv', index=False)
customers.to_csv('Data/customers_transformed.csv', index=False)
sellers.to_csv('Data/sellers_transformed.csv', index=False)
products.to_csv('Data/products_transformed.csv', index=False)

upload_to_snowflake(conn, orders, "ORDERS")
upload_to_snowflake(conn, customers, "CUSTOMERS")
upload_to_snowflake(conn, sellers, "SELLERS")
upload_to_snowflake(conn, products, "PRODUCTS")

conn.close()
print("ETL process completed successfully")