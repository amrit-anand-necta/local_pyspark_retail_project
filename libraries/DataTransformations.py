from pyspark.sql.functions import *

def filter_closed_orders(orders_df):
    return orders_df.where("order_status = 'CLOSED'")

def join_order_customer(orders_df, customers_df):
    return orders_df.join(customers_df, 'customer_id')

def count_state(joined_df):
    return joined_df.groupBy("state").count()