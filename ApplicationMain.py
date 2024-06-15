import sys
from libraries import DataTransformations, DataReader, UtilityFunctions
from pyspark.sql.functions import *


if __name__ == '__main__':

    if len(sys.argv) < 2:
        print('Please specify the environment')
        sys.exit(-1)

    job_run_env = sys.argv[1]

    print("Creating spark session")
          
    spark = UtilityFunctions.get_spark_session(job_run_env)

    print("Created spark session")

    orders_df = DataReader.read_orders(spark, job_run_env)

    orders_filtered = DataTransformations.filter_closed_orders(orders_df)

    customers_df = DataReader.read_customers(spark, job_run_env)

    joined_df = DataTransformations.join_order_customer(orders_filtered, customers_df)

    count_df = DataTransformations.count_state(joined_df)

    count_df.show()
    
    print("Application Ended")