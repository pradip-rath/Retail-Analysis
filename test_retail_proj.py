import pytest
from lib.utils import get_spark_session
from lib.dataReader import read_customers,read_orders

def test_read_customers_df():
    spark = get_spark_session("LOCAL")
    customer_count = read_customers(spark,"LOCAL").count()
    assert customer_count == 12435

def test_read_orders_df():
    spark = get_spark_session("LOCAL")
    orders_count = read_orders(spark,"LOCAL").count()
    assert orders_count == 68883