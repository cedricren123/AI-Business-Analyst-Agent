import sqlite3
import pandas as pd

TABLE_NAME = "orders"

###QUERIES###

#TOP 5 cateogry by revenue
def get_top_categories_by_revenue(conn: sqlite3.Connection):
    query = """
    SELECT
        product_category,
        ROUND(SUM(total_amount), 2) AS total_revenue
    FROM orders
    GROUP BY product_category
    ORDER BY total_revenue DESC
    LIMIT 5;
    """

    result = pd.read_sql_query(query, conn)

    return result

#Total revenue sum
def get_total_revenue(conn: sqlite3.Connection):
    query = """
    SELECT
        ROUND(SUM(total_amount), 2) AS total_revenue
    """
    result=pd.read_sql_query(query,conn)
    return result

# Average order value
def get_average_order_value(conn: sqlite3.Connection):
    query = """
    SELECT
        ROUND(AVG(total_amount), 2) AS average_order_value
    FROM orders
    """
    result=pd.read_sql_query(query,conn)
    return result

#Monthly Revenue
def get_monthly_revenue(conn: sqlite3.Connection):
    query = """
    SELECT
        ROUND(SUM(total_amount), 2) AS total_revenue
    FROM orders
    GROUP BY month
    """
    result=pd.read_sql_query(query,conn)
    return result

def get_total_order_count(conn: sqlite3.Connection):
    query = """
    SELECT
        COUNT(*) AS total_orders
    FROM orders
    """
    result=pd.read_sql_query(query,conn)
    return result

#Monthly Order Count
def get_monthly_order_count(conn: sqlite3.Connection):
    query = """
    SELECT
        COUNT(*) AS total_orders
    FROM orders
    GROUP BY month
    """
    result=pd.read_sql_query(query,conn)
    return result

#Top 5 cities
def get_top_cities_by_revenue(conn: sqlite3.Connection):
    query = """
    SELECT
        customer_city,
        ROUND(SUM(total_amount), 2) AS total_revenue
    FROM orders
    GROUP BY customer_city
    ORDER BY total_revenue DESC
    LIMIT 5;
    """

    result = pd.read_sql_query(query, conn)

    return result

def get_top_revenue_by_payment_method(conn: sqlite3.Connection):
    query = """
    SELECT
        payment_method,
        ROUND(SUM(total_amount), 2) AS total_revenue
    FROM orders
    GROUP BY payment_method
    ORDER BY total_revenue DESC
    LIMIT 5;
    """

    result = pd.read_sql_query(query, conn)

    return result

def get_top_revenue_by_device_type(conn: sqlite3.Connection):
    query = """
    SELECT
        device_type,
        ROUND(SUM(total_amount), 2) AS total_revenue
    FROM orders
    GROUP BY device_type
    ORDER BY total_revenue DESC
    LIMIT 5;
    """

    result = pd.read_sql_query(query, conn)

    return result

def get_returning_vs_new_customer_all_fields_summary(conn: sqlite3.Connection):
    query = """
    SELECT
        customer_type,
        COUNT(*) AS total_orders,
        ROUND(SUM(total_amount), 2) AS total_revenue,
        ROUND(AVG(total_amount), 2) AS average_order_value
    FROM orders
    GROUP BY customer_type
    """
    result = pd.read_sql_query(query, conn)
    return result

def get_monthly_revenue_by_customer_type(conn: sqlite3.Connection):
    query = """
    SELECT
        month,
        customer_type,
        ROUND(SUM(total_amount), 2) AS total_revenue
    FROM orders
    GROUP BY month, customer_type
    ORDER BY month, customer_type
    """
    result = pd.read_sql_query(query, conn)
    return result

def get_monthly_order_count_by_customer_type(conn: sqlite3.Connection):
    query = """
    SELECT
        month,
        customer_type,
        COUNT(*) AS total_orders
    FROM orders
    GROUP BY month, customer_type
    ORDER BY month, customer_type
    """
    result = pd.read_sql_query(query, conn)
    return result

def get_monthly_average_order_value_by_customer_type(conn: sqlite3.Connection):
    query = """
    SELECT
        month,
        customer_type,
        ROUND(AVG(total_amount), 2) AS average_order_value
    FROM orders
    GROUP BY month, customer_type
    ORDER BY month, customer_type
    """
    result = pd.read_sql_query(query, conn)
    return result

def get_monthly_revenue_by_payment_method(conn: sqlite3.Connection):
    query = """
    SELECT
        month,
        payment_method,
        ROUND(SUM(total_amount), 2) AS total_revenue
    FROM orders
    GROUP BY month, payment_method
    ORDER BY month, payment_method
    """
    result = pd.read_sql_query(query, conn)
    return result

def get_monthly_order_count_by_payment_method(conn: sqlite3.Connection):
    query = """
    SELECT
        month,
        payment_method,
        COUNT(*) AS total_orders
    FROM orders
    GROUP BY month, payment_method
    ORDER BY month, payment_method
    """
    result = pd.read_sql_query(query, conn)
    return result

def get_monthly_average_order_value_by_payment_method(conn: sqlite3.Connection):
    query = """
    SELECT
        month,
        payment_method,
        ROUND(AVG(total_amount), 2) AS average_order_value
    FROM orders
    GROUP BY month, payment_method
    ORDER BY month, payment_method
    """
    result = pd.read_sql_query(query, conn)
    return result

def get_monthly_revenue_by_device_type(conn: sqlite3.Connection):
    query = """
    SELECT
        month,
        device_type,
        ROUND(SUM(total_amount), 2) AS total_revenue
    FROM orders
    GROUP BY month, device_type
    ORDER BY month, device_type
    """
    result = pd.read_sql_query(query, conn)
    return result

def get_monthly_order_count_by_device_type(conn: sqlite3.Connection):
    query = """
    SELECT
        month,
        device_type,
        COUNT(*) AS total_orders
    FROM orders
    GROUP BY month, device_type
    ORDER BY month, device_type
    """
    result = pd.read_sql_query(query, conn)
    return result

def get_monthly_average_order_value_by_device_type(conn: sqlite3.Connection):
    query = """
    SELECT
        month,
        device_type,
        ROUND(AVG(total_amount), 2) AS average_order_value
    FROM orders
    GROUP BY month, device_type
    ORDER BY month, device_type
    """
    result = pd.read_sql_query(query, conn)
    return result

def get_monthly_revenue_by_city(conn: sqlite3.Connection):
    query = """
    SELECT
        month,
        customer_city,
        ROUND(SUM(total_amount), 2) AS total_revenue
    FROM orders
    GROUP BY month, customer_city
    ORDER BY month, customer_city
    """
    result = pd.read_sql_query(query, conn)
    return result

def get_monthly_order_count_by_city(conn: sqlite3.Connection):
    query = """
    SELECT
        month,
        customer_city,
        COUNT(*) AS total_orders
    FROM orders
    GROUP BY month, customer_city
    ORDER BY month, customer_city
    """
    result = pd.read_sql_query(query, conn)
    return result

def get_monthly_average_order_value_by_city(conn: sqlite3.Connection):
    query = """
    SELECT
        month,
        customer_city,
        ROUND(AVG(total_amount), 2) AS average_order_value
    FROM orders
    GROUP BY month, customer_city
    ORDER BY month, customer_city
    """
    result = pd.read_sql_query(query, conn)
    return result

def get_top_categories_by_quantity_sold(conn: sqlite3.Connection):
    query = """
    SELECT
        product_category,
        SUM(quantity) AS total_quantity_sold
    FROM orders
    GROUP BY product_category
    ORDER BY total_quantity_sold DESC
    LIMIT 5;
    """

    result = pd.read_sql_query(query, conn)

    return result

def get_average_order_value_by_category(conn: sqlite3.Connection):
    query = """
    SELECT
        product_category,
        ROUND(AVG(total_amount), 2) AS average_order_value
    FROM orders
    GROUP BY product_category
    ORDER BY average_order_value DESC
    """
    result = pd.read_sql_query(query, conn)
    return result

def get_average_rating_by_category(conn: sqlite3.Connection):
    query = """
    SELECT
        product_category,
        ROUND(AVG(customer_rating), 2) AS average_rating
    FROM orders
    GROUP BY product_category
    ORDER BY average_rating DESC
    """
    result = pd.read_sql_query(query, conn)
    return result

def get_average_discount_rate_by_category(conn: sqlite3.Connection):
    query = """
    SELECT
        product_category,
        ROUND(AVG(discount_rate), 4) AS average_discount_rate
    FROM orders
    GROUP BY product_category
    ORDER BY average_discount_rate DESC
    """
    result = pd.read_sql_query(query, conn)
    return result

def get_top_categories_by_return_rate(conn: sqlite3.Connection):
    query = """
    SELECT
        product_category,
        ROUND(AVG(is_returning_customer), 4) AS return_rate
    FROM orders
    GROUP BY product_category
    ORDER BY return_rate DESC
    LIMIT 5;
    """

    result = pd.read_sql_query(query, conn)

    return result

def get_average_delivery_time_by_category(conn: sqlite3.Connection):
    query = """
    SELECT
        product_category,
        ROUND(AVG(delivery_time_days), 2) AS average_delivery_time
    FROM orders
    GROUP BY product_category
    ORDER BY average_delivery_time ASC
    """
    result = pd.read_sql_query(query, conn)
    return result

def get_average_delivery_time_by_city_and_country(conn: sqlite3.Connection):
    query = """
    SELECT
        customer_city,
        customer_country,
        ROUND(AVG(delivery_time_days), 2) AS average_delivery_time
    FROM orders
    GROUP BY customer_city, customer_country
    ORDER BY average_delivery_time ASC
    """
    result = pd.read_sql_query(query, conn)
    return result  

def get_average_rating_by_delivery_time_bracket(conn: sqlite3.Connection):
    query = """
    SELECT
        CASE
            WHEN delivery_time_days <= 2 THEN '0-2 days'
            WHEN delivery_time_days <= 5 THEN '3-5 days'
            WHEN delivery_time_days <= 10 THEN '6-10 days'
            ELSE '10+ days'
        END AS delivery_time_bracket,
        ROUND(AVG(customer_rating), 2) AS average_rating
    FROM orders
    GROUP BY delivery_time_bracket
    ORDER BY average_rating DESC
    """
    result = pd.read_sql_query(query, conn)
    return result

def get_average_rating_by_payment_method(conn: sqlite3.Connection):
    query = """
    SELECT
        payment_method,
        ROUND(AVG(customer_rating), 2) AS average_rating
    FROM orders
    GROUP BY payment_method
    ORDER BY average_rating DESC
    """
    result = pd.read_sql_query(query, conn)
    return result

def get_average_rating_by_device_type(conn:sqlite3.Connection):
    query="""
    SELECT
        device_type,
        ROUND(AVG(customer_rating),2) AS average_rating
    FROM orders
    GROUP BY device_type
    ORDER BY average_rating DESC
    """
    result=pd.read_sql_query(query,conn)
    return result

def get_orders_by_month(conn:sqlite3.Connection):
    query="""
    SELECT
        month,
        SUM(quantity) AS total_orders
    FROM orders
    GROUP BY month
    """
    result = pd.read_sql_query(query,conn)
    return result

def get_average_order_value_by_month(conn:sqlite3.Connection):
    query="""
    SELECT
        month,
        ROUND(AVG(total_amount),2) AS average_order_value
    FROM orders
    GROUP BY month
    """
    result = pd.read_sql_query(query,conn)
    return result

def get_top_categries_performance_by_month(conn:sqlite3.Connection):
    query="""
    SELECT
        month,
        product_category,
        ROUND(SUM(total_amount),2) AS total_revenue
    FROM orders
    GROUP BY month, product_category
    ORDER BY month, total_revenue DESC
    """
    result = pd.read_sql_query(query,conn)
    return result

def get_returning_rate_by_month(conn:sqlite3.Connection):
    query="""
    SELECT
        month,
        ROUND(AVG(is_returning_customer),4) AS returning_rate
    FROM orders
    GROUP BY month
    ORDER BY month
    """
    result = pd.read_sql_query(query,conn)
    return result

def get_customers_with_most_orders(conn:sqlite3.Connection):
    query="""
    SELECT
        customer_id,
        COUNT(*) AS total_orders
    FROM orders
    GROUP BY customer_id
    ORDER BY total_orders DESC
    LIMIT 10
    """
    result = pd.read_sql_query(query,conn)
    return result

def get_average_pages_viewed_by_customer_type(conn:sqlite3.Connection):
    query="""
    SELECT
        customer_type,
        ROUND(AVG(pages_viewed),2) AS average_pages_viewed
    FROM orders
    GROUP BY customer_type
    ORDER BY average_pages_viewed DESC
    """
    result = pd.read_sql_query(query,conn)
    return result

def get_average_time_on_site_by_customer_type(conn:sqlite3.Connection):
    query="""
    SELECT
        customer_type,
        ROUND(AVG(time_on_site_minutes),2) AS average_time_on_site
    FROM orders
    GROUP BY customer_type
    ORDER BY average_time_on_site DESC
    """
    result = pd.read_sql_query(query,conn)
    return result

def get_top_customers_by_total_spend(conn:sqlite3.Connection):
    query="""
    SELECT
        customer_id,
        ROUND(SUM(total_amount),2) AS total_spend
    FROM orders
    GROUP BY customer_id
    ORDER BY total_spend DESC
    LIMIT 10
    """
    result = pd.read_sql_query(query,conn)
    return result

def get_average_spend_by_customer_type(conn:sqlite3.Connection):
    query="""
    SELECT
        customer_type,
        ROUND(AVG(total_amount),2) AS average_spend
    FROM orders
    GROUP BY customer_type
    ORDER BY average_spend DESC
    """
    result = pd.read_sql_query(query,conn)
    return result

def get_top_products_by_customer_rating(conn:sqlite3.Connection):
    query="""
    SELECT
        product_id,
        product_category,
        ROUND(AVG(customer_rating),2) AS average_rating
    FROM orders
    GROUP BY product_id, product_category
    ORDER BY average_rating DESC
    LIMIT 10
    """
    result = pd.read_sql_query(query,conn)
    return result
