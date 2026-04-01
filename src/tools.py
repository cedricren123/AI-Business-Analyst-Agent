import sqlite3

import pandas as pd


TABLE_NAME = "orders"


def run_query(query: str, conn: sqlite3.Connection) -> pd.DataFrame:
    return pd.read_sql_query(query, conn)


def get_top_categories_by_revenue(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        product_category,
        ROUND(SUM(total_amount), 2) AS total_revenue
    FROM {TABLE_NAME}
    GROUP BY product_category
    ORDER BY total_revenue DESC
    LIMIT 5
    """
    return run_query(query, conn)


def get_total_revenue(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        ROUND(SUM(total_amount), 2) AS total_revenue
    FROM {TABLE_NAME}
    """
    return run_query(query, conn)


def get_average_order_value(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        ROUND(AVG(total_amount), 2) AS average_order_value
    FROM {TABLE_NAME}
    """
    return run_query(query, conn)


def get_monthly_revenue(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        year,
        month,
        ROUND(SUM(total_amount), 2) AS total_revenue
    FROM {TABLE_NAME}
    GROUP BY year, month
    ORDER BY year, month
    """
    return run_query(query, conn)


def get_total_order_count(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        COUNT(*) AS total_orders
    FROM {TABLE_NAME}
    """
    return run_query(query, conn)


def get_monthly_order_count(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        year,
        month,
        COUNT(*) AS total_orders
    FROM {TABLE_NAME}
    GROUP BY year, month
    ORDER BY year, month
    """
    return run_query(query, conn)


def get_top_cities_by_revenue(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        city,
        ROUND(SUM(total_amount), 2) AS total_revenue
    FROM {TABLE_NAME}
    GROUP BY city
    ORDER BY total_revenue DESC
    LIMIT 5
    """
    return run_query(query, conn)


def get_top_revenue_by_payment_method(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        payment_method,
        ROUND(SUM(total_amount), 2) AS total_revenue
    FROM {TABLE_NAME}
    GROUP BY payment_method
    ORDER BY total_revenue DESC
    LIMIT 5
    """
    return run_query(query, conn)


def get_top_revenue_by_device_type(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        device_type,
        ROUND(SUM(total_amount), 2) AS total_revenue
    FROM {TABLE_NAME}
    GROUP BY device_type
    ORDER BY total_revenue DESC
    LIMIT 5
    """
    return run_query(query, conn)


def get_returning_vs_new_customer_all_fields_summary(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        customer_type,
        COUNT(*) AS total_orders,
        ROUND(SUM(total_amount), 2) AS total_revenue,
        ROUND(AVG(total_amount), 2) AS average_order_value
    FROM {TABLE_NAME}
    GROUP BY customer_type
    ORDER BY total_revenue DESC
    """
    return run_query(query, conn)


def get_monthly_revenue_by_customer_type(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        year,
        month,
        customer_type,
        ROUND(SUM(total_amount), 2) AS total_revenue
    FROM {TABLE_NAME}
    GROUP BY year, month, customer_type
    ORDER BY year, month, customer_type
    """
    return run_query(query, conn)


def get_monthly_order_count_by_customer_type(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        year,
        month,
        customer_type,
        COUNT(*) AS total_orders
    FROM {TABLE_NAME}
    GROUP BY year, month, customer_type
    ORDER BY year, month, customer_type
    """
    return run_query(query, conn)


def get_monthly_average_order_value_by_customer_type(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        year,
        month,
        customer_type,
        ROUND(AVG(total_amount), 2) AS average_order_value
    FROM {TABLE_NAME}
    GROUP BY year, month, customer_type
    ORDER BY year, month, customer_type
    """
    return run_query(query, conn)


def get_monthly_revenue_by_payment_method(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        year,
        month,
        payment_method,
        ROUND(SUM(total_amount), 2) AS total_revenue
    FROM {TABLE_NAME}
    GROUP BY year, month, payment_method
    ORDER BY year, month, payment_method
    """
    return run_query(query, conn)


def get_monthly_order_count_by_payment_method(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        year,
        month,
        payment_method,
        COUNT(*) AS total_orders
    FROM {TABLE_NAME}
    GROUP BY year, month, payment_method
    ORDER BY year, month, payment_method
    """
    return run_query(query, conn)


def get_monthly_average_order_value_by_payment_method(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        year,
        month,
        payment_method,
        ROUND(AVG(total_amount), 2) AS average_order_value
    FROM {TABLE_NAME}
    GROUP BY year, month, payment_method
    ORDER BY year, month, payment_method
    """
    return run_query(query, conn)


def get_monthly_revenue_by_device_type(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        year,
        month,
        device_type,
        ROUND(SUM(total_amount), 2) AS total_revenue
    FROM {TABLE_NAME}
    GROUP BY year, month, device_type
    ORDER BY year, month, device_type
    """
    return run_query(query, conn)


def get_monthly_order_count_by_device_type(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        year,
        month,
        device_type,
        COUNT(*) AS total_orders
    FROM {TABLE_NAME}
    GROUP BY year, month, device_type
    ORDER BY year, month, device_type
    """
    return run_query(query, conn)


def get_monthly_average_order_value_by_device_type(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        year,
        month,
        device_type,
        ROUND(AVG(total_amount), 2) AS average_order_value
    FROM {TABLE_NAME}
    GROUP BY year, month, device_type
    ORDER BY year, month, device_type
    """
    return run_query(query, conn)


def get_monthly_revenue_by_city(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        year,
        month,
        city,
        ROUND(SUM(total_amount), 2) AS total_revenue
    FROM {TABLE_NAME}
    GROUP BY year, month, city
    ORDER BY year, month, city
    """
    return run_query(query, conn)


def get_monthly_order_count_by_city(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        year,
        month,
        city,
        COUNT(*) AS total_orders
    FROM {TABLE_NAME}
    GROUP BY year, month, city
    ORDER BY year, month, city
    """
    return run_query(query, conn)


def get_monthly_average_order_value_by_city(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        year,
        month,
        city,
        ROUND(AVG(total_amount), 2) AS average_order_value
    FROM {TABLE_NAME}
    GROUP BY year, month, city
    ORDER BY year, month, city
    """
    return run_query(query, conn)


def get_top_categories_by_quantity_sold(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        product_category,
        SUM(quantity) AS total_quantity_sold
    FROM {TABLE_NAME}
    GROUP BY product_category
    ORDER BY total_quantity_sold DESC
    LIMIT 5
    """
    return run_query(query, conn)


def get_average_order_value_by_category(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        product_category,
        ROUND(AVG(total_amount), 2) AS average_order_value
    FROM {TABLE_NAME}
    GROUP BY product_category
    ORDER BY average_order_value DESC
    """
    return run_query(query, conn)


def get_average_rating_by_category(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        product_category,
        ROUND(AVG(customer_rating), 2) AS average_rating
    FROM {TABLE_NAME}
    GROUP BY product_category
    ORDER BY average_rating DESC
    """
    return run_query(query, conn)


def get_average_discount_rate_by_category(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        product_category,
        ROUND(AVG(discount_rate), 4) AS average_discount_rate
    FROM {TABLE_NAME}
    GROUP BY product_category
    ORDER BY average_discount_rate DESC
    """
    return run_query(query, conn)


def get_top_categories_by_return_rate(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        product_category,
        ROUND(AVG(is_returning_customer), 4) AS return_rate
    FROM {TABLE_NAME}
    GROUP BY product_category
    ORDER BY return_rate DESC
    LIMIT 5
    """
    return run_query(query, conn)


def get_average_delivery_time_by_category(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        product_category,
        ROUND(AVG(delivery_time_days), 2) AS average_delivery_time
    FROM {TABLE_NAME}
    GROUP BY product_category
    ORDER BY average_delivery_time ASC
    """
    return run_query(query, conn)


def get_average_delivery_time_by_city(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        city,
        ROUND(AVG(delivery_time_days), 2) AS average_delivery_time
    FROM {TABLE_NAME}
    GROUP BY city
    ORDER BY average_delivery_time ASC
    """
    return run_query(query, conn)


def get_average_delivery_time_by_city_and_country(conn: sqlite3.Connection) -> pd.DataFrame:
    return get_average_delivery_time_by_city(conn)


def get_average_rating_by_delivery_time_bracket(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        CASE
            WHEN delivery_time_days <= 2 THEN '0-2 days'
            WHEN delivery_time_days <= 5 THEN '3-5 days'
            WHEN delivery_time_days <= 10 THEN '6-10 days'
            ELSE '10+ days'
        END AS delivery_time_bracket,
        ROUND(AVG(customer_rating), 2) AS average_rating
    FROM {TABLE_NAME}
    GROUP BY delivery_time_bracket
    ORDER BY average_rating DESC
    """
    return run_query(query, conn)


def get_average_rating_by_payment_method(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        payment_method,
        ROUND(AVG(customer_rating), 2) AS average_rating
    FROM {TABLE_NAME}
    GROUP BY payment_method
    ORDER BY average_rating DESC
    """
    return run_query(query, conn)


def get_average_rating_by_device_type(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        device_type,
        ROUND(AVG(customer_rating), 2) AS average_rating
    FROM {TABLE_NAME}
    GROUP BY device_type
    ORDER BY average_rating DESC
    """
    return run_query(query, conn)


def get_orders_by_month(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        year,
        month,
        COUNT(*) AS total_orders
    FROM {TABLE_NAME}
    GROUP BY year, month
    ORDER BY year, month
    """
    return run_query(query, conn)


def get_average_order_value_by_month(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        year,
        month,
        ROUND(AVG(total_amount), 2) AS average_order_value
    FROM {TABLE_NAME}
    GROUP BY year, month
    ORDER BY year, month
    """
    return run_query(query, conn)


def get_top_categries_performance_by_month(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        year,
        month,
        product_category,
        ROUND(SUM(total_amount), 2) AS total_revenue
    FROM {TABLE_NAME}
    GROUP BY year, month, product_category
    ORDER BY year, month, total_revenue DESC
    """
    return run_query(query, conn)


def get_returning_rate_by_month(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        year,
        month,
        ROUND(AVG(is_returning_customer), 4) AS returning_rate
    FROM {TABLE_NAME}
    GROUP BY year, month
    ORDER BY year, month
    """
    return run_query(query, conn)


def get_customers_with_most_orders(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        customer_id,
        COUNT(*) AS total_orders
    FROM {TABLE_NAME}
    GROUP BY customer_id
    ORDER BY total_orders DESC
    LIMIT 10
    """
    return run_query(query, conn)


def get_average_pages_viewed_by_customer_type(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        customer_type,
        ROUND(AVG(pages_viewed), 2) AS average_pages_viewed
    FROM {TABLE_NAME}
    GROUP BY customer_type
    ORDER BY average_pages_viewed DESC
    """
    return run_query(query, conn)


def get_average_time_on_site_by_customer_type(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        customer_type,
        ROUND(AVG(session_duration_minutes), 2) AS average_time_on_site
    FROM {TABLE_NAME}
    GROUP BY customer_type
    ORDER BY average_time_on_site DESC
    """
    return run_query(query, conn)


def get_top_customers_by_total_spend(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        customer_id,
        ROUND(SUM(total_amount), 2) AS total_spend
    FROM {TABLE_NAME}
    GROUP BY customer_id
    ORDER BY total_spend DESC
    LIMIT 10
    """
    return run_query(query, conn)


def get_average_spend_by_customer_type(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        customer_type,
        ROUND(AVG(total_amount), 2) AS average_spend
    FROM {TABLE_NAME}
    GROUP BY customer_type
    ORDER BY average_spend DESC
    """
    return run_query(query, conn)


def get_top_categories_by_customer_rating(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
    SELECT
        product_category,
        ROUND(AVG(customer_rating), 2) AS average_rating
    FROM {TABLE_NAME}
    GROUP BY product_category
    ORDER BY average_rating DESC
    LIMIT 10
    """
    return run_query(query, conn)


def get_top_products_by_customer_rating(conn: sqlite3.Connection) -> pd.DataFrame:
    return get_top_categories_by_customer_rating(conn)
