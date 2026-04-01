from contextlib import closing
from typing import Iterable

from langchain.agents import create_agent
from langchain.tools import tool
from langchain_google_genai import ChatGoogleGenerativeAI

import tools as sql_tools
from build_db import connect_db
from prompts import SYSTEM_PROMPT


DEFAULT_MODEL = "gemini-2.5-flash"


def dataframe_to_text(result) -> str:
    if result.empty:
        return "No rows returned."
    return result.to_string(index=False)


def query_database(query_function) -> str:
    with closing(connect_db()) as conn:
        result = query_function(conn)
    return dataframe_to_text(result)


@tool
def total_revenue_tool() -> str:
    """Get total revenue across all orders."""
    return query_database(sql_tools.get_total_revenue)


@tool
def average_order_value_tool() -> str:
    """Get the average order value across all orders."""
    return query_database(sql_tools.get_average_order_value)


@tool
def top_categories_by_revenue_tool() -> str:
    """Get the top product categories by revenue."""
    return query_database(sql_tools.get_top_categories_by_revenue)


@tool
def top_cities_by_revenue_tool() -> str:
    """Get the top cities by revenue."""
    return query_database(sql_tools.get_top_cities_by_revenue)


@tool
def returning_vs_new_customer_summary_tool() -> str:
    """Compare returning and new customers by order count, total revenue, and average order value."""
    return query_database(sql_tools.get_returning_vs_new_customer_all_fields_summary)

@tool
def top_customers_by_total_spend_tool() -> str:
    """Get the top customers by total spend."""
    return query_database(sql_tools.get_top_customers_by_total_spend)


@tool
def monthly_revenue_tool() -> str:
    """Get monthly revenue by year and month."""
    return query_database(sql_tools.get_monthly_revenue)


def get_agent_tools() -> list:
    return [
        total_revenue_tool,
        average_order_value_tool,
        top_categories_by_revenue_tool,
        top_cities_by_revenue_tool,
        returning_vs_new_customer_summary_tool,
        monthly_revenue_tool,
        top_customers_by_total_spend_tool,
    ]


def build_agent(model_name: str = DEFAULT_MODEL):
    model = ChatGoogleGenerativeAI(model=model_name, temperature=0)
    return create_agent(
        model=model,
        tools=get_agent_tools(),
        system_prompt=SYSTEM_PROMPT,
        name="business_analyst_agent",
    )


def extract_final_text(messages: Iterable) -> str:
    message_list = list(messages)
    if not message_list:
        return "No response returned."

    final_message = message_list[-1]
    content = getattr(final_message, "content", final_message)

    if isinstance(content, str):
        return content

    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                parts.append(item.get("text", ""))
        text = "\n".join(part for part in parts if part)
        if text:
            return text

    return str(content)


def ask_agent(question: str, model_name: str = DEFAULT_MODEL) -> str:
    agent = build_agent(model_name=model_name)
    result = agent.invoke(
        {"messages": [{"role": "user", "content": question}]}
    )
    return extract_final_text(result["messages"])
