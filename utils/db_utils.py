"""
This file contains reusable functions to avoid boiler plates
"""

import logging
import pandas as pd


if len(logging.getLogger().handlers) > 0:
    # The Lambda environment pre-configures a handler logging to stderr. If a handler is already configured,
    # `.basicConfig` does not execute. Thus, we set the level directly.
    logging.getLogger().setLevel(logging.INFO)
else:
    logging.basicConfig(level=logging.INFO)


def execute_query(cursor, query):
    """
    This function is used to execute a query to get all the rows.
    """
    cursor.execute(query)
    databases = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(databases, columns=columns)
    return df


# Function to close the connection and cursor
def close_connection(conn):
    """
    This function is used to close the database connection.
    """
    if conn is not None and conn.is_connected():
        conn.close()
