import braintree
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import json
from datetime import date, datetime, timedelta
from flask import Flask, request, jsonify
import os
from dotenv import load_dotenv

load_dotenv()  # loads variables from .env file into environment

app = Flask(__name__)

def update_table(conn, table_name, schema, database, df):
    success, num_chunks, num_rows, output = write_pandas(
                conn=conn,
                df=df,
                table_name=table_name,
                database=database,
                schema=schema
            )

def table_to_dataframe(conn, table):
    cur = conn.cursor()
    cur.execute(f'SELECT * FROM {table}')
    df = cur.fetch_pandas_all()
    cur.close()
    return df

def incremental_refresh(conn, table_name, schema, database, new_df):
    df_table = table_to_dataframe(conn, f'{database}.{schema}.{table_name}')
    df_updated_table = pd.concat([df_table, new_df], ignore_index=True)
    df_updated_table = df_updated_table.drop_duplicates(subset='ID', keep=False)
    print(f'{len(df_updated_table)} new records added')
    # return df_updated_table
    update_table(conn, table_name, schema, database, df_updated_table)

def update_transaction_table(search_results, data):
    for transaction in search_results.items:
        data["ID"].append(transaction.id)
        data["AMOUNT"].append(transaction.amount)
        data["CREATED_AT"].append(transaction.created_at)
        data["CREDIT_CARD_DETAILS"].append( { 'card_type': f'{transaction.credit_card_details.card_type}',  'customer_location': f'{transaction.credit_card_details.customer_location}' } )
        data["CURRENCY_ISO_CODE"].append(transaction.currency_iso_code)
        data["CUSTOMER_DETAILS"].append({ 'email': f'{transaction.customer_details.email}', 'id': f'{transaction.customer_details.id}' })
        data["DISBURSEMENT_DETAILS"].append( {'disbursement_date': f'{transaction.disbursement_details.disbursement_date}', 'success': f'{transaction.disbursement_details.success}'} )
        data["GATEWAY_REJECTION_REASON"].append(transaction.gateway_rejection_reason)
        data["MERCHANT_ACCOUNT_ID"].append(transaction.merchant_account_id)
        data["ORDER_ID"].append(transaction.order_id)
        data["PAYMENT_INSTRUMENT_TYPE"].append(transaction.payment_instrument_type)
        data["PLAN_ID"].append(transaction.plan_id)
        data["PROCESSOR_AUTHORIZATION_CODE"].append(transaction.processor_authorization_code)
        data["PROCESSOR_RESPONSE_CODE"].append(transaction.processor_response_code)
        data["PROCESSOR_RESPONSE_TEXT"].append(transaction.processor_response_text)
        data["RECURRING"].append(transaction.recurring)
        data["REFUNDED_TRANSACTION_ID"].append(transaction.refunded_transaction_id)
        data["SETTLEMENT_BATCH_ID"].append(transaction.settlement_batch_id)
        data["STATUS"].append(transaction.status)
        data["SUBSCRIPTION_DETAILS"].append({ 'billing_period_start_date': f'{transaction.subscription_details.billing_period_start_date}', 'billing_period_end_date': f'{transaction.subscription_details.billing_period_end_date}' } )
        data["SUBSCRIPTION_ID"].append(transaction.subscription_id)
        data["TYPE"].append(transaction.type)
        data["UPDATED_AT"].append(transaction.updated_at)

    return data

def localize_naive(ts):
    if ts.tzinfo is None or ts.tzinfo.utcoffset(ts) is None:
        return ts.tz_localize('UTC')
    return ts

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, date):
        return obj.isoformat()
        # return localize_naive(obj.isoformat())
    raise TypeError ("Type not serializable")

@app.get("/")
def home():
    return "server running"

@app.route("/refresh", methods=["GET"])
def refresh_braintree_data():
    gateway = braintree.BraintreeGateway(
        braintree.Configuration(
            environment=os.environ.get("BRAINTREE_ENV"),
            merchant_id=os.environ.get("BRAINTREE_MERCHANT_ID"),
            public_key=os.environ.get("BRAINTREE_PUBLIC_KEY"),
            private_key=os.environ.get("BRAINTREE_PRIVATE_KEY") 
        )
    )

    conn = snowflake.connector.connect(
        user=os.environ.get("SNOWFLAKE_USER"),
        password=os.environ.get("SNOWFLAKE_PASSWORD"),
        account=os.environ.get("SNOWFLAKE_ACCOUNT")
    )

    data = {
        "ID": [],
        "AMOUNT": [],
        "CREATED_AT": [],
        "CREDIT_CARD_DETAILS": [],
        "CURRENCY_ISO_CODE": [],
        "CUSTOMER_DETAILS": [],
        "DISBURSEMENT_DETAILS": [],
        "GATEWAY_REJECTION_REASON": [],
        "MERCHANT_ACCOUNT_ID": [],
        "ORDER_ID": [],
        "PAYMENT_INSTRUMENT_TYPE": [],
        "PLAN_ID": [],
        "PROCESSOR_AUTHORIZATION_CODE": [],
        "PROCESSOR_RESPONSE_CODE": [],
        "PROCESSOR_RESPONSE_TEXT": [],
        "RECURRING": [],
        "REFUNDED_TRANSACTION_ID": [],
        "SETTLEMENT_BATCH_ID": [],
        "STATUS": [],
        "SUBSCRIPTION_DETAILS": [],
        "SUBSCRIPTION_ID": [],
        "TYPE": [],
        "UPDATED_AT": [],
    }

    # Get the current date and time
    now = datetime.now()

    # Calculate yesterday's date at 00:00 hours
    yesterday_start = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

    # Calculate tomorrow's date at 23:59 hours
    tomorrow_end = (now + timedelta(days=1)).replace(hour=23, minute=59, second=0, microsecond=0)

    # Format the dates as strings
    yesterday_start_str = yesterday_start.strftime("%m/%d/%Y %H:%M")
    tomorrow_end_str = tomorrow_end.strftime("%m/%d/%Y %H:%M")

    search_results = gateway.transaction.search(braintree.TransactionSearch.created_at.between(yesterday_start_str, tomorrow_end_str))
    data = update_transaction_table(search_results, data)

    df = pd.DataFrame(data)

    df['CREATED_AT'] = pd.to_datetime(df['CREATED_AT'], format='%Y-%m-%d %H:%M:%S')
    df['UPDATED_AT'] = pd.to_datetime(df['UPDATED_AT'], format='%Y-%m-%d %H:%M:%S')

    df['CREATED_AT'] = df['CREATED_AT'].apply(localize_naive)
    df['UPDATED_AT'] = df['UPDATED_AT'].apply(localize_naive)
    df['AMOUNT'] = df['AMOUNT'].astype(float)
    df['CREDIT_CARD_DETAILS'] = df['CREDIT_CARD_DETAILS'].apply(lambda x: json.dumps(x, default=json_serial, ensure_ascii=False))
    df['CUSTOMER_DETAILS'] = df['CUSTOMER_DETAILS'].apply(lambda x: json.dumps(x, default=json_serial, ensure_ascii=False))
    df['DISBURSEMENT_DETAILS'] = df['DISBURSEMENT_DETAILS'].apply(lambda x: json.dumps(x, default=json_serial, ensure_ascii=False))
    df['SUBSCRIPTION_DETAILS'] = df['SUBSCRIPTION_DETAILS'].apply(lambda x: json.dumps(x, default=json_serial, ensure_ascii=False))

    df = df.drop_duplicates(subset='ID', keep="last")

    incremental_refresh(conn, 'TRANSACTIONS', 'BRAINTREE_DATA', 'RAW', df)

    return "run completed"
