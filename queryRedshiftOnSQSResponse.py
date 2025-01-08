### Lambda Function 2 - Putting the processed object that is fetched from SQS into Redshift. Can explore other options for SQS-S3 direct transfer.

### Trigger: SQS
### Destination: Redshift

import boto3
import json
import logging
from config import CLUSTER_ID, ARN

# Initialize AWS clients
sqs_client = boto3.client('sqs')
redshift_client = boto3.client('redshift-data')

# In-memory cache to track processed message IDs
processed_messages = set()

# Configuration (Replace with your values)
DATABASE = "dev"

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def generate_insert_query(transaction_data):
    """
    Generates a dynamic SQL INSERT query for the enriched transaction data.
    
    Args:
        transaction_data (dict): Enriched transaction data from SQS.
    
    Returns:
        str: The dynamically generated SQL query.
    """
    # Define the table name
    table_name = "transactions"
    
    # Define the fields in the table
    fields = [
        "kitchen_id", "customer_id", "order_time", "completion_time", "total_amount",
        "payment_method", "transaction_status", "items", "delivery_address",
        "delivery_status", "delivery_fee", "discount_amount", "tax_amount",
        "estimated_delivery_time", "order_duration_minutes", "total_items_count",
        "final_amount", "is_delayed"
    ]
    
    # Extract values from transaction_data corresponding to the fields
    values = []
    for field in fields:
        value = transaction_data.get(field, None)
        
        # Handle JSON fields specifically
        if field == "items":
            value = f"JSON_PARSE('{json.dumps(value)}')" if value else "NULL"
        # Handle string fields
        elif isinstance(value, str):
            value = f"'{value.replace("'", "''")}'"  # Escape single quotes
        # Handle boolean fields
        elif isinstance(value, bool):
            value = "TRUE" if value else "FALSE"
        # Handle numeric or None
        elif value is None:
            value = "NULL"
        else:
            value = str(value)
        
        values.append(value)
    
    # Construct the SQL query
    query = f"""
    INSERT INTO {table_name} (
        {", ".join(fields)}
    ) VALUES (
        {", ".join(values)}
    );
    """
    return query

def lambda_handler(event, context):
    try:
        for record in event["Records"]:
            sqs_message = record["body"]
            #logger.info(f"Received SQS message: {sqs_message}")

            # Get deduplication ID from the message attributes or body
            if type(record) == str:
                record = json.loads(record)
            #deduplication_id = record.get('messageAttributes', {}).get('DeduplicationId', {}).get('stringValue')
            print(record)
            deduplication_id = ''
            # Fallback to a transaction_id in the message body if no DeduplicationId is provided
            if not deduplication_id:
                body = json.loads(record['body'])
                deduplication_id = body.get('transaction_id')
        
            # Skip processing if this message is already handled
            if deduplication_id in processed_messages:
                continue
        

        #     # 2. Parse the SQS message (assuming JSON format)
            if type(sqs_message) == str:
                transaction_data = json.loads(sqs_message)
            else: 
                transaction_data = sqs_message

        #     # Example transformation: Build SQL INSERT query
            sql = generate_insert_query(transaction_data)

        #     # 3. Execute SQL query using Redshift Data API
            response = execute_redshift_query(sql)
            if not response:
                return {
                    'statusCode': 417,
                    'body': "Either the query failed to Redshift or there was a validation error."
                }
            logger.info(f"Redshift Data API response: {response}")
        return {
            'statusCode': 200,
            'body': f"Transaction moved to Redshift."
        }

    
    except Exception as e:
        logger.error(f"Error processing SQS message: {str(e)}")
        raise e

def execute_redshift_query(sql):
    """
    Executes a query on Redshift using the Data API.
    """
    response = redshift_client.execute_statement(
        WorkgroupName=CLUSTER_ID,
        Database=DATABASE,
        Sql=sql
    )

    while True:

        try: 
            status_response = redshift_client.describe_statement(
                Id=response['Id']
            )
            
            status = status_response['Status']

        except Exception as e:
            logger.info(e)

        if status =='FINISHED':
            return "Success"

    return "Failed to get a response from Redshift."
