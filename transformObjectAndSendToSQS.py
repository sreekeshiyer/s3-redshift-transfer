### Lambda Function 1: This is the compute layer of the program where the object gets processed and transformed into a custom format as required by the database along with some calculated fields

### Trigger : S3
### Destination: SQS

import boto3
import json
import uuid
import os

# Initialize the S3 client
s3_client = boto3.client('s3')

# Initialize SQS
sqs_client = boto3.client('sqs')

# Define your SQS queue URL
SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/050055370753/TransactionMessageQueue.fifo"


from datetime import datetime, timedelta
import json

def enrich_transaction_data(data):
    """
    Enrich transaction data by calculating derived fields.

    Args:
        data (dict): The raw transaction data as a dictionary.

    Returns:
        dict: The enriched transaction data with derived fields.
    """
    try:
        # Parse order_time
        order_time = datetime.strptime(data['order_time'], "%Y-%m-%dT%H:%M:%S")

        # 1. Calculate estimated_delivery_time
        estimated_delivery_minutes = data.get('estimated_delivery_minutes', 30)  # Default to 30 minutes
        data['estimated_delivery_time'] = (order_time + timedelta(minutes=estimated_delivery_minutes)).isoformat()

        # 2. Calculate order_duration_minutes
        if 'completion_time' in data and data['completion_time']:
            completion_time = datetime.strptime(data['completion_time'], "%Y-%m-%dT%H:%M:%S")
            data['order_duration_minutes'] = int((completion_time - order_time).total_seconds() / 60)
        else:
            data['order_duration_minutes'] = None

        # 3. Calculate total_items_count
        if 'items' in data and isinstance(data['items'], list):
            data['total_items_count'] = sum(item.get('quantity', 0) for item in data['items'])
        else:
            data['total_items_count'] = 0

        # 4. Calculate final_amount
        data['final_amount'] = (
            data['total_amount']
            + data.get('tax_amount', 0.0)
            - data.get('discount_amount', 0.0)
            + data.get('delivery_fee', 0.0)
        )

        # 5. Determine is_delayed
        if 'delivery_status' in data and data['delivery_status'] == "Delivered" and 'completion_time' in data:
            is_delayed = completion_time > (order_time + timedelta(minutes=estimated_delivery_minutes))
        else:
            is_delayed = True  # Default to true if status is not 'Delivered' or no completion time
        data['is_delayed'] = is_delayed

        return data

    except Exception as e:
        raise ValueError(f"Error enriching transaction data: {str(e)}")

def lambda_handler(event, context):
    """
    AWS Lambda handler that fetches an object from S3.
    Triggered by an S3 PutObject event.
    """
    try:
        
        # Extract bucket name and object key from the S3 event
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']
        
        # Fetch the object from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        
        # Read the object content
        object_data = response['Body'].read().decode('utf-8')  # Assumes the object is UTF-8 encoded
        
        # If the object is JSON, parse it
        try:
            json_data = json.loads(object_data)

            ## Process Business Logic
            data = enrich_transaction_data( json_data )

            print("All Feed Data:", json.dumps(data, indent=2))

            ## Now that we have all the data, we can push this to the queue
            sqs_response = sqs_client.send_message(
                QueueUrl=SQS_QUEUE_URL,
                MessageBody=json.dumps(data),
                MessageGroupId= str(uuid.uuid4()) # Required for FIFO
            )
            print("SQS Response:", sqs_response)

        except json.JSONDecodeError:
            print("Object data is not valid JSON. Raw content returned.")
        
        print("Moved to SQS")
        return {
            'statusCode': 200,
            'body': f"Transaction {object_key} Moved to SQS Queue."
        }
    
    except Exception as e:
        print(e)
        return {
            'statusCode': 500,
            'body': f"Error: {str(e)}"
        }
