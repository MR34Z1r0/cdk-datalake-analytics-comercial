import logging
import os
import boto3
import datetime as dt

logging.basicConfig(format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("data-platform")
logger.setLevel(os.environ.get("LOGGING", logging.INFO))

class STATUS:
    IN_PROGRESS = 0
    LANDING_SUCCEEDED = 1
    RAW_SUCCEEDED = 2
    STAGE_SUCCEEDED = 3
    LANDING_FAILED = 4
    RAW_FAILED = 5
    STAGE_FAILED = 6
    WARNING = 2

class LOGGING_UTILS():

    sns_client = boto3.client("sns")
    dynamodb_resource = boto3.resource('dynamodb')

    def __init__(self, config_table_name, log_table_name, lambda_name,topic_arn):
        self.config_table_name = config_table_name
        self.log_table_name = log_table_name
        self.lambda_name = lambda_name

        self.topic_arn = topic_arn

    def send_error_message(self, table_name, error):
        
        if "no data detected to migrate" in error:
            message = f"RAW WARNING in table: {table_name} \n{error}"
        else:
            message = f"Failed table: {table_name} \nStep: {self.lambda_name} \nLog ERROR \n{error}"
        self.sns_client.publish(
            TopicArn=self.topic_arn,
            Message=message
        )

    def update_attribute_value_dynamodb(self, row_key_field_name, row_key, attribute_name, attribute_value):
        logger.info('update dynamoDb Metadata : {} ,{},{},{}'.format(row_key_field_name, row_key, attribute_name, attribute_value))
        dynamo_table = self.dynamodb_resource.Table(self.config_table_name)
        dynamo_table.update_item(
            Key={row_key_field_name: row_key},
            AttributeUpdates={
                attribute_name: {
                    'Value': attribute_value,
                    'Action': 'PUT'
                }
            }
        )

    def update_status_dynamo(self, table_name, status : STATUS, message : str = ''):
        
        if status == STATUS.IN_PROGRESS:
            status_stage = 'IN_PROGRESS'
            status_raw = 'IN_PROGRESS'
            status_landing = 'IN_PROGRESS'
            self.update_attribute_value_dynamodb('TARGET_TABLE_NAME', table_name, 'STATUS_STAGE', status_stage)
            self.update_attribute_value_dynamodb('TARGET_TABLE_NAME', table_name, 'STATUS_RAW', status_raw)
            self.update_attribute_value_dynamodb('TARGET_TABLE_NAME', table_name, 'STATUS_LANDING', status_landing)
        
        elif status == STATUS.LANDING_SUCCEEDED:
            status_landing = 'SUCCEEDED'
            self.update_attribute_value_dynamodb('TARGET_TABLE_NAME', table_name, 'STATUS_LANDING', status_landing)

        elif status == STATUS.RAW_SUCCEEDED:
            status_raw = 'SUCCEEDED'
            self.update_attribute_value_dynamodb('TARGET_TABLE_NAME', table_name, 'STATUS_RAW', status_raw)

        elif status == STATUS.STAGE_SUCCEEDED:
            status_stage = 'SUCCEEDED'
            self.update_attribute_value_dynamodb('TARGET_TABLE_NAME', table_name, 'STATUS_STAGE', status_stage)
        
        elif status == STATUS.LANDING_FAILED:
            status_stage = 'FAILED'
            status_raw = 'FAILED'
            status_landing = 'FAILED'
            self.update_attribute_value_dynamodb('TARGET_TABLE_NAME', table_name, 'STATUS_STAGE', status_stage)
            self.update_attribute_value_dynamodb('TARGET_TABLE_NAME', table_name, 'STATUS_RAW', status_raw)
            self.update_attribute_value_dynamodb('TARGET_TABLE_NAME', table_name, 'STATUS_LANDING', status_landing)
        
        elif status == STATUS.RAW_FAILED:
            status_stage = 'FAILED'
            status_raw = 'FAILED'
            self.update_attribute_value_dynamodb('TARGET_TABLE_NAME', table_name, 'STATUS_STAGE', status_stage)
            self.update_attribute_value_dynamodb('TARGET_TABLE_NAME', table_name, 'STATUS_RAW', status_raw)
            
        elif status == STATUS.STAGE_FAILED:
            status_stage = 'FAILED'
            self.update_attribute_value_dynamodb('TARGET_TABLE_NAME', table_name, 'STATUS_STAGE', status_stage)

        elif status == STATUS.WARNING :
            status_raw = 'WARNING'
            status_landing = 'WARNING'
            status_stage = 'WARNING'
            self.update_attribute_value_dynamodb('TARGET_TABLE_NAME', table_name, 'STATUS_LANDING', status_landing)
            self.update_attribute_value_dynamodb('TARGET_TABLE_NAME', table_name, 'STATUS_RAW', status_raw)
            self.update_attribute_value_dynamodb('TARGET_TABLE_NAME', table_name, 'STATUS_STAGE', status_stage)

        
        self.update_attribute_value_dynamodb('TARGET_TABLE_NAME', table_name, 'FAIL REASON', message)

  

    def upload_log(self, target_table_name : str, type : str, message : str, load_type : str, date_time = "", service : str = "GLUE", service_specification : str = "", pipeline_id : str = ""):
        dynamodb = boto3.resource('dynamodb')
        log = {
            'TARGET_TABLE_NAME': target_table_name.upper(),
            'DATETIME': date_time,
            'PROJECT_NAME': os.getenv("PROJECT_NAME"),
            'LAYER': 'RAW',
            'TYPE': type,
            'LOAD_TYPE': load_type,
            'MESSAGE': message,
            'SERVICE' : service,
            'SERVICE_SPECIFICATION' : service_specification, 
            'PIPELINE_ID' : f"{pipeline_id}_{date_time[:-6].replace(' ', '_')}"
        }
        logger.info("uploading log")
        dynamo_table = dynamodb.Table(self.log_table_name)
        response = dynamo_table.put_item(Item=log)
