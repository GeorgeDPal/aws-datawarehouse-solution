import boto3
import logging

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

events = boto3.client("events")
lambda_client = boto3.client("lambda")

RULE_NAME = "trigger-lambda1-every-10min"
LAMBDA_NAME = "lambda-trigger-glue"

def create_eventbridge_rule():
    try:
        events.describe_rule(Name=RULE_NAME)
        logger.info(f"ℹ️ Rule {RULE_NAME} already exists")
    except events.exceptions.ResourceNotFoundException:
        response = events.put_rule(
            Name=RULE_NAME,
            ScheduleExpression="rate(10 minutes)",  # every 10 min
            State="ENABLED",
            Description="Trigger Lambda 1 (Glue ETL) every 10 minutes"
        )
        logger.info(f"✅ Created EventBridge rule {RULE_NAME}")

def add_lambda_target():
    # Get Lambda ARN
    response = lambda_client.get_function(FunctionName=LAMBDA_NAME)
    lambda_arn = response["Configuration"]["FunctionArn"]

    # Add target
    events.put_targets(
        Rule=RULE_NAME,
        Targets=[{"Id": "1", "Arn": lambda_arn}]
    )

    # Add permission so EventBridge can invoke Lambda
    try:
        lambda_client.add_permission(
            FunctionName=LAMBDA_NAME,
            StatementId=f"{RULE_NAME}-permission",
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=f"arn:aws:events:{events.meta.region_name}:{boto3.client('sts').get_caller_identity()['Account']}:rule/{RULE_NAME}"
        )
        logger.info(f"✅ Added permission for EventBridge to invoke {LAMBDA_NAME}")
    except lambda_client.exceptions.ResourceConflictException:
        logger.info(f"ℹ️ Permission already exists for {LAMBDA_NAME}")

if __name__ == "__main__":
    create_eventbridge_rule()
    add_lambda_target()
    logger.info(f"🎯 EventBridge rule '{RULE_NAME}' now triggers '{LAMBDA_NAME}' every 10 minutes")
