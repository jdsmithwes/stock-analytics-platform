import os
import boto3

sns_client = boto3.client("sns", region_name="us-east-1")


def _get_sns_topic_arn() -> str:
    """
    Priority:
    1. DAGSTER_SNS_TOPIC_ARN (Dagster-specific)
    2. SNS_TOPIC_ARN (shared / legacy)
    """
    topic_arn = (
        os.getenv("DAGSTER_SNS_TOPIC_ARN")
        or os.getenv("SNS_TOPIC_ARN")
    )

    if not topic_arn:
        raise RuntimeError(
            "Neither DAGSTER_SNS_TOPIC_ARN nor SNS_TOPIC_ARN is set"
        )

    return topic_arn


def send_sns(subject: str, message: str) -> None:
    sns_client.publish(
        TopicArn=_get_sns_topic_arn(),
        Subject=subject,
        Message=message,
    )