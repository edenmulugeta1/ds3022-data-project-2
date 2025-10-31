# prefect flow goes here

from prefect import flow, task
import time
import boto3
import requests

@task 
def populate_queue():
    """Make my queue using my uva ID"""
    uva_id = "unb6ny"
    api_url = f"https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/{uva_id}"

    response = requests.post(api_url)
    response.raise_for_status()
    
    response_data = response.json()
    sqs_url = response_data["sqs_url"]

    print(f"SQS queue created: {sqs_url}")
    return sqs_url


@task
def wait_for_messages(sqs_url):
    """Keep checking the queue until all 21 messages show up"""
    sqs = boto3.client("sqs", region_name="us-east-1")

    print("Waiting for messages to appear in the queue")

    while True:
        queue_attributes = sqs.get_queue_attributes(
            QueueUrl=sqs_url,
            AttributeNames=[
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible",
                "ApproximateNumberOfMessagesDelayed"
            ]
        )["Attributes"]

        number_visible = int(queue_attributes.get("ApproximateNumberOfMessages", 0))
        number_not_visible = int(queue_attributes.get("ApproximateNumberOfMessagesNotVisible", 0))
        number_delayed = int(queue_attributes.get("ApproximateNumberOfMessagesDelayed", 0))
        total_messages = number_visible + number_not_visible + number_delayed

        print(f"Currently: {number_visible} visible, {number_not_visible} not visible, {number_delayed} delayed (total {total_messages})")

        # Once all 21 messages are visible, we can move on
        if number_visible == 21:
            print("All messages are visible and are ready to read.")
            break

        print("Still waiting for messages")
        time.sleep(10)

@task
def get_messages(sqs_url):
    """Recieve and delete all 21 messgaes from the queue"""
    sqs = boto3.client("sqs", region_name="us-east-1")
    all_messages = []

    while len(all_messages) < 21:
        response = sqs.receive_message(
            QueueUrl=sqs_url,
            MessageAttributeNames=["All"],
            MaxNumberOfMessages=10,
            WaitTimeSeconds=10
        )

        # Check if there are any messages available
        if "Messages" in response:
            for message in response["Messages"]:
                message_attributes = message["MessageAttributes"]
                order_no = int(message_attributes["order_no"]["StringValue"])
                word = message_attributes["word"]["StringValue"]

                all_messages.append((order_no, word))

                sqs.delete_message(
                    QueueUrl=sqs_url, 
                    ReceiptHandle=message["ReceiptHandle"]
                    )
            
            print(f"Collected {len(all_messages)} messages so far")
        else:
            print(f"No new messages. Currently have: {len(all_messages)}")
            
            time.sleep(5)

    print("Done collecting all messages")
    return all_messages


@task
def assemble_phrase(messages):
    """Sort messages by order number and join them into one phrase"""
    messages.sort(key=lambda message: message[0])
    phrase = " ".join(word for _, word in messages)
    print(f"Assembled phrase: {phrase}")
    return phrase


@task
def submit_solution(phrase):
    """Send the final phrase to the submission SQS queue"""
    uva_id = "unb6ny"
    submit_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
    sqs = boto3.client("sqs", region_name="us-east-1")

    response = sqs.send_message(
        QueueUrl=submit_url,
        MessageBody="submission",
        MessageAttributes={
            "uvaid": {"DataType": "String", "StringValue": uva_id},
            "phrase": {"DataType": "String", "StringValue": phrase},
            "platform": {"DataType": "String", "StringValue": "prefect"}
        }
    )

    print("Submission response:", response["ResponseMetadata"]["HTTPStatusCode"])
    return response


@flow(name="dp2-pipeline")
def main_flow():
    """Main Prefect flow to run all the above steps by order"""
    sqs_url = populate_queue()
    wait_for_messages(sqs_url)
    all_messages = get_messages(sqs_url)
    phrase = assemble_phrase(all_messages)
    submit_solution(phrase)


if __name__ == "__main__":
    main_flow()