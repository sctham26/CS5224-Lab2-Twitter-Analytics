
import json
import os
import time
import boto3

athena = boto3.client("athena", region_name="ap-southeast-1")

DATABASE = "default"
OUTPUT_LOCATION = os.environ["ATHENA_OUTPUT_S3"]

QUERY = """
SELECT
    user_id,
    followers,
    followees
FROM user_stats_spark
WHERE user_id IN ('214328887', '107830991')
ORDER BY user_id DESC
"""

def lambda_handler(event, context):
    try:
        response = athena.start_query_execution(
            QueryString=QUERY,
            QueryExecutionContext={
                "Database": DATABASE
            },
            ResultConfiguration={
                "OutputLocation": OUTPUT_LOCATION
            },
            WorkGroup="primary"
        )

        query_id = response["QueryExecutionId"]

        while True:
            execution = athena.get_query_execution(
                QueryExecutionId=query_id
            )

            status = execution["QueryExecution"]["Status"]["State"]

            if status == "SUCCEEDED":
                break

            
            if status in ["FAILED", "CANCELLED"]:
                reason = execution["QueryExecution"]["Status"].get(
                    "StateChangeReason", "No reason provided"
    )

                raise RuntimeError(
                    f"Athena query failed: {status}. Reason: {reason}"
    )


            if context.get_remaining_time_in_millis() < 3000:
                raise TimeoutError("Query timed out")

            time.sleep(1)

        results = athena.get_query_results(
            QueryExecutionId=query_id
        )

        users = []

        for row in results["ResultSet"]["Rows"][1:]:
            values = [
                col.get("VarCharValue", "")
                for col in row["Data"]
            ]

            users.append({
                "user_id": values[0],
                "followers": int(values[1]),
                "followees": int(values[2])
            })

        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": json.dumps(users)
        }

    except Exception as error:
        print("Error:", str(error))

        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": json.dumps({
                "error": "Failed to retrieve user statistics"
            })
        }