import requests
import pandas as pd
import boto3
from io import StringIO
from datetime import datetime
import json
import Consonants as con
sns = boto3.client("sns", region_name="ap-southeast-2")
ssm = boto3.client("ssm", region_name="ap-southeast-2")


def send_sns_success():
    success_sns_arn = ssm.get_parameter(Name=con.SUCCESSNOTIFICATIONARN, WithDecryption=True)["Parameter"]["Value"]
    component_name = con.COMPONENT_NAME
    env = ssm.get_parameter(Name=con.ENVIRONMENT, WithDecryption=True)['Parameter']['Value']
    success_msg = con.SUCCESS_MSG
    sns_message = (f"{component_name} :  {success_msg}")
    succ_response = sns.publish(TargetArn=success_sns_arn, Message=json.dumps({'default': json.dumps(sns_message)}),
                                Subject=env + " : " + component_name, MessageStructure="json")
    return succ_response
    

def send_error_sns(msg):
    error_sns_arn = ssm.get_parameter(Name=con.ERRORNOTIFICATIONARN)["Parameter"]["Value"]
    env = ssm.get_parameter(Name=con.ENVIRONMENT, WithDecryption=True)['Parameter']['Value']
    error_message = con.ERROR_MSG + msg
    component_name = con.COMPONENT_NAME
    sns_message = (f"{component_name} : {error_message}")
    err_response = sns.publish(TargetArn=error_sns_arn, Message=json.dumps({'default': json.dumps(sns_message)}),
                               Subject=env + " : " + component_name,
                               MessageStructure="json")
    return err_response

def stock_csv():
# current date and time for folder/file naming
    now = datetime.now()
    date_folder = now.strftime("%Y-%m-%d")
    time_raw = now.strftime("%H-%M-%S")
    time_transformed = now.strftime("%H-%M-%S")
    s3 = boto3.client('s3')
    bucket_name = "kiruba-07"
    output_key = "response/stock_analysis.csv"
    try:

        # STEP 1 — GitHub API URL
        url = "https://api.github.com/repos/squareshift/stock_analysis/contents"

        # STEP 2 — Get file list
        response = requests.get(url)
        response.raise_for_status()
        files = response.json()

        # STEP 3 — Extract CSV URLs
        csv_files = [f["download_url"] for f in files if f["name"].endswith(".csv")]

        # Last file used as sector mapping
        sector_file = csv_files.pop()

        # STEP 4 — Read sector file
        sector_df = pd.read_csv(sector_file)

        # STEP 5 — Read all stock files
        dataframes = []
        for file_url in csv_files:
            symbol = file_url.split("/")[-1].replace(".csv", "")
            df = pd.read_csv(file_url)
            df["Symbol"] = symbol
            dataframes.append(df)

        combined_df = pd.concat(dataframes, ignore_index=True)

        # STEP 7 — Aggregation by Sector
        result = merged_df.groupby("Sector").agg({
            "open": "mean",
            "close": "mean",
            "high": "max",
            "low": "min",
            "volume": "mean"
        }).reset_index()

            # STEP 6 — Merge with sector info
        merged_df = pd.merge(combined_df, sector_df, on="Symbol", how="left")

        # STEP 8 — Time Filter
        merged_df["timestamp"] = pd.to_datetime(merged_df["timestamp"])
        filtered_df = merged_df[
            (merged_df["timestamp"] >= "2021-01-01") &
            (merged_df["timestamp"] <= "2021-05-26")
            ]


        # STEP 9 — Aggregate only selected sectors
        list_sector = ["TECHNOLOGY", "FINANCE"]
        result_time = filtered_df.groupby("Sector").agg({
            "open": "mean",
            "close": "mean",
            "high": "max",
            "low": "min",
            "volume": "mean"
        }).reset_index()

        result_time = result_time.rename(columns={
            "open": "aggregate_open",
            "close": "aggregate_close",
            "high": "aggregate_high",
            "low": "aggregate_low",
            "volume": "aggregate_volume"
        })

        result_time = result_time[result_time["Sector"].isin(list_sector)]

        # Raw folder
        raw_key = f"raw/{date_folder}/{time_raw}.csv"
        raw_buffer = StringIO()
        combined_df.to_csv(raw_buffer, index=False)
        s3.put_object(Bucket=bucket_name, Key=raw_key, Body=raw_buffer.getvalue())

        # Transformed folder
        transformed_key = f"transformed/{date_folder}/{time_transformed}.csv"
        transformed_buffer = StringIO()
        result_time.to_csv(transformed_buffer, index=False)
        s3.put_object(Bucket=bucket_name, Key=transformed_key, Body=transformed_buffer.getvalue())
        send_sns_success()

        return {
            "statusCode": 200,
            "body": result_time.to_dict(orient="records"),
            "message": "Final output saved to S3",
            "s3_path": f"s3://{bucket_name}/{output_key}/",
            "preview": result_time.to_dict(orient="records")
        }
    except Exception as e:
        # Error handling - write error to error/ folder with date/time in filename folder - error msg
        msg = str(e)
        failure_key = f"Failure/{date_folder}/{time_raw}.txt"
        s3.put_object(Bucket=bucket_name, Key=failure_key, Body=str(e))
        send_error_sns(msg)
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Pipeline failed",
                "error": str(e)}),
            "error_path": f"s3://{bucket_name}/{failure_key}"

        }


def lambda_handler(event, context):
        result = stock_csv()
        return result
