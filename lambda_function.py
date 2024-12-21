# def is_pdf_openable(s3_bucket_name, key):
#     s3 = boto3.client("s3")
#     try:
#         obj = s3.get_object(Bucket=s3_bucket_name, Key=key)
#         pdf_content = BytesIO(obj["Body"].read())
#         reader = PdfReader(pdf_content)
#         _ = len(reader.pages)
#         return True
#     except Exception as e:
#         print(f"error opening {key}: {str(e)}")
#         return False

# import configparser
# import json
import os
import time

# import urllib.parse
from io import BytesIO

# import boto3
from PyPDF2 import PdfReader

finalresult={"statusCode":"",
             "bucket_name":"",
             "key":""}

def start_job(client, s3_bucket_name, object_name):
    response = None
    response = client.start_document_text_detection(
        DocumentLocation={"S3Object": {"Bucket": s3_bucket_name, "Name": object_name}}
    )
    print("in start_job function")
    return response["JobId"]


def is_job_complete(client, job_id):
    time.sleep(1)
    response = client.get_document_text_detection(JobId=job_id)
    print(response)
    status = response["JobStatus"]
    print("Job status: {}".format(status))

    while status == "IN_PROGRESS":
        time.sleep(1)
        response = client.get_document_text_detection(JobId=job_id)
        status = response["JobStatus"]
        print("Job status: {}".format(status))
    print("in is job complete function")
    return status


def get_job_results(client, job_id):
    pages = []
    time.sleep(1)
    response = client.get_document_text_detection(JobId=job_id)
    pages.append(response)
    print("Resultset page received: {}".format(len(pages)))
    next_token = None
    if "NextToken" in response:
        next_token = response["NextToken"]

    while next_token:
        time.sleep(1)
        response = client.get_document_text_detection(JobId=job_id, NextToken=next_token)
        pages.append(response)
        print("Resultset page received: {}".format(len(pages)))
        next_token = None
        if "NextToken" in response:
            next_token = response["NextToken"]
    print("in get job results functions")
    print(pages)
    return pages

#mocking now!!
def is_pdf_openable(s3, s3_bucket_name, key):
    # s3 = boto3.client("s3",aws_access_key_id="AKIARU3MIISFVHPQBHLV",aws_secret_access_key="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")
    try:
        obj = s3.get_object(Bucket=s3_bucket_name, Key=key)
        pdf_content = BytesIO(obj["Body"].read())
        reader = PdfReader(pdf_content)
        _ = len(reader.pages)
        print("in is pdf openable function")
        return True
    except Exception as e:
        print(f"error opening {key}: {str(e)}")
        return False

def cleanup_local_file(directory):
    for file_name in os.listdir(directory):
        file_path = os.path.join(directory, file_name)
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
                print("in clean up function")
        except Exception as e:
            print(f"Error deleting file {file_path}: {str(e)}")

def main(status, bucket, files, temp, client, s3):
    output_list=[]
    if status == 200:
        finalresult['bucket_name']=str(bucket)
        for input_file in files:
            filetexts = dict()
            s3_bucket_name = bucket
            document_name = input_file

            if is_pdf_openable(s3, s3_bucket_name,document_name):
                print(s3_bucket_name, document_name)

                file = document_name.split("/")[-1]
                # client = boto3.client("textract")

                job_id = start_job(client, s3_bucket_name, document_name)
                print("Started job with id: {}".format(job_id))
                if is_job_complete(client, job_id):
                    response = get_job_results(client, job_id)

                lines = []
                for result_page in response:
                    for item in result_page["Blocks"]:
                        if item["BlockType"] == "LINE":
                            print(item["Text"])
                            lines.append(item["Text"])
                filetexts[file] = lines

                for file, text in filetexts.items():
                    file2 = file.split(".")[0]
                    file2 = file.replace(".pdf", "").replace(".", "")
                    with open(f"{temp}/{file2}.txt", "w", encoding="utf-8") as outfile:
                        for line in lines:
                            outfile.write(line + "\n")
                        outfile.close()

                # s3 = boto3.client("s3",aws_access_key_id="",aws_secret_access_key="")
                s3.upload_file(
                    Filename=f"{temp}/{file2}.txt", Bucket=s3_bucket_name, Key=f"bg/{file2}.txt"
                )
                outputfile=f"bg/{file2}.txt"
                output_list.append(str(outputfile))

                cleanup_local_file(temp)         
        finalresult['statusCode']=str(200)
        finalresult['key']=str(output_list)
        return finalresult
    else:
        raise Exception("Status code not 200")


# # For local run
# final_list = main()
# print(final_list)
