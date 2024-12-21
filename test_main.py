# from botocore.exceptions import NoCredentialsError
from unittest.mock import patch

import boto3
import pytest
from moto import mock_s3

from lambda_function import (
    cleanup_local_file,
    get_job_results,
    is_job_complete,
    is_pdf_openable,
    main,
    start_job,
)

# mocking starts now!

@pytest.fixture
def s3_bucket_name():
    return "s3sagemakerbucket"


@pytest.fixture
def files():
    return ["bg/BG170100136.pdf"] 


@mock_s3
def test_start_job():
    s3_bucket_name = "s3sagemakerbucket"
    object_name = "bg/BG170100136.pdf"
    boto3.client("s3").create_bucket(Bucket=s3_bucket_name,
                                     CreateBucketConfiguration={
                                         'LocationConstraint':'eu-west-1'
                                     })
    boto3.client("s3").put_object(Bucket=s3_bucket_name, Key=object_name, Body=b"%PDF-1.4\n" \
                  b"1 0 obj\n" \
                  b"<< /Title (Sample PDF) /Creator (Your Creator) >>\n" \
                  b"endobj\n" \
                  b"2 0 obj\n" \
                  b"<< /Type /Catalog /Pages 3 0 R >>\n" \
                  b"endobj\n" \
                  b"3 0 obj\n" \
                  b"<< /Type /Pages /Kids [4 0 R] /Count 1 >>\n" \
                  b"endobj\n" \
                  b"4 0 obj\n" \
                  b"<< /Type /Page /Parent 3 0 R /Resources << /Font << /F1 5 0 R >> >> /Contents 6 0 R >>\n" \
                  b"endobj\n" \
                  b"5 0 obj\n" \
                  b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>\n" \
                  b"endobj\n" \
                  b"6 0 obj\n" \
                  b"<< /Length 44 >>\n" \
                  b"stream\n" \
                  b"BT\n" \
                  b"/F1 12 Tf\n" \
                  b"72 712 Td\n" \
                  b"(Hello, World!) Tj\n" \
                  b"ET\n" \
                  b"endstream\n" \
                  b"endobj\n" \
                  b"xref\n" \
                  b"0 7\n" \
                  b"0000000000 65535 f \n" \
                  b"0000000009 00000 n \n" \
                  b"0000000060 00000 n \n" \
                  b"0000000113 00000 n \n" \
                  b"0000000189 00000 n \n" \
                  b"0000000273 00000 n \n" \
                  b"0000000325 00000 n \n" \
                  b"trailer\n" \
                  b"<< /Root 2 0 R /Size 7 >>\n" \
                  b"startxref\n" \
                  b"396\n" \
                  b"%%EOF\n")

    # Mock Textract start_document_text_detection
    with patch("boto3.client") as mock_client:
        mock_textract = mock_client.return_value
        mock_textract.start_document_text_detection.return_value = {"JobId": "mock_job_id"}

        job_id = start_job(mock_textract, s3_bucket_name, object_name)
        assert job_id == "mock_job_id"


@mock_s3
def test_is_job_complete():
    # Mock Textract get_document_text_detection
    with patch("boto3.client") as mock_client:
        mock_textract = mock_client.return_value
        mock_textract.get_document_text_detection.side_effect = [
            {"JobStatus": "IN_PROGRESS"},
            {"JobStatus": "SUCCEEDED"},
        ]

        job_id = "mock_job_id"
        status = is_job_complete(mock_textract, job_id)
        assert status == "SUCCEEDED"


@mock_s3
def test_get_job_results():
    # Mock Textract get_document_text_detection with multiple result pages
    with patch("boto3.client") as mock_client:
        mock_textract = mock_client.return_value
        mock_textract.get_document_text_detection.side_effect = [
            {"Pages": [{"Blocks": ["page1"]}], "NextToken": "mock_token", "Blocks":[]},
            {"Pages": [{"Blocks": ["page2"]}], "NextToken": None,"Blocks":[]},
        ]

        job_id = "mock_job_id"
        results = get_job_results(mock_textract, job_id)
        assert len(results) == 2
        assert results[0]["Pages"] == [{"Blocks": ["page1"]}]
        assert results[1]["Pages"] == [{"Blocks": ["page2"]}]
        assert results[1]["NextToken"] is None

@mock_s3
def test_is_pdf_openable():
    s3_bucket_name = "s3sagemakerbucket"
    object_name = "bg/BG1307018880.pdf"
    s3obj=boto3.client("s3")
    s3obj.create_bucket(Bucket=s3_bucket_name,
                                     CreateBucketConfiguration={
                                         'LocationConstraint':'eu-west-1'
                                     })
    s3obj.put_object(Bucket=s3_bucket_name, Key=object_name, Body=b"%PDF-1.4\n" \
                  b"1 0 obj\n" \
                  b"<< /Title (Sample PDF) /Creator (Your Creator) >>\n" \
                  b"endobj\n" \
                  b"2 0 obj\n" \
                  b"<< /Type /Catalog /Pages 3 0 R >>\n" \
                  b"endobj\n" \
                  b"3 0 obj\n" \
                  b"<< /Type /Pages /Kids [4 0 R] /Count 1 >>\n" \
                  b"endobj\n" \
                  b"4 0 obj\n" \
                  b"<< /Type /Page /Parent 3 0 R /Resources << /Font << /F1 5 0 R >> >> /Contents 6 0 R >>\n" \
                  b"endobj\n" \
                  b"5 0 obj\n" \
                  b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>\n" \
                  b"endobj\n" \
                  b"6 0 obj\n" \
                  b"<< /Length 44 >>\n" \
                  b"stream\n" \
                  b"BT\n" \
                  b"/F1 12 Tf\n" \
                  b"72 712 Td\n" \
                  b"(Hello, World!) Tj\n" \
                  b"ET\n" \
                  b"endstream\n" \
                  b"endobj\n" \
                  b"xref\n" \
                  b"0 7\n" \
                  b"0000000000 65535 f \n" \
                  b"0000000009 00000 n \n" \
                  b"0000000060 00000 n \n" \
                  b"0000000113 00000 n \n" \
                  b"0000000189 00000 n \n" \
                  b"0000000273 00000 n \n" \
                  b"0000000325 00000 n \n" \
                  b"trailer\n" \
                  b"<< /Root 2 0 R /Size 7 >>\n" \
                  b"startxref\n" \
                  b"396\n" \
                  b"%%EOF\n")
    openable = is_pdf_openable(s3obj, s3_bucket_name, object_name)
    assert openable is True


def test_cleanup_local_file(tmpdir):
    # Create a temporary file
    file_path = tmpdir.join("temp_file.txt")
    file_path.write("content")

    # Calling the function and check if the file is deleted
    cleanup_local_file(tmpdir)
    assert not file_path.exists()

@mock_s3
def test_main_with_valid_status_code(tmpdir):
    # Set up your mock AWS environment
    # s3 = boto3.client('s3', region_name='eu-west-1')
    # s3_bucket_name = "s3sagemakerbucket"
    # files = ["bg/BG1307018880.pdf"]
    # s3.create_bucket(Bucket='test-Bucket',
    #                                  CreateBucketConfiguration={
    #                                      'LocationConstraint':'eu-west-1'
    #                                  })
    # client = boto3.client('textract', region_name='us-west-1')
    # s3.put_object(Bucket=s3_bucket_name, Key=object_name, Body=b"dummy content")

    oname=[]
    s3_bucket_name = "s3sagemakerbucket"
    object_name = "bg/BG170100136.pdf"
    s3obj=boto3.client("s3")
    s3obj.create_bucket(Bucket=s3_bucket_name,
                                     CreateBucketConfiguration={
                                         'LocationConstraint':'eu-west-1'
                                     })
    s3obj.put_object(Bucket=s3_bucket_name, Key=object_name, Body=b"%PDF-1.4\n" \
                  b"1 0 obj\n" \
                  b"<< /Title (Sample PDF) /Creator (Your Creator) >>\n" \
                  b"endobj\n" \
                  b"2 0 obj\n" \
                  b"<< /Type /Catalog /Pages 3 0 R >>\n" \
                  b"endobj\n" \
                  b"3 0 obj\n" \
                  b"<< /Type /Pages /Kids [4 0 R] /Count 1 >>\n" \
                  b"endobj\n" \
                  b"4 0 obj\n" \
                  b"<< /Type /Page /Parent 3 0 R /Resources << /Font << /F1 5 0 R >> >> /Contents 6 0 R >>\n" \
                  b"endobj\n" \
                  b"5 0 obj\n" \
                  b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>\n" \
                  b"endobj\n" \
                  b"6 0 obj\n" \
                  b"<< /Length 44 >>\n" \
                  b"stream\n" \
                  b"BT\n" \
                  b"/F1 12 Tf\n" \
                  b"72 712 Td\n" \
                  b"(Hello, World!) Tj\n" \
                  b"ET\n" \
                  b"endstream\n" \
                  b"endobj\n" \
                  b"xref\n" \
                  b"0 7\n" \
                  b"0000000000 65535 f \n" \
                  b"0000000009 00000 n \n" \
                  b"0000000060 00000 n \n" \
                  b"0000000113 00000 n \n" \
                  b"0000000189 00000 n \n" \
                  b"0000000273 00000 n \n" \
                  b"0000000325 00000 n \n" \
                  b"trailer\n" \
                  b"<< /Root 2 0 R /Size 7 >>\n" \
                  b"startxref\n" \
                  b"396\n" \
                  b"%%EOF\n")

    
    with patch("boto3.client") as mock_client:
        mock_textract = mock_client.return_value
        mock_textract.start_document_text_detection.return_value = {"JobId": "mock_job_id"}
        mock_textract.get_document_text_detection.side_effect = [
            {"JobStatus": "IN_PROGRESS"},
            {"JobStatus": "SUCCEEDED"},
            {"Pages": [{"Blocks": ["page1"]}], "NextToken": "mock_token", "Blocks":[]},
            {"Pages": [{"Blocks": ["page2"]}], "NextToken": None, "Blocks":[]},
        ]
        # mock_textract.get_document_text_detection.side_effect = [
        #     {"Pages": [{"Blocks": ["page1"]}], "NextToken": "mock_token"},
        #     {"Pages": [{"Blocks": ["page2"]}], "NextToken": None},
        # ]
    oname.append(object_name)
    status=200
    
    file_path = tmpdir.join("temp_file.txt")
    file_path.write("content")

    cleanup_local_file(tmpdir)
    # assert not file_path.exists()
    result = main(status, s3_bucket_name, oname, tmpdir, mock_textract, s3obj)

    assert result["statusCode"] == "200"
    