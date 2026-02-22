import pytest #For testing framework 
from fastapi.testclient import TestClient #To create a test client for the FastAPI application
from bdi_api.app import app #Importing the FastAPI application to be tested
from unittest.mock import patch, MagicMock #For mocking external dependencies like boto3 and requests

#Fixture to create a test client for the FastAPI application
@pytest.fixture
def client():
    with TestClient(app) as client:
        yield client 


#Patching the boto3 client and requests.get to test without making actual API or AWS calls
@patch("bdi_api.s4.exercise.boto3.client")
@patch("bdi_api.s4.exercise.requests.get")


def test_download_endpoint_exists(mock_requests, mock_boto, client):
    fake_response = MagicMock(status_code=200, content=b"fake data")
    #Mocking the response from requests.get to return a succesful response with fake data
    mock_requests.return_value = fake_response
    #Mocking the boto3 client to return a MagicMock because we are not testing the actual AWS interactions here
    mock_boto.return_value = MagicMock()
    #Making a POST request to the download endpoint with a file limit of 1
    response = client.post("/api/s4/aircraft/download?file_limit=1")
    #Calling the API endpoint and asserting that the response status code is 200, indicating success
    assert response.status_code == 200


#Patching the boto3 client to test the prepare endpoint without making actual AWS calls
#Only using the boto3 patch because prepare downloads from S3 to local storage
@patch("bdi_api.s4.exercise.boto3.client")
def test_prepare_endpoint_exists(mock_boto, client):
    #S3 mock to simulate the presence of a file in the S3 bucket
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value={"Contents": [{"Key": "raw/day=20231101/000000Z.json.gz"}]}
    #Mocking the boto3 client to return our mock S3 client
    mock_boto.return_value = mock_s3
    #Execute the endpoint 
    response = client.post("/api/s4/aircraft/prepare")
    assert response.status_code == 200