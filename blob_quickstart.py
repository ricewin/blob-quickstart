"""クイック スタート: Python 用 Azure Blob Storage クライアント ライブラリ

https://learn.microsoft.com/ja-jp/azure/storage/blobs/storage-quickstart-blobs-python

Next: Python クライアント ライブラリを使用する Azure Storage サンプル
https://learn.microsoft.com/ja-jp/azure/storage/common/storage-samples-python
"""

import os
import uuid

from azure.core.paging import ItemPaged
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobClient, BlobServiceClient, ContainerClient
from azure.storage.blob._models import BlobProperties

try:
    print("Azure Blob Storage Python quickstart sample")

    # Quickstart code goes here
    account_url = "https://ricewin.blob.core.windows.net"
    default_credential = DefaultAzureCredential()

    # Create the BlobServiceClient object
    blob_service_client = BlobServiceClient(account_url, credential=default_credential)

    # Create a unique name for the container
    container_name = str(uuid.uuid4())

    # Create the container
    container_client: ContainerClient = blob_service_client.create_container(
        container_name
    )

    # Create a local directory to hold blob data
    local_path = "./data"
    os.mkdir(local_path)

    # Create a file in the local data directory to upload and download
    local_file_name: str = str(uuid.uuid4()) + ".txt"
    upload_file_path: str = os.path.join(local_path, local_file_name)

    # Write text to the file
    file: os.TextIOWrapper[_WrappedBuffer] = open(file=upload_file_path, mode="w")  # type: ignore
    file.write("Hello, World!")
    file.close()

    # Create a blob client using the local file name as the name for the blob
    blob_client: BlobClient = blob_service_client.get_blob_client(
        container=container_name, blob=local_file_name
    )

    print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)

    # Upload the created file
    with open(file=upload_file_path, mode="rb") as data:
        blob_client.upload_blob(data)

    print("\nListing blobs...")

    # List the blobs in the container
    blob_list: ItemPaged[BlobProperties] = container_client.list_blobs()
    for blob in blob_list:
        print("\t" + blob.name)

    # Download the blob to a local file
    # Add 'DOWNLOAD' before the .txt extension so you can see both files in the data directory
    download_file_path: str = os.path.join(
        local_path, str.replace(local_file_name, ".txt", "DOWNLOAD.txt")
    )
    container_client = blob_service_client.get_container_client(
        container=container_name
    )
    print("\nDownloading blob to \n\t" + download_file_path)

    with open(file=download_file_path, mode="wb") as download_file:
        download_file.write(container_client.download_blob(blob.name).readall())

    # Clean up
    print("\nPress the Enter key to begin clean up")
    input()

    print("Deleting blob container...")
    container_client.delete_container()

    print("Deleting the local source and downloaded files...")
    os.remove(upload_file_path)
    os.remove(download_file_path)
    os.rmdir(local_path)

    print("Done")
except Exception as ex:
    print("Exception:")
    print(ex)
