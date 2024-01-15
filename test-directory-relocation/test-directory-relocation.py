
from azure.storage.filedatalake import FileSystemClient , DataLakeDirectoryClient, DataLakeServiceClient, DataLakeFileClient
# get the key run it

file_system = (
    FileSystemClient.from_connection_string(
    # f"https://b2aistaging.dfs.core.windows.net/;sharedkey={key}",
    f"DefaultEndpointsProtocol=https;AccountName=b2aistaging;AccountKey=ARD+Hr4hEquCtqw9jnmSgaO/hxIg5QBXZBVurhVrWt+nnK4KI34IgwCxCLmRUCwND6Sz5rMSy5xt+ASt1rMvYw==;EndpointSuffix=core.windows.net",
    file_system_name="stage-1-container"
))

dir_name = "AI-READI/temp/copy-test"

directory = file_system.get_directory_client(dir_name)

new_dir_name = "AI-READI/copy-test"
directory.rename_directory(
        new_name=f"{directory.file_system_name}/{new_dir_name}")

