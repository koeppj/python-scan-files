import os
import sys
from box_sdk_gen import BoxClient, BoxJWTAuth, JWTConfig, UploadFileAttributes, UploadFileAttributesParentField

def get_jwt_client(config_path, user_id=None):
    jwt_config = JWTConfig.from_config_file(config_file_path=config_path)
    jwt_auth_config = BoxJWTAuth(config=jwt_config)
    if user_id:
        jwt_auth_config = jwt_auth_config.with_user_subject(user_id)
    client = BoxClient(auth=jwt_auth_config)
    return client

def upload_files_in_directory_to_box_folder(local_dir, box_folder_id, client: BoxClient):
    for filename in os.listdir(local_dir):
        file_path = os.path.join(local_dir, filename)
        if os.path.isfile(file_path):
            print(f"Uploading {filename}...")
            with open(file_path, "rb") as f:
                uploaded_file = client.uploads.upload_file(
                    UploadFileAttributes(
                        name=filename,
                        parent=UploadFileAttributesParentField(id=box_folder_id)
                    ),
                    file=f,
                )
                print(f"Uploaded: {filename} ")

if __name__ == "__main__":
    argc = len(sys.argv)
    if argc not in (4, 5):
        print("Usage: python upload_folder_to_box.py <box_config.json> <local_dir> <box_folder_id> [as_user_id]")
        sys.exit(1)

    config_path = sys.argv[1]
    local_dir = sys.argv[2]
    box_folder_id = sys.argv[3]
    user_id = sys.argv[4] if argc == 5 else None

    if not os.path.isfile(config_path):
        print(f"ERROR: Config file not found: {config_path}")
        sys.exit(1)
    if not os.path.isdir(local_dir):
        print(f"ERROR: Local directory not found: {local_dir}")
        sys.exit(1)

    client = get_jwt_client(config_path, user_id)
    upload_files_in_directory_to_box_folder(local_dir, box_folder_id, client)
