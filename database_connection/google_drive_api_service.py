from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from .google_api_service import create_service_client
import pandas as pd
import json
import io


class GoogleDriveService:

    def __init__(self, client_file, scope, include_shared_drive=False):
        self.client_file = client_file
        self.scope = scope
        self.service = self.create_service()
        self.include_shared_drive = include_shared_drive

    def create_service(self):
        api_name = 'drive'
        api_version = 'v3'
        scopes = ['https://www.googleapis.com/auth/drive',
                  'https://www.googleapis.com/auth/drive.file',
                  'https://www.googleapis.com/auth/drive.readonly',
                  'https://www.googleapis.com/auth/drive.metadata.readonly',
                  'https://www.googleapis.com/auth/drive.metadata',
                  'https://www.googleapis.com/auth/drive.photos.readonly'
                  ]
        scope_exists = self.scope in scopes
        if scope_exists:
            return create_service_client(self.client_file, api_name, api_version, self.scope)
        else:
            raise ValueError(
                'Please choose a scope from the list at https://developers.google.com/drive/api/v3/reference/permissions/list')

    def ls(self, folder_id):
        query = f"parents = '{folder_id}'"
        results = self.service.files().list(pageSize=10,
                                            fields="nextPageToken, files(id, name)",
                                            includeItemsFromAllDrives=self.include_shared_drive,
                                            supportsAllDrives=self.include_shared_drive,
                                            q=query).execute()
        items = results.get('files', [])
        next_page_token = results.get('nextPageToken', False)

        while next_page_token:
            results = self.service.files().list(pageSize=10,
                                                fields="nextPageToken, files(id, name)",
                                                includeItemsFromAllDrives=self.include_shared_drive,
                                                supportsAllDrives=self.include_shared_drive,
                                                q=query).execute()
            items.extend(results.get('files', []))
            next_page_token = results.get('nextPageToken', False)

        item_df = pd.DataFrame(items)
        return item_df

    def download_file(self, gd_file_id, file_out_name):
        request = self.service.files().get_media(fileId=gd_file_id)

        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fd=fh, request=request)

        done = False
        while not done:
            status, done = downloader.next_chunk()
            print(f"Downloading next chunk, progress - {status.progress() * 100}")

        fh.seek(0)

        with open(file_out_name, 'wb') as f:
            f.write(fh.read())
            f.close()

    def upload_file(self, in_file_name, in_file_path, gd_folder_id):
        with open('mime_types.json') as f:
            mime_types = json.load(f)

        suffix = in_file_name.split('.')[-1]
        mime_type = mime_types[suffix]

        file_metadata = {'name': in_file_name,
                         'parents': [gd_folder_id]}
        media = MediaFileUpload(in_file_path + in_file_name, mimetype=mime_type)

        self.service.files().create(body=file_metadata,
                                    media_body=media,
                                    supportsAllDrives=self.include_shared_drive,
                                    fields='id').execute()

    def create_folder(self, folder_to_create, parent_folder_id):
        mime_type = 'application/vnd.google-apps.folder'
        file_metadata = {'name': folder_to_create,
                         'mimeType': mime_type,
                         'parents': [parent_folder_id]}
        self.service.files().create(body=file_metadata,
                                    supportsAllDrives=self.include_shared_drive).execute()
