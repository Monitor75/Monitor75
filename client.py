"""
Simple command-line client to test the Mini Cloud Storage API.
Run the server first: uvicorn main:app --reload
Then use this script to upload, list, download, and delete files.

Usage examples:
  python client.py upload myfile.txt
  python client.py list
  python client.py download myfile.txt
  python client.py delete myfile.txt
"""

import sys
import requests
import os

BASE_URL = "http://localhost:8000"


def upload(filepath: str):
    """Upload a file to the server."""
    if not os.path.exists(filepath):
        print(f"❌ File not found: {filepath}")
        return

    filename = os.path.basename(filepath)
    with open(filepath, "rb") as f:
        response = requests.post(f"{BASE_URL}/upload", files={"file": (filename, f)})

    if response.status_code == 200:
        data = response.json()
        print(f"✅ Uploaded: {data['filename']} ({data['size_bytes']} bytes)")
    else:
        print(f"❌ Upload failed: {response.text}")


def list_files():
    """List all files stored on the server."""
    response = requests.get(f"{BASE_URL}/files")

    if response.status_code == 200:
        data = response.json()
        print(f"\n📁 Files stored ({data['total_files']} total):")
        print("-" * 50)
        if not data["files"]:
            print(" (no files yet)")
        for f in data["files"]:
            print(f" 📄 {f['filename']}")
            print(f" Size: {f['file_size']} bytes | Type: {f['content_type']}")
            print(f" Uploaded: {f['uploaded_at']}")
        print("-" * 50)
    else:
        print(f"❌ Failed to list files: {response.text}")


def download(filename: str):
    """Download a file from the server."""
    response = requests.get(f"{BASE_URL}/download/{filename}", stream=True)

    if response.status_code == 200:
        save_path = f"downloaded_{filename}"
        with open(save_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"✅ Downloaded '{filename}' → saved as '{save_path}'")
    else:
        print(f"❌ Download failed: {response.text}")


def delete(filename: str):
    """Delete a file from the server."""
    response = requests.delete(f"{BASE_URL}/delete/{filename}")

    if response.status_code == 200:
        print(f"✅ Deleted: {filename}")
    else:
        print(f"❌ Delete failed: {response.text}")


def print_usage():
    print("Usage:")
    print(" python client.py upload <filepath>")
    print(" python client.py list")
    print(" python client.py download <filename>")
    print(" python client.py delete <filename>")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print_usage()
        sys.exit(1)

    command = sys.argv[1].lower()

    if command == "upload" and len(sys.argv) == 3:
        upload(sys.argv[2])
    elif command == "list":
        list_files()
    elif command == "download" and len(sys.argv) == 3:
        download(sys.argv[2])
    elif command == "delete" and len(sys.argv) == 3:
        delete(sys.argv[2])
    else:
        print_usage()
