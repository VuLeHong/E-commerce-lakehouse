import os
from pathlib import Path
from dotenv import load_dotenv
import requests
import json

from minio import Minio

import requests
import json

# ---------------------------
# Superset setup
# ---------------------------
def setup_superset_connection(base_url, username, password, conn_name, sqlalchemy_uri):
    """
    Tạo connection (database) trong Superset qua API.
    
    Args:
        base_url: URL của Superset, ví dụ "http://superset:8088"
        username: admin user
        password: admin password
        conn_name: tên connection hiển thị trong Superset
        sqlalchemy_uri: chuỗi SQLAlchemy để kết nối Dremio
                        ví dụ: "dremio+pyodbc://user:pass@dremio:31010/your_db"
    """
    # login để lấy JWT token
    login_url = f"{base_url}/api/v1/security/login"
    payload = {"username": username, "password": password, "provider": "db"}
    r = requests.post(login_url, json=payload)
    r.raise_for_status()
    token = r.json()["access_token"]

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    db_url = f"{base_url}/api/v1/database/"

    payload = {
        "database_name": conn_name,
        "sqlalchemy_uri": sqlalchemy_uri,
        "expose_in_sqllab": True,
        "allow_run_async": True,
    }

    r = requests.post(db_url, headers=headers, data=json.dumps(payload))

    if r.status_code == 201:
        return {"status": "created", "data": r.json()}
    elif r.status_code == 422:
        return {"status": "exists", "message": "Database already exists"}
    else:
        r.raise_for_status()


def register_superset_dataset(base_url, username, password, database_id, schema_name, table_name):
    """
    Tạo dataset (table) trong Superset.
    """
    # login
    login_url = f"{base_url}/api/v1/security/login"
    payload = {"username": username, "password": password, "provider": "db"}
    r = requests.post(login_url, json=payload)
    r.raise_for_status()
    token = r.json()["access_token"]

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    dataset_url = f"{base_url}/api/v1/dataset/"

    payload = {
        "database": database_id,
        "schema": schema_name,
        "table_name": table_name,
    }

    r = requests.post(dataset_url, headers=headers, data=json.dumps(payload))

    if r.status_code == 201:
        return {"status": "created", "data": r.json()}
    elif r.status_code == 422:
        return {"status": "exists", "message": "Dataset already exists"}
    else:
        r.raise_for_status()


# ---------------------------
# Dremio setup
# ---------------------------
def setup_dremio_space(base_url, username, password, space_name):
    """
    Tạo Space trong Dremio để chứa dataset.
    
    Args:
        base_url: URL Dremio REST API, ví dụ "http://dremio:9047"
        username: admin user
        password: admin password
        space_name: tên space muốn tạo
    """
    # login
    login_url = f"{base_url}/apiv2/login"
    payload = {"userName": username, "password": password}
    r = requests.post(login_url, json=payload)
    r.raise_for_status()
    token = r.json()["token"]

    headers = {"Authorization": f"_dremio{token}", "Content-Type": "application/json"}
    space_url = f"{base_url}/api/v3/catalog"

    payload = {"entityType": "space", "name": space_name}
    r = requests.post(space_url, headers=headers, data=json.dumps(payload))

    if r.status_code == 200:
        return {"status": "created", "data": r.json()}
    elif r.status_code == 409:
        return {"status": "exists", "message": "Space already exists"}
    else:
        r.raise_for_status()

            
def check_minio_has_data(bucket, prefix) -> bool:
    client = Minio(
        "minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

    return any(client.list_objects(bucket, prefix=prefix, recursive=True))