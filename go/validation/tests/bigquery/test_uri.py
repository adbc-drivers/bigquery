# Copyright (c) 2025 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import urllib.parse

import adbc_driver_manager.dbapi
import pytest


@pytest.mark.feature(group="Configuration", name="Connect with URI")
def test_application_default_credentials_uri_parsing(
    driver_path: str,
) -> None:
    """Test that Application Default Credentials URI is parsed correctly."""
    uri = "bigquery:///dummyproject?OAuthType=0&DatasetId=dummydataset&Location=dummylocation"

    params = {
        "uri": uri,
    }

    with adbc_driver_manager.AdbcDatabase(driver=driver_path, **params) as db:
        auth_type = db.get_option("adbc.bigquery.sql.auth_type")
        assert auth_type == "adbc.bigquery.sql.auth_type.app_default_credentials"

        project_id = db.get_option("adbc.bigquery.sql.project_id")
        assert project_id == "dummyproject"

        dataset_id = db.get_option("adbc.bigquery.sql.dataset_id")
        assert dataset_id == "dummydataset"

        location = db.get_option("adbc.bigquery.sql.location")
        assert location == "dummylocation"


@pytest.mark.feature(group="Configuration", name="Connect with URI")
def test_service_account_file_uri_parsing(
    driver_path: str,
) -> None:
    """Test that service account JSON file URI is parsed correctly."""
    credentials_path = "/path/to/service-account.json"
    uri = f"bigquery:///dummyproject?OAuthType=1&AuthCredentials={credentials_path}&DatasetId=dummydataset&TableId=mytable"

    params = {
        "uri": uri,
    }

    with adbc_driver_manager.AdbcDatabase(driver=driver_path, **params) as db:
        auth_type = db.get_option("adbc.bigquery.sql.auth_type")
        assert auth_type == "adbc.bigquery.sql.auth_type.json_credential_file"

        computed_credentials = db.get_option("adbc.bigquery.sql.auth_credentials")
        assert computed_credentials == credentials_path

        project_id = db.get_option("adbc.bigquery.sql.project_id")
        assert project_id == "dummyproject"

        dataset_id = db.get_option("adbc.bigquery.sql.dataset_id")
        assert dataset_id == "dummydataset"

        table_id = db.get_option("adbc.bigquery.sql.table_id")
        assert table_id == "mytable"


@pytest.mark.feature(group="Configuration", name="Connect with URI")
def test_service_account_string_uri_parsing(
    driver_path: str,
) -> None:
    """Test that service account JSON string URI is parsed correctly."""
    json_credentials = '{"type":"service_account","project_id":"test"}'
    encoded_credentials = urllib.parse.quote(json_credentials)
    uri = f"bigquery:///dummyproject?OAuthType=2&AuthCredentials={encoded_credentials}&DatasetId=dummydataset&Location=dummylocation"

    params = {
        "uri": uri,
    }

    with adbc_driver_manager.AdbcDatabase(driver=driver_path, **params) as db:
        auth_type = db.get_option("adbc.bigquery.sql.auth_type")
        assert auth_type == "adbc.bigquery.sql.auth_type.json_credential_string"

        computed_credentials = db.get_option("adbc.bigquery.sql.auth_credentials")
        assert computed_credentials == json_credentials

        project_id = db.get_option("adbc.bigquery.sql.project_id")
        assert project_id == "dummyproject"

        dataset_id = db.get_option("adbc.bigquery.sql.dataset_id")
        assert dataset_id == "dummydataset"

        location = db.get_option("adbc.bigquery.sql.location")
        assert location == "dummylocation"


@pytest.mark.feature(group="Configuration", name="Connect with URI")
def test_user_oauth_uri_parsing(
    driver_path: str,
) -> None:
    """Test that User OAuth authentication URI is parsed correctly."""
    client_id = "test_client_id"
    client_secret = "test_client_secret"
    refresh_token = "test_refresh_token"
    uri = f"bigquery:///dummyproject?OAuthType=3&AuthClientId={client_id}&AuthClientSecret={client_secret}&AuthRefreshToken={refresh_token}&DatasetId=dummydataset&Location=dummylocation"

    params = {
        "uri": uri,
    }

    with adbc_driver_manager.AdbcDatabase(driver=driver_path, **params) as db:
        auth_type = db.get_option("adbc.bigquery.sql.auth_type")
        assert auth_type == "adbc.bigquery.sql.auth_type.user_authentication"

        endpoint = db.get_option("adbc.bigquery.sql.endpoint")
        assert endpoint == "bigquery.googleapis.com:443"

        computed_client_id = db.get_option("adbc.bigquery.sql.auth.client_id")
        assert computed_client_id == client_id

        computed_client_secret = db.get_option("adbc.bigquery.sql.auth.client_secret")
        assert computed_client_secret == client_secret

        computed_refresh_token = db.get_option("adbc.bigquery.sql.auth.refresh_token")
        assert computed_refresh_token == refresh_token

        project_id = db.get_option("adbc.bigquery.sql.project_id")
        assert project_id == "dummyproject"

        dataset_id = db.get_option("adbc.bigquery.sql.dataset_id")
        assert dataset_id == "dummydataset"

        location = db.get_option("adbc.bigquery.sql.location")
        assert location == "dummylocation"


@pytest.mark.feature(group="Configuration", name="Connect with URI")
def test_custom_endpoint_uri_parsing(
    driver_path: str,
) -> None:
    """Test that custom endpoint in URI is parsed correctly."""
    uri = "bigquery://bigquery.dummyapis.com:445/dummyproject?DatasetId=dummydataset&QuotaProject=billing-project"

    params = {
        "uri": uri,
    }

    with adbc_driver_manager.AdbcDatabase(driver=driver_path, **params) as db:
        auth_type = db.get_option("adbc.bigquery.sql.auth_type")
        assert auth_type == "adbc.bigquery.sql.auth_type.app_default_credentials"

        endpoint = db.get_option("adbc.bigquery.sql.endpoint")
        assert endpoint == "bigquery.dummyapis.com:445"

        project_id = db.get_option("adbc.bigquery.sql.project_id")
        assert project_id == "dummyproject"

        dataset_id = db.get_option("adbc.bigquery.sql.dataset_id")
        assert dataset_id == "dummydataset"

        quota_project = db.get_option("adbc.bigquery.sql.auth.quota_project")
        assert quota_project == "billing-project"


@pytest.mark.feature(group="Configuration", name="Connect with URI")
def test_custom_endpoint_without_port_uri_parsing(
    driver_path: str,
) -> None:
    """Test that custom endpoint without port defaults to 443."""
    uri = "bigquery://bigquery.dummyapis.com/dummyproject?OAuthType=0&DatasetId=dummydataset"

    params = {
        "uri": uri,
    }

    with adbc_driver_manager.AdbcDatabase(driver=driver_path, **params) as db:
        auth_type = db.get_option("adbc.bigquery.sql.auth_type")
        assert auth_type == "adbc.bigquery.sql.auth_type.app_default_credentials"

        endpoint = db.get_option("adbc.bigquery.sql.endpoint")
        assert endpoint == "bigquery.dummyapis.com:443"

        project_id = db.get_option("adbc.bigquery.sql.project_id")
        assert project_id == "dummyproject"

        dataset_id = db.get_option("adbc.bigquery.sql.dataset_id")
        assert dataset_id == "dummydataset"


@pytest.mark.feature(group="Configuration", name="Connect with URI")
def test_port_without_host_uri_parsing(
    driver_path: str,
) -> None:
    """Test that port without host in URI defaults to custom port on the default host."""
    uri = "bigquery://:448/dummyproject?OAuthType=0"

    params = {
        "uri": uri,
    }

    with adbc_driver_manager.AdbcDatabase(driver=driver_path, **params) as db:
        auth_type = db.get_option("adbc.bigquery.sql.auth_type")
        assert auth_type == "adbc.bigquery.sql.auth_type.app_default_credentials"

        endpoint = db.get_option("adbc.bigquery.sql.endpoint")
        assert endpoint == "bigquery.googleapis.com:448"

        project_id = db.get_option("adbc.bigquery.sql.project_id")
        assert project_id == "dummyproject"


@pytest.mark.feature(group="Configuration", name="Connect with URI")
def test_missing_project_id_uri_error(
    driver_path: str,
) -> None:
    """Test that missing project ID in URI raises error during parsing."""
    uri = "bigquery:///?OAuthType=0"

    with pytest.raises(
        adbc_driver_manager.dbapi.ProgrammingError,
        match="project ID is required in URI path",
    ):
        with adbc_driver_manager.dbapi.connect(
            driver=driver_path,
            db_kwargs={"uri": uri},
        ):
            pass


@pytest.mark.feature(group="Configuration", name="Connect with URI")
def test_minimal_uri_parsing(
    driver_path: str,
) -> None:
    """Test that minimal URI (project only) works with default authentication."""
    uri = "bigquery:///dummyproject"

    params = {
        "uri": uri,
    }

    with adbc_driver_manager.AdbcDatabase(driver=driver_path, **params) as db:
        auth_type = db.get_option("adbc.bigquery.sql.auth_type")
        assert auth_type == "adbc.bigquery.sql.auth_type.app_default_credentials"

        project_id = db.get_option("adbc.bigquery.sql.project_id")
        assert project_id == "dummyproject"


@pytest.mark.feature(group="Configuration", name="Connect with URI")
def test_invalid_oauth_type_uri_error(
    driver_path: str,
) -> None:
    """Test that invalid OAuthType in URI raises error during parsing."""
    uri = "bigquery:///my-project-123?OAuthType=999"

    with pytest.raises(
        adbc_driver_manager.dbapi.ProgrammingError,
        match="invalid OAuthType value",
    ):
        with adbc_driver_manager.dbapi.connect(
            driver=driver_path,
            db_kwargs={"uri": uri},
        ):
            pass


@pytest.mark.feature(group="Configuration", name="Connect with URI")
def test_invalid_uri_scheme_error(
    driver_path: str,
) -> None:
    """Test that invalid URI scheme raises error during parsing."""
    uri = "mysql://localhost/database"

    with pytest.raises(
        adbc_driver_manager.dbapi.ProgrammingError,
        match="invalid BigQuery URI scheme",
    ):
        with adbc_driver_manager.dbapi.connect(
            driver=driver_path,
            db_kwargs={"uri": uri},
        ):
            pass


@pytest.mark.feature(group="Configuration", name="Connect with URI")
def test_missing_auth_credentials_service_account_uri_error(
    driver_path: str,
) -> None:
    """Test that missing AuthCredentials for service account raises error during parsing."""
    uri = "bigquery:///my-project-123?OAuthType=1"

    with pytest.raises(
        adbc_driver_manager.dbapi.ProgrammingError,
        match="AuthCredentials required for service account authentication",
    ):
        with adbc_driver_manager.dbapi.connect(
            driver=driver_path,
            db_kwargs={"uri": uri},
        ):
            pass


@pytest.mark.feature(group="Configuration", name="Connect with URI")
def test_missing_oauth_credentials_uri_error(
    driver_path: str,
) -> None:
    """Test that incomplete OAuth credentials in URI raise error during parsing."""
    uri = "bigquery:///my-project-123?OAuthType=3&AuthClientId=client_id"

    with pytest.raises(
        adbc_driver_manager.dbapi.ProgrammingError,
        match="AuthClientId, AuthClientSecret and AuthRefreshToken required for OAuth authentication",
    ):
        with adbc_driver_manager.dbapi.connect(
            driver=driver_path,
            db_kwargs={"uri": uri},
        ):
            pass


@pytest.mark.feature(group="Configuration", name="Connect with URI")
def test_end_to_end_real_connection(
    driver_path: str,
    bigquery_project: str,
    bigquery_dataset: str,
) -> None:
    """Test end-to-end connection with real BigQuery project."""
    uri = f"bigquery://bigquery.googleapis.com/{bigquery_project}?DatasetId={bigquery_dataset}"

    with adbc_driver_manager.dbapi.connect(
        driver=driver_path, db_kwargs={"uri": uri}, autocommit=True
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
