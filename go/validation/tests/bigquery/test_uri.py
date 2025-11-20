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


@pytest.mark.feature(group="Configuration", name="URI Parsing")
def test_application_default_credentials_uri_parsing(
    driver_path: str,
    bigquery_project: str,
    bigquery_dataset: str,
) -> None:
    """Test that Application Default Credentials URI is parsed correctly."""
    uri = f"bigquery:///{bigquery_project}?OAuthType=0&DatasetId={bigquery_dataset}"

    # This should parse the URI successfully without throwing parsing errors
    # Authentication errors are expected and fine
    with adbc_driver_manager.dbapi.connect(
        driver=driver_path,
        db_kwargs={"uri": uri}
    ):
        pass


@pytest.mark.feature(group="Configuration", name="URI Parsing")
def test_service_account_file_uri_parsing(
    driver_path: str,
    bigquery_project: str,
    bigquery_dataset: str,
) -> None:
    """Test that service account JSON file URI is parsed correctly."""
    credentials_path = "/path/to/service-account.json"
    uri = f"bigquery:///{bigquery_project}?OAuthType=1&AuthCredentials={credentials_path}&DatasetId={bigquery_dataset}"

    with adbc_driver_manager.dbapi.connect(
        driver=driver_path,
        db_kwargs={"uri": uri}
    ):
        pass


@pytest.mark.feature(group="Configuration", name="URI Parsing")
def test_service_account_string_uri_parsing(
    driver_path: str,
    bigquery_project: str,
    bigquery_dataset: str,
) -> None:
    """Test that service account JSON string URI is parsed correctly."""
    json_credentials = '{"type":"service_account","project_id":"test"}'
    encoded_credentials = urllib.parse.quote(json_credentials)
    uri = f"bigquery:///{bigquery_project}?OAuthType=2&AuthCredentials={encoded_credentials}&DatasetId={bigquery_dataset}"

    with adbc_driver_manager.dbapi.connect(
        driver=driver_path,
        db_kwargs={"uri": uri}
    ):
        pass


@pytest.mark.feature(group="Configuration", name="URI Parsing")
def test_user_oauth_uri_parsing(
    driver_path: str,
    bigquery_project: str,
    bigquery_dataset: str,
) -> None:
    """Test that User OAuth authentication URI is parsed correctly."""
    client_id = "test_client_id"
    client_secret = "test_client_secret"
    refresh_token = "test_refresh_token"
    uri = f"bigquery:///{bigquery_project}?OAuthType=3&AuthClientId={client_id}&AuthClientSecret={client_secret}&AuthRefreshToken={refresh_token}&DatasetId={bigquery_dataset}"

    with adbc_driver_manager.dbapi.connect(
        driver=driver_path,
        db_kwargs={"uri": uri}
    ):
        pass


@pytest.mark.feature(group="Configuration", name="URI Parsing")
def test_default_auth_uri_parsing(
    driver_path: str,
    bigquery_project: str,
    bigquery_dataset: str,
) -> None:
    """Test that Default auth type URI is parsed correctly."""
    uri = f"bigquery:///{bigquery_project}?OAuthType=4&DatasetId={bigquery_dataset}"

    with adbc_driver_manager.dbapi.connect(
        driver=driver_path,
        db_kwargs={"uri": uri}
    ):
        pass


@pytest.mark.feature(group="Configuration", name="URI Parsing")
def test_location_parameter_uri_parsing(
    driver_path: str,
    bigquery_project: str,
    bigquery_dataset: str,
) -> None:
    """Test that BigQuery location parameter is parsed correctly from URI."""
    location = "EU"
    uri = f"bigquery:///{bigquery_project}?OAuthType=0&DatasetId={bigquery_dataset}&Location={location}"

    with adbc_driver_manager.dbapi.connect(
        driver=driver_path,
        db_kwargs={"uri": uri}
    ):
        pass


@pytest.mark.feature(group="Configuration", name="URI Parsing")
def test_custom_endpoint_uri_parsing(
    driver_path: str,
    bigquery_project: str,
    bigquery_dataset: str,
) -> None:
    """Test that custom endpoint in URI is parsed correctly."""
    uri = f"bigquery://bigquery.googleapis.com:443/{bigquery_project}?OAuthType=0&DatasetId={bigquery_dataset}"

    with adbc_driver_manager.dbapi.connect(
        driver=driver_path,
        db_kwargs={"uri": uri}
    ):
        pass


@pytest.mark.feature(group="Configuration", name="URI Parsing")
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


@pytest.mark.feature(group="Configuration", name="URI Parsing")
def test_missing_oauth_type_uri_error(
    driver_path: str,
) -> None:
    """Test that missing OAuthType in URI raises error during parsing."""
    uri = "bigquery:///my-project-123"

    with pytest.raises(
        adbc_driver_manager.dbapi.ProgrammingError,
        match="OAuthType parameter is required",
    ):
        with adbc_driver_manager.dbapi.connect(
            driver=driver_path,
            db_kwargs={"uri": uri},
        ):
            pass


@pytest.mark.feature(group="Configuration", name="URI Parsing")
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


@pytest.mark.feature(group="Configuration", name="URI Parsing")
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


@pytest.mark.feature(group="Configuration", name="URI Parsing")
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


@pytest.mark.feature(group="Configuration", name="URI Parsing")
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