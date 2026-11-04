import requests
from requests.auth import HTTPBasicAuth


class ADTClient:
    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        timeout: int = 120,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(username, password)

        self.endpoint = (
            f"{self.base_url}/sap/bc/adt/datapreview/freestyle"
        )

    def execute_request(
        self,
        query: str,
        row_limit: int = 100,
    ) -> requests.Response:
        if not query or not query.strip():
            raise ValueError("Query cannot be empty.")

        if row_limit < 1:
            raise ValueError("Row limit must be greater than zero.")

        response = self.session.post(
            self.endpoint,
            params={"rows": row_limit},
            data=query,
            timeout=self.timeout,
        )

        response.raise_for_status()
        return response

    def close(self):
        self.session.close()