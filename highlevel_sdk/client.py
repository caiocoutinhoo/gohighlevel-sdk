from copy import deepcopy
import json
from requests import get, post, put, delete
from time import sleep

from highlevel_sdk.config import HighLevelConfig
from highlevel_sdk.exceptions import HighLevelRequestException


class HighLevelClient(object):
    """
    Encapsulates session attributes and methods to make API calls.
    """

    def __init__(self) -> None:
        pass

    def build_headers(access_token=None):
        assert access_token != None, "Must provide access token"
        headers = {
            "Content-Type": "application/json",
            "version": HighLevelConfig.VERSION,
        }
        if access_token:
            headers["Authorization"] = f"Bearer {access_token}"
        return headers

    @classmethod
    def _call(cls, method, path, token_data=None, data=None):
        path = HighLevelConfig.API_BASE_URL + path
        access_token = token_data["access_token"]
        headers = cls.build_headers(access_token=access_token)

        # Try up to 3 times to make the request
        RETRY_DELAY = 3
        for i in range(3):
            try:
                if method == "GET":
                    response = get(path, headers=headers, params=data)
                elif method == "DELETE":
                    response = delete(path, headers=headers, params=data)
                elif method == "POST":
                    response = post(path, headers=headers, data=json.dumps(data))
                elif method == "PUT":
                    response = put(path, headers=headers, data=json.dumps(data))

                break
            except Exception as e:
                if i == 2:
                    raise e
                sleep(RETRY_DELAY)
                RETRY_DELAY *= 2
                continue

        highlevel_response = HighLevelResponse(
            body=response.text,
            headers=response.headers,
            status_code=response.status_code,
            call={"method": method, "path": path, "params": data, "headers": headers},
        )

        # push token_data to response
        highlevel_response.token_data = token_data

        if highlevel_response.is_error():
            raise highlevel_response.error()

        return highlevel_response


class HighLevelResponse(object):
    """
    Encapsulates response attributes and methods.
    """

    def __init__(self, body, headers, status_code, call) -> None:
        self.body = body
        self.headers = headers
        self.status_code = status_code
        self.call = call

    def is_error(self):
        return self.status_code >= 400

    def error(self):
        if self.is_error():
            return HighLevelRequestException(
                "Call to HighLevel API was unsuccessful.",
                request_context=self.call,
                http_headers=self.headers,
                http_status=self.status_code,
                body=self.body,
            )
        else:
            return None

    def json(self):
        return json.loads(self.body)

    def text(self):
        return self.body

    def __repr__(self):
        return f"<HighLevelResponse {self.status_code} {self.body}>"


class HighLevelRequest(object):
    """
    Encapsulates request attributes and methods
    """

    def __init__(
        self,
        method,
        node,
        endpoint,
        token_data=None,
        api=None,
        api_type=None,
        target_class=None,
        response_parser=None,
        custom_pagination_fn=None,
    ) -> None:
        """
        Args:
            method : The HTTP method to use for the request.
            node : The node to use for the request.
            endpoint : The endpoint to use for the request.
            api_type (optional): The type of API call to make.
            param_checker (optional): The type checker to use for the request.
            target_class (optional): The class to use for the request.
            response_parser (optional): The parser to use for the response.
            custom_pagination_fn (optional): Custom pagination function to use for the cursor.

        """
        self._method = method
        self._node = node
        self._endpoint = endpoint
        self.token_data = token_data
        self._api = api
        self._api_type = api_type
        if bool(node):
            self._path = f"{endpoint}/{node}"
        else:
            self._path = f"{endpoint}/"
        self._params = {}
        self._target_class = target_class
        self._response_parser = response_parser
        self._custom_pagination_fn = custom_pagination_fn

    def add_param(self, key, value):
        self._params[key] = self._extract_value(value)
        return self

    def add_params(self, params):
        if params is None:
            return self
        for key in params.keys():
            self.add_param(key, params[key])
        return self

    def _extract_value(self, value):
        if hasattr(value, "export_all_data"):
            return value.export_all_data()
        elif isinstance(value, list):
            return [self._extract_value(item) for item in value]
        elif isinstance(value, dict):
            return dict(
                (self._extract_value(k), self._extract_value(v))
                for (k, v) in value.items()
            )
        else:
            return value

    def execute(self):
        params = deepcopy(self._params)
        if self._api_type == "EDGE" and self._method == "GET":
            cursor = Cursor(
                target_objects_class=self._target_class,
                params=params,
                endpoint=self._endpoint,
                token_data=self.token_data,
                api=self._api,
                object_parser=self._response_parser,
                custom_pagination_fn=self._custom_pagination_fn,
            )
            cursor.load_next_page()
            return cursor
        response = self._api._call(
            method=self._method,
            path=self._path,
            data=params,
            token_data=self.token_data,
        )

        if response.error():
            raise response.error()
        if self._response_parser:
            return self._response_parser.parse_single(
                response.json(), self._target_class, self.token_data
            )
        else:
            return response


class Cursor(object):
    """
    Iterates over pages of data.
    """

    def __init__(
        self,
        target_objects_class,
        params,
        endpoint,
        token_data,
        api,
        object_parser,
        custom_pagination_fn=None,
    ) -> None:
        """
        Args:
            target_objects_class : an instance the AbstractObject class. Must have an ID
            params : The parameters to use for the request.
            node : The node to use for the request.
            endpoint : The endpoint to use for the request.
            object_parser : The parser to use for the response.
            custom_pagination_fn (optional): Custom pagination function to use for the cursor.
        """

        self._target_objects_class = target_objects_class
        self._params = params
        self._endpoint = endpoint
        self.token_data = token_data
        self._api = api
        self._path = f"{endpoint}"
        self._object_parser = object_parser
        self._queue = []
        self._headers = None
        self._has_next_page = False
        self._start_after_id = None
        self.custom_pagination_fn = custom_pagination_fn

    def __repr__(self):
        return str(self._queue)

    def __len__(self):
        return len(self._queue)

    def __iter__(self):
        return self

    def __next__(self):
        if not self._queue and not self.load_next_page():
            raise StopIteration()

        return self._queue.pop(0)

    def __getitem__(self, index):
        return self._queue[index]

    def headers(self):
        return self._headers

    def load_next_page(self):
        """
        Loads the next page of data.

        Returns:
            bool: True if there is a next page, False otherwise.
        """
        if self.custom_pagination_fn:
            return self.custom_pagination_fn(self)
        else:
            return self.load_next_page_meta()

    def load_next_page_meta(self):

        response = self._api._call(
            method="GET",
            path=self._path,
            data=self._params,
            token_data=self.token_data,
        )

        self._headers = response.headers

        body = response.json()
        self._queue = self._object_parser.parse_multiple(
            body, self._target_objects_class, self.token_data
        )
        if not self._queue:
            return False
        if not body.get("meta"):
            return False
        self._has_next_page = (
            body["meta"]["nextPage"] is not None
            and body["meta"]["startAfter"] is not None
        )
        self._params["startAfter"] = body["meta"]["startAfter"]
        self._params["startAfterId"] = body["meta"]["startAfterId"]

        if not self._has_next_page:
            return False

        return True
