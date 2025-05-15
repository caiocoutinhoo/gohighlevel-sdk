import json
import collections.abc as collections_abc
from highlevel_sdk.client import HighLevelClient


class AbstractObject(collections_abc.MutableMapping):

    """
    Represents an abstract object
    """

    class Fields:
        pass

    def __init__(self, token_data=None, id=None):
        self._data = {}
        self.api = HighLevelClient

        if id:
            self["id"] = id

        if token_data:
            self.set_token_data(token_data)

    def set_token_data(self, token_data):
        self.token_data = token_data

    def get_token_data(self):
        return self.token_data

    def refresh_token(self):
        if not self.token_data:
            raise ValueError("Token data is not set")

        from highlevel_sdk.auth import refresh_token

        token_data = refresh_token(self.token_data["refresh_token"])

        self.set_token_data(token_data)

        return

    def __getitem__(self, key):
        return self._data[str(key)]

    def __setitem__(self, key, value):
        if key.startswith("_"):
            self.__setattr__(key, value)
        else:
            self._data[key] = value
        return self

    def __eq__(self, other):
        return (
            other is not None
            and hasattr(other, "export_all_data")
            and self.export_all_data() == other.export_all_data()
        )

    def __delitem__(self, key):
        del self._data[key]

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def __contains__(self, key):
        return key in self._data

    def __repr__(self):
        return "<%s> %s" % (
            self.__class__.__name__,
            json.dumps(
                self.export_value(self._data),
                sort_keys=True,
                indent=4,
                separators=(",", ": "),
            ),
        )

    def get_endpoint(self):
        """
        Returns the endpoint for this object
        """
        raise NotImplementedError

    def api_get(self, params=None):
        """
        Returns the object from the API
        """
        method = "GET"
        path = self.get_endpoint()
        token_data = self.get_token_data()
        response = self.api._call(method, path, token_data=token_data, data=params)
        self._set_data(response.json())
        return self

    # reads in data from json object
    def _set_data(self, data):
        """
        sets data from a json object
        """
        if isinstance(data, dict):
            for key, value in data.items():
                self[key] = value
        else:
            # raise error
            raise ValueError("Bad data to set object data")
        self._json = data

    def export_value(self, data):
        if isinstance(data, AbstractObject):
            data = data.export_all_data()
        elif isinstance(data, dict):
            data = dict(
                (k, self.export_value(v)) for k, v in data.items() if v is not None
            )
        elif isinstance(data, list):
            data = [self.export_value(v) for v in data]
        return data

    def export_all_data(self):
        return self.export_value(self._data)

    def create_object(data, target_class, token_data):
        new_object = target_class()
        new_object._set_data(data)
        new_object.set_token_data(token_data)
        return new_object
