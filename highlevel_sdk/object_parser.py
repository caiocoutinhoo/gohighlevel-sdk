from highlevel_sdk.exceptions import HighLevelError
from highlevel_sdk.models.abstract_object import AbstractObject


class ObjectParser(object):
    def parse_single(response, target_class, token_data=None):
        if not target_class:
            raise HighLevelError("Must specify target class when parsing single object")

        if isinstance(response, dict):
            return AbstractObject.create_object(response, target_class, token_data)
        else:
            raise HighLevelError("Must specify either target class calling object")

    def parse_multiple(response, target_class=None, token_data=None):
        ret = []
        for key in response.keys():
            if key in [
                "meta",
                "traceId",
                "aggregations",
                "total",
                "lastMessageId",
                "nextPage",
            ]:
                continue

            if isinstance(response[key], list):
                for json_obj in response[key]:
                    ret.append(
                        ObjectParser.parse_single(json_obj, target_class, token_data)
                    )
            else:
                ret.append(
                    ObjectParser.parse_single(response[key], target_class, token_data)
                )
        return ret
