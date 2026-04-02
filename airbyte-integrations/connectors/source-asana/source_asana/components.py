#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from dataclasses import dataclass
from pkgutil import get_data
from typing import Any, Callable, Mapping, MutableMapping, Optional, Union

import requests
from yaml import safe_load

from airbyte_cdk.sources.declarative.requesters.http_requester import HttpRequester
from airbyte_cdk.sources.declarative.requesters.request_options.interpolated_request_input_provider import InterpolatedRequestInputProvider
from airbyte_cdk.sources.declarative.types import StreamSlice, StreamState
from airbyte_cdk.utils.traced_exception import AirbyteTracedException


@dataclass
class AsanaHttpRequester(HttpRequester):
    request_parameters: Optional[Union[str, Mapping[str, str]]] = None

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        super().__post_init__(parameters)
        self.request_parameters = self.request_parameters or {}
        self._request_params_interpolator = InterpolatedRequestInputProvider(
            config=self.config, request_inputs=self.request_parameters, parameters=parameters
        )

    def send_request(
        self,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
        path: Optional[str] = None,
        request_headers: Optional[Mapping[str, Any]] = None,
        request_params: Optional[Mapping[str, Any]] = None,
        request_body_data: Optional[Union[Mapping[str, Any], str]] = None,
        request_body_json: Optional[Mapping[str, Any]] = None,
        log_formatter: Optional[Callable[[requests.Response], Any]] = None,
    ) -> Optional[requests.Response]:
        try:
            return super().send_request(
                stream_state,
                stream_slice,
                next_page_token,
                path,
                request_headers,
                request_params,
                request_body_data,
                request_body_json,
                log_formatter,
            )
        except AirbyteTracedException as e:
            if "Unauthorized" not in str(e):
                raise
            # Access token expired mid-sync: force a refresh and retry once.
            # The CDK bakes the auth header into the PreparedRequest before retrying,
            # so retries use the same expired token. We refresh here and let the next
            # call to super().send_request() create a fresh PreparedRequest.
            if hasattr(self.authenticator, "refresh_and_set_access_token"):
                self.authenticator.refresh_and_set_access_token()
                return super().send_request(
                    stream_state,
                    stream_slice,
                    next_page_token,
                    path,
                    request_headers,
                    request_params,
                    request_body_data,
                    request_body_json,
                    log_formatter,
                )
            raise

    def get_request_params(
        self,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        params = self._request_params_interpolator.eval_request_inputs(stream_state, stream_slice, next_page_token)
        if isinstance(params, dict):
            params.update(self._get_opt_fields())
            return params
        return {}

    def _get_opt_fields(self) -> MutableMapping[str, str]:
        """
        For "GET all" request for almost each stream Asana API by default returns 3 fields for each
        record: `gid`, `name`, `resource_type`. Since we want to get all fields we need to specify those fields in each
        request. For each stream set of fields will be different, and we get those fields from stream's schema.
        Also, each nested object, like `workspace`, or list of nested objects, like `followers`, also by default returns
        those 3 fields mentioned above, so for nested stuff we also need to specify fields we want to return, and we
        decided that for all nested objects and list of objects we will be getting only `gid` field.
        Plus each stream can have its exceptions about how request required fields, like in `Tasks` stream.
        More info can be found here - https://developers.asana.com/docs/input-output-options.
        """

        opt_fields = []
        schema = self._get_stream_schema()

        for prop, value in schema["properties"].items():
            if "object" in value["type"]:
                opt_fields.append(self._handle_object_type(prop, value))
            elif "array" in value["type"]:
                opt_fields.append(self._handle_array_type(prop, value.get("items", [])))
            else:
                opt_fields.append(prop)

        return {"opt_fields": ",".join(opt_fields)} if opt_fields else {}

    def _handle_object_type(self, prop: str, value: MutableMapping[str, Any]) -> str:
        if self.name == "tasks":
            if prop == "custom_fields":
                return prop
            elif prop in ("hearts", "likes"):
                return f"{prop}.user.gid"
            elif prop == "memberships":
                return "memberships.(project|section).gid"

        if self.name == "users" and prop == "photo":
            return prop

        return f"{prop}.gid"

    def _handle_array_type(self, prop: str, value: MutableMapping[str, Any]) -> str:
        if "type" in value and "object" in value["type"]:
            return self._handle_object_type(prop, value)

        return prop

    def _get_stream_schema(self) -> MutableMapping[str, Any]:
        raw_manifest_file = get_data("source_asana", "manifest.yaml")
        if raw_manifest_file:
            manifest = safe_load(raw_manifest_file.decode())
            return manifest.get("definitions", {}).get(f"{self.name}_schema", {})
        return {}
