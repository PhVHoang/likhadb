"""Thin httpx wrappers that map HTTP status codes to SDK exceptions."""

from __future__ import annotations

from typing import Any

import httpx

from .exceptions import (
    LikhaDBBadRequestError,
    LikhaDBConflictError,
    LikhaDBConnectionError,
    LikhaDBNotFoundError,
    LikhaDBServerError,
)


def _raise_for_status(response: httpx.Response) -> None:
    if response.is_success:
        return
    try:
        detail = response.json().get("error", response.text)
    except Exception:
        detail = response.text
    status = response.status_code
    if status == 400:
        raise LikhaDBBadRequestError(detail)
    if status == 404:
        raise LikhaDBNotFoundError(detail)
    if status == 409:
        raise LikhaDBConflictError(detail)
    if status >= 500:
        raise LikhaDBServerError(f"[{status}] {detail}")
    response.raise_for_status()


class HttpClient:
    def __init__(self, base_url: str, timeout: float) -> None:
        self._client = httpx.Client(base_url=base_url, timeout=timeout)

    def get(self, path: str) -> httpx.Response:
        try:
            r = self._client.get(path)
        except httpx.ConnectError as exc:
            raise LikhaDBConnectionError(str(exc)) from exc
        _raise_for_status(r)
        return r

    def post(self, path: str, json: Any | None = None) -> httpx.Response:
        try:
            r = self._client.post(path, json=json)
        except httpx.ConnectError as exc:
            raise LikhaDBConnectionError(str(exc)) from exc
        _raise_for_status(r)
        return r

    def delete(self, path: str) -> httpx.Response:
        try:
            r = self._client.delete(path)
        except httpx.ConnectError as exc:
            raise LikhaDBConnectionError(str(exc)) from exc
        _raise_for_status(r)
        return r

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> HttpClient:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


class AsyncHttpClient:
    def __init__(self, base_url: str, timeout: float) -> None:
        self._client = httpx.AsyncClient(base_url=base_url, timeout=timeout)

    async def get(self, path: str) -> httpx.Response:
        try:
            r = await self._client.get(path)
        except httpx.ConnectError as exc:
            raise LikhaDBConnectionError(str(exc)) from exc
        _raise_for_status(r)
        return r

    async def post(self, path: str, json: Any | None = None) -> httpx.Response:
        try:
            r = await self._client.post(path, json=json)
        except httpx.ConnectError as exc:
            raise LikhaDBConnectionError(str(exc)) from exc
        _raise_for_status(r)
        return r

    async def delete(self, path: str) -> httpx.Response:
        try:
            r = await self._client.delete(path)
        except httpx.ConnectError as exc:
            raise LikhaDBConnectionError(str(exc)) from exc
        _raise_for_status(r)
        return r

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> AsyncHttpClient:
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.aclose()
