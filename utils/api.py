import requests

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


DEFAULT_STATUS_FORCELIST = [
    429,
    500,
    502,
    503,
    504,
]

DEFAULT_ALLOWED_METHODS = [
    "GET",
]


def create_session(
    retries=5,
    backoff_factor=2,
    status_forcelist=None,
    allowed_methods=None,
):
    if status_forcelist is None:
        status_forcelist = DEFAULT_STATUS_FORCELIST

    if allowed_methods is None:
        allowed_methods = DEFAULT_ALLOWED_METHODS

    retry_strategy = Retry(
        total=retries,
        connect=retries,
        read=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=allowed_methods,
    )

    adapter = HTTPAdapter(
        max_retries=retry_strategy
    )

    session = requests.Session()

    session.mount(
        "http://",
        adapter,
    )

    session.mount(
        "https://",
        adapter,
    )

    return session


def get(
    session,
    url,
    params=None,
    headers=None,
    timeout=60,
):
    response = session.get(
        url,
        params=params,
        headers=headers,
        timeout=timeout,
    )

    response.raise_for_status()

    return response


def get_json(
    session,
    url,
    params=None,
    headers=None,
    timeout=60,
):
    response = get(
        session=session,
        url=url,
        params=params,
        headers=headers,
        timeout=timeout,
    )

    return response.json()


def get_text(
    session,
    url,
    params=None,
    headers=None,
    timeout=60,
):
    response = get(
        session=session,
        url=url,
        params=params,
        headers=headers,
        timeout=timeout,
    )

    return response.text