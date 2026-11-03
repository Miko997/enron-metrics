import gzip
import tarfile
from collections.abc import Iterator
from email import policy
from email.errors import MessageError
from email.parser import BytesHeaderParser
from threading import Event
from urllib.request import Request, urlopen

MAX_HEADER_BYTES = 64 * 1024  # cap memory used by headers
PARSER = BytesHeaderParser(policy=policy.default)


class IngestionCancelled(Exception):
    """ingestion stopped during shutdown"""


def parse_sender(headers: bytes) -> str | None:
    """return one valid sender, or none if it can't be used"""
    try:
        from_headers = PARSER.parsebytes(headers).get_all("From", [])
        if len(from_headers) != 1 or from_headers[0].defects:
            return None
        addresses = from_headers[0].addresses
        if len(addresses) != 1:
            return None
        address = addresses[0]
        if not address.username or not address.domain:
            return None
        # only lowercase the domain
        local, domain = address.addr_spec.rsplit("@", 1)
        return f"{local}@{domain.lower()}"
    except (ValueError, IndexError, MessageError, RecursionError):
        return None


def iter_senders(url: str, timeout: float, stop: Event) -> Iterator[str | None]:
    """read senders without extracting the archive"""
    request = Request(url, headers={"User-Agent": "enron-metrics/1.0"})
    with (
        urlopen(request, timeout=timeout) as response,
        gzip.GzipFile(fileobj=response) as data,
        tarfile.open(fileobj=data, mode="r|", stream=True) as archive,
    ):
        for member in archive:
            if stop.is_set():
                raise IngestionCancelled
            if not member.isfile():
                continue
            with archive.extractfile(member) as email_file:
                # read headers, not the message body
                headers = bytearray()
                while len(headers) <= MAX_HEADER_BYTES:
                    if stop.is_set():
                        raise IngestionCancelled
                    line = email_file.readline(MAX_HEADER_BYTES - len(headers) + 1)
                    headers.extend(line)
                    if line in (b"", b"\n", b"\r\n"):
                        break
                if len(headers) > MAX_HEADER_BYTES:
                    yield None
                else:
                    yield parse_sender(bytes(headers))
        # finish reading so gzip checks for a truncated download
        while data.read(64 * 1024):
            if stop.is_set():
                raise IngestionCancelled
        if stop.is_set():
            raise IngestionCancelled
