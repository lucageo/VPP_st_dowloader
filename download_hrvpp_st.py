#!/usr/bin/env python3
"""Standalone downloader for CLMS HR-VPP Seasonal Trajectories (ST).
"""

from __future__ import annotations

import argparse
import getpass
import json
import os
import re
import shutil
import sys
import threading
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, datetime, time as datetime_time, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Iterable

import requests
from pyproj import Transformer


DATASET_ID = "EO:EEA:DAT:CLMS_HRVPP_ST-LAEA"
PLATFORM = "S2A, S2B"
RESOLUTION = "10"
DEFAULT_PRODUCTS = ("PPI", "QFLAG")
PRODUCT_RE = re.compile(
    r"^ST_(?P<date>\d{8})T(?P<time>\d{6})_S2_"
    r"(?P<tile>[EW]\d{2}[NS]\d{2})-(?P<epsg>\d{5})-(?P<resolution>\d{3}m)_"
    r"V(?P<version>\d{3})_(?P<kind>PPI|QFLAG)(?:\.(?:tif|tiff|zip))?$",
    re.IGNORECASE,
)
TILE_RE = re.compile(r"^(?P<ew>[EW])(?P<x>\d{2})(?P<ns>[NS])(?P<y>\d{2})$", re.IGNORECASE)


@dataclass(frozen=True)
class Product:
    product_id: str
    tile: str
    when: datetime
    kind: str
    size: int | None
    location: str
    match: Any = field(repr=False, compare=False)

    def manifest_record(self) -> dict[str, Any]:
        return {
            "product_id": self.product_id,
            "tile": self.tile,
            "date": self.when.date().isoformat(),
            "product_type": self.kind,
            "size_bytes": self.size,
            "location": self.location,
        }


class PersistentHourlyLimiter:
    """Limit initiated HDA orders using a rolling, restart-safe ledger."""

    def __init__(self, maximum: int, ledger: Path) -> None:
        if maximum < 0:
            raise ValueError("downloads-per-hour cannot be negative")
        self.maximum = maximum
        self.ledger = ledger
        self.lock = threading.Lock()

    def _read(self) -> list[float]:
        if not self.ledger.is_file():
            return []
        try:
            payload = json.loads(self.ledger.read_text(encoding="utf-8"))
            return [float(value) for value in payload.get("request_timestamps", [])]
        except (OSError, TypeError, ValueError, json.JSONDecodeError):
            return []

    def _write(self, timestamps: list[float]) -> None:
        payload = {
            "updated_utc": datetime.now(timezone.utc).isoformat(),
            "maximum_downloads_per_hour": self.maximum,
            "request_timestamps": timestamps,
        }
        temporary = self.ledger.with_suffix(self.ledger.suffix + ".tmp")
        temporary.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        temporary.replace(self.ledger)

    def wait(self) -> None:
        if self.maximum == 0:
            return
        while True:
            with self.lock:
                now = time.time()
                timestamps = [value for value in self._read() if now - value < 3600]
                if len(timestamps) < self.maximum:
                    timestamps.append(now)
                    self._write(timestamps)
                    return
                seconds = max(1.0, 3601 - (now - min(timestamps)))
            print(
                f"HDA hourly allowance reached ({len(timestamps)}/{self.maximum}); "
                f"waiting {seconds / 60:.1f} minutes",
                flush=True,
            )
            time.sleep(seconds)


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("dates must use YYYY-MM-DD") from exc


def normalise_tile(value: str) -> str:
    tile = value.strip().upper()
    if TILE_RE.fullmatch(tile) is None:
        raise argparse.ArgumentTypeError(f"invalid CLMS tile code: {value!r}")
    return tile


def tile_bbox_wgs84(tile: str) -> tuple[float, float, float, float]:
    """Convert a 100 km CLMS LAEA tile code to a densified WGS84 bbox."""
    match = TILE_RE.fullmatch(normalise_tile(tile))
    assert match is not None
    x = int(match.group("x")) * 100_000
    y = int(match.group("y")) * 100_000
    if match.group("ew").upper() == "W":
        x = -x
    if match.group("ns").upper() == "S":
        y = -y
    transformer = Transformer.from_crs("EPSG:3035", "EPSG:4326", always_xy=True)
    bounds = transformer.transform_bounds(x, y, x + 100_000, y + 100_000, densify_pts=21)
    return tuple(round(float(value), 8) for value in bounds)


def iso_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def build_query(
    *,
    product_type: str,
    bbox: tuple[float, float, float, float],
    start: datetime,
    end: datetime,
    tile: str | None = None,
) -> dict[str, Any]:
    query: dict[str, Any] = {
        "dataset_id": DATASET_ID,
        "productType": product_type,
        "platformSerialIdentifier": PLATFORM,
        "resolution": RESOLUTION,
        "bbox": list(bbox),
        "startdate": iso_z(start),
        "enddate": iso_z(end),
        "itemsPerPage": 200,
        "startIndex": 0,
    }
    if tile is not None:
        query["tileId"] = normalise_tile(tile)
    return query


def parse_product(match: Any) -> Product:
    results = getattr(match, "results", None)
    if not isinstance(results, list) or len(results) != 1:
        raise RuntimeError("unexpected WEkEO search-result structure")
    feature = results[0]
    raw_id = str(feature.get("id", ""))
    parsed = PRODUCT_RE.fullmatch(raw_id)
    if parsed is None:
        raise RuntimeError(f"unexpected HR-VPP ST product identifier: {raw_id!r}")
    product_id = re.sub(r"\.(?:tif|tiff|zip)$", "", raw_id, flags=re.IGNORECASE)
    properties = feature.get("properties") or {}
    raw_size = properties.get("size")
    size = raw_size if isinstance(raw_size, int) else None
    return Product(
        product_id=product_id,
        tile=parsed.group("tile").upper(),
        when=datetime.strptime(parsed.group("date"), "%Y%m%d").replace(tzinfo=timezone.utc),
        kind=parsed.group("kind").upper(),
        size=size,
        location=str(properties.get("location", "")),
        match=match,
    )


def search(client: Any, queries: Iterable[dict[str, Any]]) -> tuple[list[Product], list[dict[str, Any]]]:
    products: dict[str, Product] = {}
    recorded_queries: list[dict[str, Any]] = []
    for query in queries:
        label = query.get("tileId", "bbox")
        print(f"Searching {query['productType']} for {label} ...", flush=True)
        matches = client.search(query)
        recorded_queries.append(query)
        for index in range(len(matches)):
            product = parse_product(matches[index])
            products[product.product_id] = product
        print(f"Found {len(matches)} result(s) for this query", flush=True)
    ordered = sorted(products.values(), key=lambda item: (item.when, item.tile, item.kind, item.product_id))
    return ordered, recorded_queries


def download_url(product: Product) -> tuple[str, str]:
    urls = product.match.get_download_urls(limit=1)
    if len(urls) != 1:
        raise RuntimeError(f"WEkEO did not return one download URL for {product.product_id}")
    client = product.match.client
    token = str(getattr(client, "token", ""))
    if not token:
        raise RuntimeError("WEkEO did not provide an access token")
    return urls[0], token


def transfer_product(product: Product, destination: Path, cache: Path, limiter: PersistentHourlyLimiter) -> Path:
    final_path = destination / f"{product.product_id}.tif"
    if final_path.is_file() and final_path.stat().st_size > 0:
        return final_path

    cache.mkdir(parents=True, exist_ok=True)
    payload = cache / f"{product.product_id}.download"
    partial = payload.with_suffix(payload.suffix + ".part")
    attempts = 8
    expected_total: int | None = None

    for attempt in range(1, attempts + 1):
        limiter.wait()
        try:
            url, token = download_url(product)
            offset = partial.stat().st_size if partial.is_file() else 0
            headers = {"Authorization": f"Bearer {token}"}
            if offset:
                headers["Range"] = f"bytes={offset}-"
            with requests.get(url, headers=headers, stream=True, timeout=(30, 600)) as response:
                if response.status_code == 416 and offset:
                    partial.unlink(missing_ok=True)
                    raise RuntimeError("remote server rejected the retained byte range")
                response.raise_for_status()
                append = offset > 0 and response.status_code == 206
                if not append:
                    offset = 0

                content_range = response.headers.get("Content-Range", "")
                range_match = re.fullmatch(r"bytes (\d+)-(\d+)/(\d+|\*)", content_range)
                if append and (range_match is None or int(range_match.group(1)) != offset):
                    partial.unlink(missing_ok=True)
                    raise RuntimeError(f"unexpected Content-Range: {content_range!r}")
                if range_match is not None and range_match.group(3) != "*":
                    expected_total = int(range_match.group(3))
                else:
                    length = response.headers.get("Content-Length")
                    expected_total = offset + int(length) if length and length.isdigit() else None

                with partial.open("ab" if append else "wb") as output:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            output.write(chunk)

            if not partial.is_file() or partial.stat().st_size == 0:
                raise RuntimeError("received an empty file")
            if expected_total is not None and partial.stat().st_size != expected_total:
                raise RuntimeError(
                    f"incomplete response: {partial.stat().st_size} of {expected_total} bytes"
                )
            partial.replace(payload)
            break
        except Exception as exc:
            if attempt == attempts:
                raise RuntimeError(f"failed after {attempts} attempts: {product.product_id}") from exc
            retained = partial.stat().st_size if partial.is_file() else 0
            delay = min(60, 5 * (2 ** (attempt - 1)))
            print(
                f"Retry {attempt}/{attempts} for {product.product_id}; "
                f"retained {retained} bytes; waiting {delay}s: {exc}",
                flush=True,
            )
            time.sleep(delay)

    final_partial = final_path.with_suffix(final_path.suffix + ".part")
    if zipfile.is_zipfile(payload):
        with zipfile.ZipFile(payload) as archive:
            expected_name = final_path.name.lower()
            members = [
                member
                for member in archive.infolist()
                if PurePosixPath(member.filename).name.lower() == expected_name
            ]
            if len(members) != 1:
                raise RuntimeError(
                    f"archive for {product.product_id} contains {len(members)} matching GeoTIFFs"
                )
            with archive.open(members[0]) as source, final_partial.open("wb") as output:
                shutil.copyfileobj(source, output)
    else:
        shutil.move(payload, final_partial)
    if not final_partial.is_file() or final_partial.stat().st_size == 0:
        raise RuntimeError(f"materialised file is empty: {final_partial}")
    final_partial.replace(final_path)
    payload.unlink(missing_ok=True)
    return final_path


def write_manifest(
    destination: Path,
    *,
    start: date,
    end: date,
    queries: list[dict[str, Any]],
    products: list[Product],
    downloaded: Iterable[Path],
    status: str,
) -> Path:
    manifest = {
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "dataset_id": DATASET_ID,
        "date_range": {"start": start.isoformat(), "end": end.isoformat()},
        "status": status,
        "queries": queries,
        "selected_count": len(products),
        "products": [item.manifest_record() for item in products],
        "downloaded_files": [str(path.resolve()) for path in sorted(downloaded)],
    }
    path = destination / "download_manifest.json"
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)
    return path


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Download CLMS HR-VPP ST PPI and/or QFLAG GeoTIFFs from WEkEO."
    )
    result.add_argument("--username", help="WEkEO username; prompted if omitted")
    result.add_argument("--destination", required=True, type=Path, help="Output folder")
    result.add_argument("--start-date", required=True, type=parse_date, help="Inclusive YYYY-MM-DD")
    result.add_argument("--end-date", required=True, type=parse_date, help="Inclusive YYYY-MM-DD")
    area = result.add_mutually_exclusive_group(required=True)
    area.add_argument("--tiles", nargs="+", type=normalise_tile, help="CLMS tile code(s), e.g. E44N24")
    area.add_argument(
        "--bbox",
        nargs=4,
        type=float,
        metavar=("WEST", "SOUTH", "EAST", "NORTH"),
        help="WGS84 bounding box",
    )
    result.add_argument(
        "--products",
        nargs="+",
        choices=DEFAULT_PRODUCTS,
        default=list(DEFAULT_PRODUCTS),
        help="ST product types (default: PPI QFLAG)",
    )
    result.add_argument("--workers", type=int, default=4, help="Parallel downloads, 1-16 (default: 4)")
    result.add_argument(
        "--downloads-per-hour",
        type=int,
        default=500,
        help="HDA rolling allowance; 0 disables local limiting (default: 500)",
    )
    result.add_argument("--dry-run", action="store_true", help="Search and write a manifest without downloading")
    return result


def main() -> None:
    args = parser().parse_args()
    if args.start_date > args.end_date:
        raise SystemExit("--start-date cannot be later than --end-date")
    if not 1 <= args.workers <= 16:
        raise SystemExit("--workers must be between 1 and 16")
    if args.downloads_per_hour < 0:
        raise SystemExit("--downloads-per-hour cannot be negative")
    if args.bbox is not None:
        west, south, east, north = args.bbox
        if not (-180 <= west < east <= 180 and -90 <= south < north <= 90):
            raise SystemExit("invalid WGS84 --bbox; expected WEST SOUTH EAST NORTH")

    username = (args.username or input("WEkEO username: ")).strip()
    if not username:
        raise SystemExit("WEkEO username cannot be empty")
    password = getpass.getpass("WEkEO password (hidden): ")
    if not password:
        raise SystemExit("WEkEO password cannot be empty")

    try:
        from hda import Client, Configuration
    except ImportError as exc:
        raise SystemExit("Missing dependency 'hda'. Install requirements.txt first.") from exc

    destination = args.destination.expanduser().resolve()
    destination.mkdir(parents=True, exist_ok=True)
    start = datetime.combine(args.start_date, datetime_time.min, tzinfo=timezone.utc)
    end = datetime.combine(args.end_date, datetime_time.max, tzinfo=timezone.utc)

    queries: list[dict[str, Any]] = []
    if args.tiles:
        for tile in args.tiles:
            bbox = tile_bbox_wgs84(tile)
            for product_type in args.products:
                queries.append(
                    build_query(
                        product_type=product_type,
                        bbox=bbox,
                        start=start,
                        end=end,
                        tile=tile,
                    )
                )
    else:
        bbox = tuple(args.bbox)
        for product_type in args.products:
            queries.append(
                build_query(product_type=product_type, bbox=bbox, start=start, end=end)
            )

    client = Client(
        config=Configuration(user=username, password=password, path=os.devnull),
        retry_max=5,
        sleep_max=60,
        progress=False,
        max_workers=1,
    )
    products, recorded_queries = search(client, queries)
    print(f"Selected {len(products)} unique file(s)", flush=True)
    if not products:
        write_manifest(
            destination,
            start=args.start_date,
            end=args.end_date,
            queries=recorded_queries,
            products=products,
            downloaded=[],
            status="no-results",
        )
        raise SystemExit("No HR-VPP ST products matched the request")

    if args.dry_run:
        manifest = write_manifest(
            destination,
            start=args.start_date,
            end=args.end_date,
            queries=recorded_queries,
            products=products,
            downloaded=[],
            status="dry-run",
        )
        print(f"Dry-run manifest: {manifest}")
        return

    limiter = PersistentHourlyLimiter(
        args.downloads_per_hour,
        destination / ".hda_request_ledger.json",
    )
    cache = destination / ".download-cache"
    completed: list[Path] = []
    failures: list[tuple[Product, BaseException]] = []

    existing = [destination / f"{item.product_id}.tif" for item in products]
    existing = [path for path in existing if path.is_file() and path.stat().st_size > 0]
    completed.extend(existing)
    pending = [item for item in products if destination / f"{item.product_id}.tif" not in existing]
    print(f"Downloaded {len(completed)}/{len(products)} files", flush=True)

    with ThreadPoolExecutor(max_workers=min(args.workers, len(pending) or 1)) as executor:
        futures = {
            executor.submit(transfer_product, item, destination, cache, limiter): item
            for item in pending
        }
        for future in as_completed(futures):
            item = futures[future]
            try:
                completed.append(future.result())
                print(f"Downloaded {len(completed)}/{len(products)} files", flush=True)
            except BaseException as exc:
                failures.append((item, exc))
                print(f"FAILED {item.product_id}: {exc}", file=sys.stderr, flush=True)

    status = "complete" if not failures else "incomplete"
    manifest = write_manifest(
        destination,
        start=args.start_date,
        end=args.end_date,
        queries=recorded_queries,
        products=products,
        downloaded=completed,
        status=status,
    )
    print(f"Manifest: {manifest}")
    if failures:
        print(
            f"{len(failures)} file(s) failed. Partial downloads were retained; rerun the same command to resume.",
            file=sys.stderr,
        )
        raise SystemExit(1)
    print(f"Complete: {len(completed)} file(s) in {destination}")


if __name__ == "__main__":
    main()
