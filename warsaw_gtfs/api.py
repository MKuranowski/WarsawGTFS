import logging
import os
from collections import defaultdict
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from operator import attrgetter
from pprint import pprint
from shutil import copyfileobj
from tempfile import TemporaryFile
from typing import IO
from urllib.parse import quote_plus as url_quote
from zipfile import ZipFile
from zoneinfo import ZoneInfo

import requests
from impuls.errors import InputNotModified
from impuls.model import Date
from impuls.multi_file import IntermediateFeed, IntermediateFeedProvider, prune_outdated_feeds
from impuls.resource import FETCH_CHUNK_SIZE, ConcreteResource

# TODO: Better secret management (from command line or files)

SECRET_USER = os.getenv("WARSAW_ZTM_USER", "")
SECRET_PASS = os.getenv("WARSAW_ZTM_PASS", "")
SECRET_KEY = os.getenv("WARSAW_ZTM_KEY", "")
TZ = ZoneInfo("Europe/Warsaw")

logger = logging.getLogger("api")


@dataclass
class File:
    name: str
    date: datetime


@dataclass
class FilePairs:
    lookups: list[File] = field(default_factory=list)
    schedules: list[File] = field(default_factory=list)


class ZTMFileProvider(IntermediateFeedProvider["ZTMResource"]):
    def __init__(self, for_day: Date | None = None) -> None:
        self.for_day = for_day or Date.today()

    def needed(self) -> list[IntermediateFeed["ZTMResource"]]:
        all_files = self.get_all_files()
        grouped = self.group_files_by_version(all_files)
        feeds = self.to_latest_feeds(grouped)
        prune_outdated_feeds(feeds, self.for_day)
        return feeds

    def get_all_files(self) -> list[File]:
        with requests.get(
            "https://eta.ztm.waw.pl/timetable1",
            headers={"X-Api-Key": SECRET_KEY},
            auth=(SECRET_USER, SECRET_PASS),
        ) as r:
            r.raise_for_status()
            data = r.json()

        return [
            File(i["name"], datetime.strptime(i["date"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=TZ))
            for i in data
        ]

    def group_files_by_version(
        self,
        files: Iterable[File],
    ) -> defaultdict[str, FilePairs]:
        grouped = defaultdict[str, FilePairs](FilePairs)
        for file in files:
            if file.name.startswith(("Rozklady", "Slowniki")):
                _, version, timestamp = file.name.partition(".")[0].split("_", maxsplit=2)
                fixed_date = datetime.strptime(timestamp, "%Y-%m-%d_%H-%M-%S").replace(tzinfo=TZ)
                fixed_file = File(file.name, fixed_date)
                if file.name[0] == "R":
                    grouped[version].schedules.append(fixed_file)
                else:
                    grouped[version].lookups.append(fixed_file)
        return grouped

    def to_latest_feeds(
        self,
        grouped: Mapping[str, FilePairs],
    ) -> list[IntermediateFeed["ZTMResource"]]:
        return [
            f
            for version, file_pairs in grouped.items()
            if (f := self.to_latest_feed(version, file_pairs))
        ]

    def to_latest_feed(
        self,
        version: str,
        file_pairs: FilePairs,
    ) -> IntermediateFeed["ZTMResource"] | None:
        lookup_file = max(file_pairs.lookups, key=attrgetter("date"), default=None)
        schedules_file = max(file_pairs.schedules, key=attrgetter("date"), default=None)

        if lookup_file is None or schedules_file is None:
            if lookup_file is None:
                logger.warning("Missing lookup tables for %s", version)
            if schedules_file is None:
                logger.warning("Missing schedules for %s", version)
            return None

        if lookup_file.date != schedules_file.date:
            logger.warning(
                "Mismatched upload dates for %s: %s vs. %s",
                version,
                lookup_file,
                schedules_file,
            )

        resource = ZTMResource(version, lookup_file.name, schedules_file.name)
        return IntermediateFeed(resource, version + ".zip", version, Date.from_ymd_str(version))

    def single(self) -> IntermediateFeed["ZTMResource"]:
        all_files = self.get_all_files()
        grouped = self.group_files_by_version(all_files)
        version = self.for_day.isoformat()
        feed = self.to_latest_feed(version, grouped[version])
        assert feed
        feed.resource.update_last_modified()
        return feed


class ZTMResource(ConcreteResource):
    def __init__(
        self,
        version: str,
        lookups_file_name: str,
        schedules_file_name: str,
    ) -> None:
        super().__init__()
        self.logger = logger.getChild(version)
        self.version = version
        self.lookups_file_name = lookups_file_name
        self.schedules_file_name = schedules_file_name

    def __repr__(self) -> str:
        return f"<ZTMResource {self.lookups_file_name}, {self.schedules_file_name}>"

    def update_last_modified(self) -> None:
        self.last_modified = max(
            self.last_modified_from_filename(self.lookups_file_name),
            self.last_modified_from_filename(self.schedules_file_name),
        )

    @staticmethod
    def last_modified_from_filename(name: str) -> datetime:
        _, _, timestamp = name.partition(".")[0].split("_", maxsplit=2)
        return datetime.strptime(timestamp, "%Y-%m-%d_%H-%M-%S").replace(tzinfo=TZ)

    def fetch(self, conditional: bool) -> Iterator[bytes]:
        self.update_last_modified()
        if conditional and self.last_modified < self.fetch_time:
            raise InputNotModified
        self.fetch_time = datetime.now(TZ)

        with TemporaryFile(prefix=f"impuls-warsaw-{self.version}-", suffix=".zip") as f:
            self.logger.debug("creating combined zip")
            self.make_single_zip(f)
            yield from self.yield_zip(f)

    def make_single_zip(self, f: IO[bytes]) -> None:
        with ZipFile(f, mode="w") as arch:
            self.download_lookups(arch)
            self.download_schedules(arch)

    def download_lookups(self, arch: ZipFile) -> None:
        self.download_json_to_zip(arch, lookups=True)

    def download_schedules(self, arch: ZipFile) -> None:
        self.download_json_to_zip(arch, lookups=False)

    def download_json_to_zip(self, dst_arch: ZipFile, lookups: bool) -> None:
        dst_name = "slowniki.json" if lookups else "rozklady.json"
        src_name = self.lookups_file_name if lookups else self.schedules_file_name

        with TemporaryFile(prefix=f"impuls-warsaw-{self.version}-", suffix=f"{dst_name}.zip") as f:
            self.logger.debug("downloading %s", src_name)
            self.download_file(f, src_name)

            self.logger.debug("copying %s", dst_name)
            f.seek(0)
            with ZipFile(f, mode="r") as src_arch:
                if len(src_arch.filelist) != 1:
                    raise ValueError(f"Multiple files in {src_name}")

                with (
                    src_arch.open(src_arch.filelist[0], mode="r") as src,
                    dst_arch.open(dst_name, mode="w") as dst,
                ):
                    copyfileobj(src, dst)

    @staticmethod
    def yield_zip(f: IO[bytes]) -> Iterator[bytes]:
        f.seek(0)
        while chunk := f.read(FETCH_CHUNK_SIZE):
            yield chunk

    @staticmethod
    def download_file(dst: IO[bytes], file: str) -> None:
        with requests.get(
            f"https://eta.ztm.waw.pl/timetable1/{url_quote(file)}",
            headers={"X-Api-Key": SECRET_KEY},
            auth=(SECRET_USER, SECRET_PASS),
            stream=True,
        ) as r:
            r.raise_for_status()
            for chunk in r.iter_content(chunk_size=None, decode_unicode=False):
                dst.write(chunk)


if __name__ == "__main__":
    from pprint import pprint

    pprint(ZTMFileProvider().needed())
