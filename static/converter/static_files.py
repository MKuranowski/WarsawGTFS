from os.path import join

from ..util import ConversionOpts

"""
Functions generating GTFS files not depending on ZTM data.
"""


def static_agency(target_dir: str) -> None:
    filename = join(target_dir, "agency.txt")
    with open(filename, mode="w", encoding="utf8", newline="\r\n") as f:
        f.write(
            "agency_id,agency_name,agency_url,agency_timezone,agency_lang,agency_phone,"
            "agency_fare_url\n"

            '0,"Warszawski Transport Publiczny","https://www.wtp.waw.pl",Europe/Warsaw,pl,'
            '19 115,"https://www.wtp.waw.pl/ceny-i-rodzaje-biletow/"\n'
        )


def static_feedinfo(target_dir: str, version: str, pub_name: str = "", pub_url: str = "") -> None:
    # Don't create feed_info.txt w/out publisher_name or publisher_url
    if not pub_name or not pub_url:
        return

    # Escape CSV values
    pub_name = '"' + pub_name.replace('"', '""') + '"'
    pub_url = '"' + pub_url.replace('"', '""') + '"'

    filename = join(target_dir, "feed_info.txt")
    with open(filename, mode="w", encoding="utf8", newline="\r\n") as f:
        f.write("feed_publisher_name,feed_publisher_url,feed_lang,feed_version\n")
        f.write(",".join([pub_name, pub_url, "pl", version]) + "\n")


def static_attributions(target_dir: str, shapes: bool, download_time: str) -> None:
    filename = join(target_dir, "attributions.txt")
    with open(filename, mode="w", encoding="utf8", newline="\r\n") as f:

        f.write(
            "organization_name,is_producer,is_operator,is_authority,is_data_source,"
            "attribution_url\n"

            "Mikołaj Kuranowski,1,0,0,1,https://mkuran.pl/gtfs/\n"

            f'"Data provided by: ZTM Warszawa (retrieved {download_time})",0,0,1,1,'
            '"https://www.ztm.waw.pl/pliki-do-pobrania/dane-rozkladowe/"\n'
        )

        if shapes:
            f.write(
                '"Bus shapes based on data by: © OpenStreetMap contributors '
                f'(retrieved {download_time}, under ODbL license)",'
                '0,0,1,1,"https://www.openstreetmap.org/copyright/"\n'
            )


def static_all(target_dir: str, version: str, opts: ConversionOpts) -> None:
    static_agency(target_dir)
    static_feedinfo(target_dir, version, opts.pub_name, opts.pub_url)
    static_attributions(target_dir, opts.shapes, opts.sync_time)
