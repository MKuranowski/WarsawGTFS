FROM python:3.9

WORKDIR /app

COPY data_curated data_curated
COPY static static

COPY requirements.txt requirements.txt
COPY warsawgtfs.py warsawgtfs.py

ENV GTFS_ARGS=""

RUN mkdir "gtfs"

RUN pip3 install -r requirements.txt

CMD python3 warsawgtfs.py $GTFS_ARGS --target "gtfs/warsaw.gtfs.zip"
