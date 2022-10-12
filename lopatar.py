#!/usr/bin/env python3
import argparse
from pathlib import PurePath
from datetime import timezone
from time import time
import os
import logging
import platform
from uuid import uuid4
import backoff
from smart_open import open
from dateutil.parser import parse
import requests
import time
logging.basicConfig(
    format="%(asctime)s | %(name)-12s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)
LOGGER = logging.getLogger("lopatar")
MAX_SIZE = 5 * 1024 * 1024  # Five and a half megs; limit is 6 megs
try:
    import ujson as json
except ImportError:
    import warnings

    warnings.warn("falling back to old json module, this will be slow!")
    import json


class Lopatar:
    def __init__(self, token, api, ts_field,session=None):
        self._token = token
        self._api = api
        self._ts_field=ts_field
        if session is None:
            self._session = str(uuid4())

    @backoff.on_exception(
        backoff.expo,
                      requests.exceptions.RequestException,
                      max_time=300,
                      giveup=lambda e:e.response.status_code!=429
    )
    def post_events(self,events):
        message = {
            "token": self._token,
            "session": self._session,
            #FIXME - add some extraction of hostname from logs
            "sessioninfo": {"serverHost": platform.node()},
            # "events": events
        }
        # LOGGER.debug(message,len(json.dumps(events)))
        message['events']=events
        LOGGER.debug(f"POST'ing {len(events)} events")
        with open(r'C:\temp\example.json',"w") as f:
            f.write(json.dumps(message))
            import sys
            sys.exit(-1)
        r=requests.post(self._api,json=message)
        if r.status_code!=200:
            LOGGER.error(r,r.text)            
        r.raise_for_status()
        if r.text.startswith("{"):
            jr=json.loads(r.text)
            if 'warnings' in jr:
                LOGGER.warning(jr)
        LOGGER.debug(f"{r},{r.text},{r.json()}")

    def upload_file(self, src):

        fp = open(src, encoding="utf-8", errors="ignore")
        errors = []
        buf = []
        buf_len = 0
        line_no = 0
        while True:
            line_no += 1
            line = fp.readline()            
            if buf_len + len(line) >= MAX_SIZE or line == "":
                events=[]
                for event_raw in buf:
                    e = {
                        'attrs': json.loads(event_raw)
                    }
                    if self._ts_field is not None:
                        if self._ts_field in e['attrs']:
                            ts = parse(e[self._ts_field])
                            e["ts"] = str(
                                int(ts.timestamp()*1000000000)                                
                                )
                        else:
                            err = f"No ts field for line {i}"
                            LOGGER.debug(err)
                            errors = append(err)
                            e["ts"] = str(time.time_ns())
                    else:
                        e["ts"] = str(time.time_ns())
                    events.append(e)
                self.post_events(events)
                buf = []
                buf_len = 0
            else:
                buf.append(line)
                buf_len += len(line)

def main():
    parser = argparse.ArgumentParser(description="Shovel data into dataset")
    parser.add_argument("--token", metavar="DATASET_TOKEN", type=str)
    parser.add_argument("--ts-field", type=str)
    parser.add_argument("--local", action="store_true", default=False)
    parser.add_argument("source", nargs="?")
    parser.add_argument(
        "--api", type=str, default="https://app.scalyr.com/api/addEvents"
    )
    args = parser.parse_args()
    loglevel = getattr(logging, "DEBUG")
    LOGGER.setLevel(loglevel)
    

    if args.token:
        token = args.token
    else:
        token = os.getenv("DATASET_TOKEN", None)
        if token is None:
            print("Missing DATASET TOKEN")
            parser.print_usage()
            return
    LOGGER.info(
        f"Welcome to the lopatar, operating in {['aws','local'][int(args.local)]} mode"
    )

    if args.local:
        l=Lopatar(token,args.api,args.ts_field)
        l.upload_file(args.source)
    else:
        raise NotImplementedError


if __name__ == "__main__":
    main()