#!/usr/bin/env python3
import argparse
from pathlib import PurePath
from datetime import timezone, datetime
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
from urllib.parse import urlparse
import boto3
import queue 
import threading
logging.basicConfig(
    format="%(asctime)s | %(name)-12s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)
LOGGER = logging.getLogger("lopatar")
MAX_SIZE = 4 * 1024 * 1024  # Four and a half megs; limit is 6 megs
try:
    import ujson as json
except ImportError:
    import warnings

    warnings.warn("falling back to old json module, this will be slow!")
    import json


class Lopatar:
    def __init__(self, token, api, ts_field=None,alt_ts_field=None,session=None,progresss_bar=False):
        self._token = token
        self._api = api
        self._ts_field=ts_field
        self._alt_ts_field=alt_ts_field
        self._progress_bar=progresss_bar

        if session is None:
            self._session = str(uuid4())
        self.stats=[]

    @backoff.on_exception(
        backoff.expo,
                      requests.exceptions.RequestException,
                      max_time=300,
                      giveup=lambda e:e.response.status_code!=429,
                      on_backoff= lambda details: LOGGER.info("Backoff: {details}")
    )
    def post_events(self,events):
        message = {
            "token": self._token,
            "session": self._session,
            #FIXME - add some extraction of hostname from logs
            "sessionInfo": {"serverHost": platform.node()},
            # "events": events
        }
        # LOGGER.debug(message,len(json.dumps(events)))
        message['events']=events
        LOGGER.debug(f"POST'ing {len(events)} events")
        # with open(r'C:\temp\example.json',"w") as f:
        #      f.write(json.dumps(message))
        #      import sys
        #      sys.exit(-1)
        start_time=datetime.now()
        r=requests.post(self._api,json=message)
        if r.status_code!=200:
            LOGGER.error(r,r.text)            
        r.raise_for_status()
        if r.text.startswith("{"):
            jr=json.loads(r.text)
            if 'warnings' in jr:
                LOGGER.warning(jr)
        end_time=datetime.now()
        time_spent=end_time-start_time
        self.stats.append(
            (start_time,end_time,len(str(message)))
        )        
        LOGGER.debug(f"{r},{r.text},{r.json()} in {time_spent}")
        #FIXME: right now no errors are returned

    def process_events(self,buf):
        errors=[]
        events=[]
        for line_no,event_raw in buf:
            try:
                attrs_raw=json.loads(event_raw)
            except json.JSONDecodeError as e:
                LOGGER.error(f"Unable to parse line {line_no} due to {e}")
                LOGGER.debug(f"{line_no}: `{event_raw}`")
            attrs={}
            for k,v in attrs_raw.items():
                attrs[k.strip().replace(" ","-")]=v
                # if k.startswith(" ") or k.endswith(" "):
                    # del attrs[k]
                    # attrs[k.strip()]=v
                # attrs[k.strip()=attrs[v]
            e = {
                'attrs': attrs
            }
            if self._alt_ts_field is not None:
                # if self._alt_ts_field not in e["attrs""]:
                #     print(e)
                #     sys.exit(-1)
                alt_ts=parse(e["attrs"][self._alt_ts_field])
                alt_ts_field_name=f"ts_{self._alt_ts_field}"
                e["attrs"][alt_ts_field_name]=str(
                        int(alt_ts.timestamp()*1000000000)                                
                        )
                assert len(e["attrs"][alt_ts_field_name])==19, "Badly formatted alt_ts for {line_no}"
            if self._ts_field is not None:
                if self._ts_field in e['attrs']:
                    ts = parse(e[self._ts_field])
                    e["ts"] = str(
                        int(ts.timestamp()*1000000000)                                
                        )
                else:
                    err = f"No ts field for line {line_no}"
                    LOGGER.debug(f"{line_no}: {err}")
                    errors.append(line_no,err)
                    e["ts"] = str(time.time_ns())
            else:
                e["ts"] = str(time.time_ns())
            events.append(e)
            assert len(e["ts"])==19, "Badly formatted ts"
        self.post_events(events)
        return errors


    

    def _worker(self,q,errors,die_event):
        while True:
            if die_event.is_set():
                LOGGER.debug("Received die event, exiting")
                return
            try:
                buf=q.get_nowait()                
            except queue.Empty:
                LOGGER.debug(f"{threading.get_ident()}: queue empty, sleeping")
                #Skip a beat if queue is empty
                time.sleep(1)
                continue
            try:
                err=self.process_events(buf)
                errors.append(err)            
            except Exception as e:
                LOGGER.error(f"Exception in {threading.get_ident()}: {e}")
                raise
            finally:
                q.task_done()
            

    def upload_file_threads(self,src,thread_count=10):
        
        q = queue.Queue(maxsize=2*thread_count)
        die_event = threading.Event()       

        errors = []

        if self._progress_bar:
            from tqdm.auto import tqdm
            pbar=tqdm(total=self._get_file_size(src),unit='B',unit_scale=True)
        try:
            threads = [threading.Thread(target=self._worker, args=[q,errors,die_event], daemon=True)
            for _ in range(thread_count)]
            for t in threads:
                t.start()

            for buf in self.read_chunks(src):                        
                buf_len=sum(len(x[1]) for x in buf)
                q.put(buf)
                if self._progress_bar:
                    pbar.update(buf_len)
            while not q.empty():
                time.sleep(1)
            die_event.set() # signal workers to quit
            for t in threads:  # wait until workers exit
                t.join()        
        except KeyboardInterrupt:
                    print("Ctrl+C pressed...")
                    die_event.set()  # notify the child thread to exit
                    # sys.exit(1)
        return errors


    def upload_file(self, src):        
        if self._progress_bar:
            pbar=tqdm(total=self._get_file_size(src),unit='B',unit_scale=True)
        errors = []
        for buf in self.read_chunks(src):                        
            buf_len=sum(len(x[1]) for x in buf)
            err=self.process_events(buf)
            errors.append(err)
            pbar.update(buf_len)
        return errors
    
    def read_chunks(self, src):
        fp = open(src, encoding="utf-8", errors="ignore")
        buf = []
        buf_len = 0
        line_no = 0
        while True:
            line_no += 1
            line = fp.readline()            
            if buf_len + len(line) >= MAX_SIZE or line == "":
                # LOGGER.debug(f"Buflen in {len(buf)}")
                yield buf
                buf = []
                buf_len = 0                
                # We've reached the end of file
                if line == "": 
                    break
            else:
                buf.append((line_no,line))
                buf_len += len(line)

    def _get_file_size(self,filepath)->int:
        if filepath.startswith("s3://"):
            session=boto3.Session()
            p=urlparse(filepath)                    
            total=session.resource('s3').Object(p.netloc,p.path[1:]).content_length
        else:
            total=os.path.getsize(filepath)
        return total


def main():
    parser = argparse.ArgumentParser(description="Shovel data into dataset/scalyr")
    parser.add_argument("--token", metavar="DATASET_TOKEN", type=str,
    help="Dataset/Scalyr API token")
    parser.add_argument("--ts-field", type=str, help="Timestamp field name, will be converted to \"ts\"")
    parser.add_argument("--alt-ts-field", type=str,help="Field name which will additionally be converted to epoch time (not to be confused with ts field)")
    parser.add_argument("--debug", action="store_true",default=False,
    help="Enable debugging mode")
    parser.add_argument("source",metavar="FILE",
    help="File path of s3 URI for file to import")
    parser.add_argument(
        "--api", type=str, default="https://app.scalyr.com/api/addEvents",
        help='Scalyr/DataSet API endpoint'
    )
    parser.add_argument('--threads','-t',default=1,type=int,
        help="Set the number of worker threads")
    
    parser.add_argument("--plot",action="store_true",default=False,
    help="Plot a nice performance chart!")
    parser.add_argument("--progress-bar",action="store_true",default=False,
    help="Display a progress bar")
    args = parser.parse_args()
    if args.debug:
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

    
    l_start_time=datetime.now()
    l=Lopatar(token,args.api,args.ts_field,args.alt_ts_field,progresss_bar=args.progress_bar)
    if args.threads>1:
        l.upload_file_threads(args.source,args.threads)
    else:
        l.upload_file(args.source)
    ticks=[
        (x[1]-l_start_time).seconds for x in l.stats
    ]
    values=[
        (x[1]-x[0]).seconds for x in l.stats
    ]
    if args.plot:
        # print(list(zip(ticks,values)))
        import plotext as plt
        plt.plot(ticks,values)
        plt.title(f"Started at {l_start_time}")
        plt.xlabel("Time in seconds since start")
        plt.ylabel("Time in seconds to post")
        plt.show()
    

if __name__ == "__main__":
    main()
