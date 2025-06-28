import multiprocessing
import threading
from multiprocessing.managers import SyncManager

import dateutil
from bs4 import BeautifulSoup

from config import logger, settings
from scraper.request import Request


class Sustainalytics:

    THREAD_COUNT = settings.THREAD_COUNT
    BASE_URL = "https://www.sustainalytics.com/sustapi/companyratings/getcompanyratings"
    FIELDS_MAP = {
        "name": ["div", "row company-name"],
        "sustainalytics_ticker": ["strong", "identifier"],
        "ticker": [],
        "exchange": [],
        "industry": ["strong", "industry-group"],
        "country": ["strong", "country"],
        "desc": ["div", "company-description-text"],
        "ftemployees": ["div", "row company-description-details"],
        "risk_rating": ["div", "col-6 risk-rating-score"],
        "risk_assessment": ["div", "col-6 risk-rating-assessment"],
        "ind_pos": ["strong", "industry-group-position"],
        "ind_pos_total": ["span", "industry-group-positions-total"],
        "uni_pos": ["strong", "universe-position"],
        "uni_pos_total": ["span", "universe-positions-total"],
        "exposure_assessment": ["strong", "company-exposure-assessment"],
        "risk_management_assessment": [
            "strong",
            "company-risk-management-assessment",
        ],
        "last_update": ["div", "row last-update rr-details"],
    }

    def __init__(self):
        logger.info("Initializing Sustainalytics instance")
        self.request = Request().request
        self.urls = []
        self.result = {}
        self.tasks = []

    def run(self):
        logger.info("Sustainalytics run started")
        self.fetch_tickers()
        self.tasks = self.urls.copy()
        logger.info("Fetched %d Tickers", len(self.urls))
        self.start_workers()
        logger.info("All processes and threads have completed")
        return dict(self.result)

    def fetch_tickers(self):
        logger.info("Fetching tickers from %s", self.BASE_URL)
        try:
            page = 1
            while True:
                data = {
                    "page": page,
                    "pageSize": 4000,
                    "resourcePackage": "Sustainalytics",
                }
                resp = self.request("POST", self.BASE_URL, data=data)
                urls = self._helper_extract_urls(resp.text)
                if urls:
                    self.urls.extend(urls)
                    page += 1
                else:
                    break

            logger.info("Successfully fetched tickers")
        except Exception as e:
            logger.error("Failed to fetch tickers: %s", str(e))
            raise

    def start_workers(self):
        manager = self._start_sync_manager()
        self.tasks = manager.list(self.tasks)
        self.result = manager.dict()
        lock = manager.RLock()
        n_proc = multiprocessing.cpu_count()
        logger.info(
            "Starting %d processes × %d threads each", n_proc, self.THREAD_COUNT
        )
        processes = [
            multiprocessing.Process(
                target=self._process_target,
                name=f"Proc-{i}",
                args=(lock,),
            )
            for i in range(n_proc)
        ]

        for p in processes:
            p.start()
            logger.debug("Started %s", p.name)

        for p in processes:
            p.join()
            logger.debug("%s has finished", p.name)

    def _process_target(self, lock):
        local_threads = [
            threading.Thread(
                target=self.worker,
                name=f"{multiprocessing.current_process().name}-T{t}",
                args=(lock,),
                daemon=True,
            )
            for t in range(self.THREAD_COUNT)
        ]

        for t in local_threads:
            t.start()

        for t in local_threads:
            t.join()

    def worker(self, lock):
        thread_name = threading.current_thread().name
        logger.debug("%s: started", thread_name)

        while True:
            with lock:
                if not self.tasks:
                    logger.debug("%s: no more tasks", thread_name)
                    break
                url = self.tasks.pop(0)

            ticker = "-".join(url.split("/")[-2:])
            if ticker in self.result:
                logger.debug("%s: skipping duplicate ticker %s", thread_name, ticker)
                continue

            try:
                logger.debug("%s: fetching ESG scores for %s", thread_name, ticker)
                data = self.fetch_esg_scores(url)
                with lock:
                    self.result[ticker] = data
                logger.debug("%s: fetched data for %s", thread_name, ticker)
            except Exception as e:
                logger.warning(
                    "%s: unable to fetch data for %s: %s", thread_name, ticker, e
                )

    def fetch_esg_scores(self, url):
        logger.debug("Fetching ESG scores from %s", url)
        resp = self.request("GET", url)
        resp.raise_for_status()
        result = self._extract_esg_scores(resp.text)
        return result

    def _extract_esg_scores(self, html):
        result = {}
        soup = BeautifulSoup(html, "html.parser")
        for key, tags in self.FIELDS_MAP.items():
            if not tags:
                result[key] = None
                continue

            x = soup.find(tags[0], class_=tags[1])
            if not x:
                result[key] = None
                continue

            try:
                if key == "last_update":
                    result[key] = dateutil.parser.parse(x.find("strong").text.strip())
                    continue
                if key == "ftemployees":
                    result[key] = x.find("strong").text.strip()
                    continue

                result[key] = x.text.strip()
            except Exception:
                result[key] = None

        return result

    @staticmethod
    def _start_sync_manager():
        m = SyncManager(address=("127.0.0.1", 0), authkey=b"sustainalytics")
        m.start()
        return m

    @staticmethod
    def _helper_extract_urls(html):
        urls = []
        soup = BeautifulSoup(html, "html.parser")
        divs = soup.find_all("div", class_="company-row d-flex")
        for div in divs:
            atag = div.find("a")
            urls.append("https://www.sustainalytics.com/esg-rating" + atag["data-href"])

        return urls
