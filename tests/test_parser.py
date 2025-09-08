import json
import logging
import os
import sys
import time
from types import SimpleNamespace

import pytest
from bs4 import BeautifulSoup

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import parser
from parser import CarsParser


@pytest.fixture
def parser_instance():
    return CarsParser([], "https://example.com", 1, min_delay=0, max_delay=0)


def test_load_proxies_parses_file(tmp_path):
    proxy_file = tmp_path / "proxies.txt"
    proxy_file.write_text("host1:80:user1:pass1\nhost2:81:user2:pass2\n\n")

    proxies = parser.load_proxies(str(proxy_file))

    assert proxies == [
        {"host": "host1", "port": "80", "user": "user1", "password": "pass1"},
        {"host": "host2", "port": "81", "user": "user2", "password": "pass2"},
    ]


def test_main_uses_load_proxies(tmp_path, monkeypatch):
    proxy_file = tmp_path / "proxies.txt"
    proxy_file.write_text("host1:80:user1:pass1\n")

    monkeypatch.setenv("PROXY_FILE", str(proxy_file))
    monkeypatch.setenv("DEFAULT_URL", "https://example.com")
    monkeypatch.setenv("PROCESSES", "1")

    captured = {}

    class DummyParser:
        def __init__(self, proxies, default_url, processes):
            captured["proxies"] = proxies

        def run(self):
            captured["ran"] = True

    monkeypatch.setattr(parser, "CarsParser", DummyParser)

    parser.main()

    assert captured["proxies"] == [
        {"host": "host1", "port": "80", "user": "user1", "password": "pass1"}
    ]
    assert captured.get("ran")


def test_get_random_proxies_and_headers_without_proxies(monkeypatch):
    parser_instance = CarsParser([], "https://example.com", 1, min_delay=0, max_delay=0)

    class DummyUA:
        @property
        def random(self):
            return "agent"

    monkeypatch.setattr(parser, "UserAgent", lambda: DummyUA())

    proxies, headers = parser_instance.get_random_proxies_and_headers()

    assert proxies == {}
    assert headers == {
        "User-Agent": "agent",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://example.com",
        "Connection": "keep-alive",
    }


def test_parse_basics_block_logs_url(parser_instance, caplog):
    soup = BeautifulSoup("<html></html>", "html.parser")
    url = "https://cars.com/vehicledetail/123"
    with caplog.at_level(logging.INFO):
        result = parser_instance.parse_basics_block(soup, url)
    assert result == {}
    assert f"Basics block not found for {url}" in caplog.text


def test_parse_features_block_logs_url(parser_instance, caplog):
    soup = BeautifulSoup("<html></html>", "html.parser")
    url = "https://cars.com/vehicledetail/456"
    with caplog.at_level(logging.INFO):
        result = parser_instance.parse_features_block(soup, url)
    assert result == {}
    assert f"Features block not found for {url}" in caplog.text


def test_parse_sellers_info_block_logs_url(parser_instance, caplog):
    soup = BeautifulSoup("<html></html>", "html.parser")
    url = "https://cars.com/vehicledetail/789"
    with caplog.at_level(logging.INFO):
        result = parser_instance.parse_sellers_info_block(soup, url)
    assert result == ""
    assert f"Seller info block not found for {url}" in caplog.text


def test_get_params_refreshes_stale_cache(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    cache_file = tmp_path / "car_params.json"
    cache_file.write_text(json.dumps({"car_stock_types": ["stale"], "car_makes": ["Old"]}))
    old_time = time.time() - 90000
    os.utime(cache_file, (old_time, old_time))

    parser_instance = CarsParser([], "https://example.com", 1, min_delay=0, max_delay=0)
    monkeypatch.setattr(parser.CarsParser, "get_random_proxies_and_headers", lambda self: ({}, {}))

    html = (
        '<div id="search-basics-area">'
        '<select data-activitykey="make_select">'
        '<optgroup>'
        '<option value="BrandA"></option>'
        '<option value="BrandB"></option>'
        '</optgroup>'
        '</select>'
        '</div>'
    )

    calls = []

    def fake_get(url, headers=None, proxies=None):
        calls.append(url)
        return SimpleNamespace(status_code=200, content=html.encode("utf-8"))

    monkeypatch.setattr(parser.requests, "get", fake_get)

    stock_types, makes = parser_instance.get_params()

    assert calls, "Expected network call for stale cache"
    assert stock_types == ["used"]
    assert makes == ["BrandA", "BrandB"]

    with open("car_params.json", "r") as f:
        saved = json.load(f)
    assert saved["car_makes"] == ["BrandA", "BrandB"]


def test_get_all_car_models_refreshes_stale_cache(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    cache_file = tmp_path / "car_models.json"
    cache_file.write_text(json.dumps({"old": {"Brand": ["X"]}}))
    old_time = time.time() - 90000
    os.utime(cache_file, (old_time, old_time))

    parser_instance = CarsParser([], "https://example.com", 1, min_delay=0, max_delay=0)

    calls = []

    def fake_get_models(self, stock_type, car_make):
        calls.append((stock_type, car_make))
        return ["Model1"]

    class DummyPool:
        def __init__(self, processes):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            pass

        def starmap(self, func, args):
            return [func(*a) for a in args]

    monkeypatch.setattr(parser, "Pool", DummyPool)
    monkeypatch.setattr(parser, "tqdm", lambda x, **kwargs: x)
    monkeypatch.setattr(parser.CarsParser, "get_models", fake_get_models)

    result = parser_instance.get_all_car_models(["used"], ["BrandA"])

    assert calls == [("used", "BrandA")]
    assert result == {"used": {"BrandA": ["Model1"]}}

    with open("car_models.json", "r") as f:
        saved = json.load(f)
    assert saved == {"used": {"BrandA": ["Model1"]}}

    with open("car_models_filter.json", "r") as f:
        filters = json.load(f)
    assert filters == {
        "used": {"BrandA": {"_enabled": True, "models": {"Model1": True}}}
    }


def test_parse_data_respects_filter(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    parser_instance = CarsParser([], "https://example.com", 1, min_delay=0, max_delay=0)

    car_stock_types = ["used"]
    car_makes = ["BrandA", "BrandB"]
    car_models = {"used": {"BrandA": ["Model1", "Model2"], "BrandB": ["ModelX"]}}

    filter_data = {
        "used": {
            "BrandA": {"_enabled": True, "models": {"Model1": True, "Model2": False}},
            "BrandB": {"_enabled": False, "models": {"ModelX": True}},
        }
    }
    with open("car_models_filter.json", "w") as f:
        json.dump(filter_data, f)

    calls = []

    def fake_get_pages(self, stock_type, car_make, car_model):
        calls.append((stock_type, car_make, car_model))
        return 1

    def fake_page_hrefs(self, stock_type, car_make, car_model, page):
        return [], 0, 0

    class DummyPool:
        def __init__(self, processes):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            pass

        def imap(self, func, iterable):
            return []

    monkeypatch.setattr(parser, "Pool", DummyPool)
    monkeypatch.setattr(parser, "tqdm", lambda x, **kwargs: x)
    monkeypatch.setattr(CarsParser, "get_pages_num", fake_get_pages)
    monkeypatch.setattr(CarsParser, "get_vehicle_page_hrefs", fake_page_hrefs)
    monkeypatch.setattr(CarsParser, "get_vehicle_info", lambda self, href, brand_words_num=1: None)
    monkeypatch.setattr(CarsParser, "get_all_vehicle_ids", lambda self: [])

    parser_instance.parse_data(car_stock_types, car_makes, car_models)

    assert calls == [("used", "BrandA", "Model1")]


def test_get_proxies_user_agents_filters_and_persists(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    proxy_file = tmp_path / "proxies.txt"
    proxy_file.write_text("host1:80:user1:pass1\nhost2:81:user2:pass2\n")

    monkeypatch.setenv("PROXY_FILE", str(proxy_file))

    proxies = parser.load_proxies(str(proxy_file))
    parser_instance = CarsParser(proxies, "https://example.com", 1, min_delay=0, max_delay=0)

    class DummyUA:
        @property
        def chrome(self):
            return "agent"

    monkeypatch.setattr(parser, "UserAgent", lambda: DummyUA())

    def fake_get(url, headers=None, proxies=None, timeout=15):
        if "host1" in proxies["http"]:
            return SimpleNamespace(status_code=200)
        raise parser.requests.exceptions.RequestException("boom")

    monkeypatch.setattr(parser.requests, "get", fake_get)

    parser_instance.get_proxies_user_agents(max_retries=1)

    assert parser_instance.proxies == [
        {"host": "host1", "port": "80", "user": "user1", "password": "pass1"}
    ]

    assert proxy_file.read_text() == "host1:80:user1:pass1\n"

    with open("proxies_user_agents.json", "r") as f:
        data = json.load(f)

    assert list(data.keys()) == ["host1"]


def test_get_proxies_user_agents_handles_empty_list(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    parser_instance = CarsParser([], "https://example.com", 1, min_delay=0, max_delay=0)

    parser_instance.get_proxies_user_agents()

    assert parser_instance.proxies == []
    assert not (tmp_path / "proxies.txt").exists()


def test_get_proxies_user_agents_all_fail(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    proxy_file = tmp_path / "proxies.txt"
    proxy_file.write_text("host1:80:user1:pass1\n")

    proxies = parser.load_proxies(str(proxy_file))
    parser_instance = CarsParser(proxies, "https://example.com", 1, min_delay=0, max_delay=0)

    class DummyUA:
        @property
        def chrome(self):
            return "agent"

    monkeypatch.setattr(parser, "UserAgent", lambda: DummyUA())

    def failing_get(url, headers=None, proxies=None, timeout=15):
        raise parser.requests.exceptions.RequestException("boom")

    monkeypatch.setattr(parser.requests, "get", failing_get)

    parser_instance.get_proxies_user_agents(max_retries=1)

    assert parser_instance.proxies == []
    # original proxy file should remain unchanged
    assert proxy_file.read_text() == "host1:80:user1:pass1\n"


def test_get_logs_and_retries_on_403(tmp_path, monkeypatch):
    log_file = tmp_path / "requests.log"
    handler = logging.FileHandler(log_file)
    handler.setFormatter(logging.Formatter("%(message)s"))
    test_logger = logging.getLogger("test.logger")
    test_logger.setLevel(logging.INFO)
    test_logger.addHandler(handler)
    monkeypatch.setattr(parser, "request_logger", test_logger)

    parser_instance = CarsParser([], "https://example.com", 1, min_delay=0, max_delay=0, max_retries=1)

    responses = [SimpleNamespace(status_code=403), SimpleNamespace(status_code=200)]

    def fake_get(url, headers=None, proxies=None, timeout=15):
        return responses.pop(0)

    monkeypatch.setattr(parser.requests, "get", fake_get)

    proxies_headers = [
        (
            {"http": "http://u:p@1.1.1.1:80", "https": "http://u:p@1.1.1.1:80"},
            {"User-Agent": "ua1"},
        ),
        (
            {"http": "http://u:p@2.2.2.2:80", "https": "http://u:p@2.2.2.2:80"},
            {"User-Agent": "ua2"},
        ),
    ]

    def fake_rand(self):
        return proxies_headers.pop(0)

    monkeypatch.setattr(CarsParser, "get_random_proxies_and_headers", fake_rand)

    response = parser_instance._get("http://example.com")

    assert response.status_code == 200

    records = [json.loads(line) for line in log_file.read_text().splitlines()]
    assert records[0]["proxy"] == "1.1.1.1"
    assert records[0]["event"] == "request"
    assert records[1]["event"] == "403"
    assert records[2]["proxy"] == "2.2.2.2"
    assert records[2]["event"] == "request"


def test_analyze_logs_counts(tmp_path):
    log_file = tmp_path / "requests.log"
    entries = [
        {"event": "request", "proxy": "1.1.1.1", "url": "a", "headers": {"User-Agent": "ua1"}},
        {"event": "403", "proxy": "1.1.1.1", "url": "a", "headers": {"User-Agent": "ua1"}},
        {"event": "403", "proxy": "2.2.2.2", "url": "b", "headers": {"User-Agent": "ua2"}},
    ]
    log_file.write_text("\n".join(json.dumps(e) for e in entries))

    proxy_counts, header_counts = parser.analyze_logs(str(log_file))

    assert proxy_counts == {"1.1.1.1": 1, "2.2.2.2": 1}
    assert header_counts == {"ua1": 1, "ua2": 1}
