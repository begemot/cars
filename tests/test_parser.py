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
    return CarsParser([], "https://example.com", 1)


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

    parser_instance = CarsParser([], "https://example.com", 1)
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

    parser_instance = CarsParser([], "https://example.com", 1)

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
