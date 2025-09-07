import logging

import pytest
from bs4 import BeautifulSoup

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from parser import CarsParser


@pytest.fixture
def parser_instance():
    return CarsParser([], [], [], [], "https://example.com", 1)


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
