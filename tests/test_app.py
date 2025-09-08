import os
import sys
import pytest
from bs4 import BeautifulSoup

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app import app


@pytest.fixture
def client():
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client


def test_index_default_sorted_and_recent(client):
    response = client.get('/')
    assert response.status_code == 200
    soup = BeautifulSoup(response.data, 'html.parser')
    rows = soup.find_all('tr')[1:]
    ids = [r.find_all('td')[0].text.strip() for r in rows]
    assert ids == ['1', '2']
