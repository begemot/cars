# Cars Parser

This project scrapes car listing data using rotating proxies.

## Headers

The parser sends requests with a set of browser-like headers. The
`get_random_proxies_and_headers` helper returns a dictionary containing the
following required keys:

- `User-Agent`
- `Accept`
- `Accept-Language`
- `Referer` (defaults to the base URL)
- `Connection` (`keep-alive`)

Tests or integrations that mock headers should include these fields to match
the behaviour of the parser.

## Selenium support

Some pages load critical data via JavaScript. For such cases the parser
provides a small helper to fetch fully rendered HTML using a headless Chrome
browser:

```
pip install selenium webdriver-manager
```

```python
from parser import CarsParser

parser = CarsParser([], "https://example.com", 1)
html = parser.get_page_with_selenium("https://example.com")
```

The returned string contains the page source after any dynamic content has
been loaded.

