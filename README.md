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

