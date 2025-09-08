import json
import logging
import os
import time
from functools import partial
from urllib.request import urlretrieve

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from fake_useragent import UserAgent
from multiprocessing import Value
from multiprocessing.pool import ThreadPool as Pool
from tqdm import tqdm
from typing import List, Dict, Any, Tuple


_proxy_index = None


def init_pool(proxy_index):
    """Pool initializer to set shared proxy index in child processes."""
    global _proxy_index
    _proxy_index = proxy_index


def load_proxies(file_path: str) -> List[Dict[str, str]]:
    """Load proxy configuration from a file.

    Each non-empty line in ``file_path`` should contain four ``:``-separated
    values representing host, port, username and password respectively.

    Args:
        file_path: Path to the proxies file.

    Returns:
        A list of dictionaries with keys ``host``, ``port``, ``user`` and
        ``password`` representing the proxy records.
    """

    proxies: List[Dict[str, str]] = []

    with open(file_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            host, port, user, password = line.split(":")
            proxies.append({
                "host": host,
                "port": port,
                "user": user,
                "password": password,
            })

    return proxies


logging.basicConfig(level=logging.INFO)


class CarsParser:
    def __init__(
        self,
        proxies: List[Dict[str, str]],
        default_url: str,
        processes: int,
    ):
        """Create a parser instance using proxy data from a file.

        Args:
            proxies: List of proxy records returned by :func:`load_proxies`.
            default_url: Base URL for requests.
            processes: Number of worker processes to use.
        """

        self.proxies = proxies
        self.default_url = default_url
        self.processes = processes
        global _proxy_index
        _proxy_index = Value('i', 0)


    def get_proxies_user_agents(
        self,
        max_retries: int = 3,
        retry_delay: float = 0,
    ):
        """ Получает словарь {прокси-хост: user-agent}

        Args:
            max_retries: Максимальное количество попыток для каждого прокси.
            retry_delay: Задержка между повторными попытками в секундах.
        """

        ua = UserAgent()

        user_agents_url = "proxies_user_agents.json"

        if os.path.exists(user_agents_url):
            with open(user_agents_url, "r") as f:
                user_agents = json.load(f)
        else:
            user_agents = {}

        successful_proxies = []

        for proxy in tqdm(
            self.proxies,
            desc="Загрузка User-Agent для Proxies",
            total=len(self.proxies),
        ):
            host = proxy["host"]
            port_http = proxy["port"]
            user = proxy["user"]
            password = proxy["password"]

            if user_agents.get(host):
                continue

            for _ in range(max_retries):
                user_agent = ua.chrome

                proxies = {
                    "http": f"http://{user}:{password}@{host}:{port_http}",
                    "https": f"http://{user}:{password}@{host}:{port_http}",
                }
                headers = {"User-Agent": user_agent}
                try:
                    response = requests.get(
                        self.default_url, headers=headers, proxies=proxies, timeout=15
                    )
                    if response.status_code == 200:
                        user_agents[host] = user_agent
                        successful_proxies.append(proxy)
                        break
                    raise ValueError(
                        "Unexpected status code: %s" % response.status_code
                    )
                except (requests.exceptions.RequestException, ValueError) as exc:
                    logging.warning(
                        f"Could not retrieve user agent for proxy {host}: {exc}"
                    )
                    if retry_delay:
                        time.sleep(retry_delay)
            else:
                logging.error(
                    f"Exceeded {max_retries} retries for proxy {host}. Skipping."
                )
                continue

        with open(user_agents_url, "w") as f:
            json.dump(user_agents, f)

        self.user_agents = user_agents
        self.proxies = successful_proxies

        proxy_file = os.getenv("PROXY_FILE", "proxies.txt")
        try:
            with open(proxy_file, "w") as f:
                for proxy in self.proxies:
                    f.write(
                        f"{proxy['host']}:{proxy['port']}:{proxy['user']}:{proxy['password']}\n"
                    )
        except OSError as exc:
            logging.warning(f"Could not persist proxies to {proxy_file}: {exc}")

    
    def get_random_proxies_and_headers(self) -> Tuple[Dict[str, str], Dict[str, str]]:
        """ Получает proxies и headers для GET-запроса в детерминированном порядке """
        # When no proxies are configured, fall back to direct requests without
        # proxy authentication.  Previously this resulted in a ZeroDivisionError
        # when the proxies list was empty.
        if not self.proxies:
            try:
                user_agent = UserAgent().random
            except Exception:
                user_agent = ""
            headers = {"User-Agent": user_agent} if user_agent else {}
            return {}, headers

        global _proxy_index
        with _proxy_index.get_lock():
            idx = _proxy_index.value
            _proxy_index.value = (idx + 1) % len(self.proxies)

        proxy = self.proxies[idx]
        proxy_host = proxy["host"]
        proxy_port_http = proxy["port"]
        proxy_user = proxy["user"]
        proxy_pass = proxy["password"]

        proxies = {
            "http": f"http://{proxy_user}:{proxy_pass}@{proxy_host}:{proxy_port_http}",
            "https": f"http://{proxy_user}:{proxy_pass}@{proxy_host}:{proxy_port_http}",
        }

        user_agent = self.user_agents.get(proxy_host)
        if not user_agent:
            try:
                user_agent = UserAgent().random
                self.user_agents[proxy_host] = user_agent
            except Exception:
                logging.warning(
                    "Skipping proxy %s due to missing user-agent", proxy_host
                )
                return self.get_random_proxies_and_headers()

        headers = {"User-Agent": user_agent}

        return proxies, headers
    

    def get_params(self):
        """ Получает список типов продаж (new, used, cpo) и список брендов с сайта """

        cache_path = "car_params.json"
        if os.path.exists(cache_path):
            # invalidate cache if older than 24 hours
            if time.time() - os.path.getmtime(cache_path) < 86400:
                with open(cache_path, "r") as f:
                    data = json.load(f)
                return data["car_stock_types"], data["car_makes"]

        proxies, headers = self.get_random_proxies_and_headers()
        response = requests.get(self.default_url, headers=headers, proxies=proxies)
        if response.status_code != 200:
            raise ValueError("Ошибка:", response.status_code)

        soup = BeautifulSoup(response.content, "html.parser")
        filters = soup.find("div", {"id": "search-basics-area"})

        # car_stock_types = filters.find("select", {"data-activitykey": "stock-type-select"}).find_all("option")
        # car_stock_types = list(map(lambda cond: cond["value"], car_stock_types))
        # car_stock_types.remove("all")

        car_makes = []
        car_makes_groups = filters.find("select", {"data-activitykey": "make_select"}).find_all("optgroup")
        for car_makes_group in car_makes_groups:
            car_makes_group = car_makes_group.find_all("option")
            car_makes_group = list(map(lambda make: make["value"], car_makes_group))
            car_makes.extend(car_makes_group)

        car_stock_types = ["used"]
        with open(cache_path, "w") as f:
            json.dump({
                "car_stock_types": car_stock_types,
                "car_makes": car_makes
            }, f)

        return car_stock_types, car_makes
    

    def get_models(self, stock_type: str, car_make: str) -> List[str]:
        """ Получает список моделей для конкретного бренда и типа продажи """

        proxies, headers = self.get_random_proxies_and_headers()
        params = {
            "include_shippable": "true",
            "makes[]": car_make,
            "maximum_distance": "all",
            "page_size": "100",
            "sort": "best_match_desc",
            "stock_type": stock_type,
            "zip": "60606"
        }


        response = requests.get(
            self.default_url, 
            headers=headers, 
            proxies=proxies, 
            params=params
        )

        if response.status_code != 200:
            raise ValueError("Ошибка:", response.status_code)


        soup = BeautifulSoup(response.content, "html.parser")
        car_models = soup.find("div", {"id": "model"}).find_all("input", {"class": "sds-input"})
        car_models = list(map(lambda model: model["value"], car_models))

        return car_models
    

    def get_all_car_models(self, car_stock_types: str, car_makes: str) -> Dict:
        """ Получает все модели автомобиля по состоянию и марке """

        cache_path = "car_models.json"
        if os.path.exists(cache_path) and time.time() - os.path.getmtime(cache_path) < 86400:
            with open(cache_path, "r") as f:
                car_models = json.load(f)
        else:
            car_models = {}

            for stock_type in tqdm(car_stock_types, desc="Загрузка моделей автомобилей..."):

                pool_kwargs = {"initializer": init_pool, "initargs": (_proxy_index,)}
                try:
                    pool = Pool(self.processes, **pool_kwargs)
                except TypeError:
                    pool = Pool(self.processes)
                with pool:
                    # Создаем список аргументов для каждого процесса
                    args = [(stock_type, car_make) for car_make in car_makes]

                    # Запускаем процессы параллельно
                    results = pool.starmap(self.get_models, args)

                    # Собираем результаты в словарь
                    car_make_models = dict(zip(car_makes, results))

                car_models[stock_type] = car_make_models

            with open(cache_path, 'w') as f:
                json.dump(car_models, f)

        return car_models
    

    def get_pages_num(
        self,
        stock_type: str, 
        car_make: str,
        car_model: str
    ):
        """ Определяет количество страниц объявлений для конкретного типа, бренда и модели """

        proxies, headers = self.get_random_proxies_and_headers()
        params = {
            "include_shippable": "true",
            "makes[]": car_make,
            "maximum_distance": "all",
            "page_size": "100",
            "sort": "best_match_desc",
            "stock_type": stock_type,
            "zip": "60606",
            "models[]": car_model,
            "page": 1
        }

        response = requests.get(self.default_url, headers=headers, proxies=proxies, params=params)

        if response.status_code != 200:
            raise ValueError("Ошибка:", response.status_code)
    
        soup = BeautifulSoup(response.content, "html.parser")
        pages = [int(a["phx-value-page"]) for a in soup.select("a[id^=pagination-direct-link-]")]
        last_page = max(pages) if pages else 1
        
        return last_page
    

    def get_vehicle_page_hrefs(
        self,
        stock_type: str,
        car_make: str,
        car_model: str,
        page: int
    ):
        """Извлекает ссылки на авто и подсчитывает количество новых и пропущенных.

        Помимо возврата ссылок на автомобили, функция также подсчитывает
        общее количество объявлений на странице и число объявлений, которые уже
        присутствуют в локальных данных. Это позволяет выводить пользователю
        более подробную статистику при обработке страниц.
        """

        proxies, headers = self.get_random_proxies_and_headers()
        params = {
            "include_shippable": "true",
            "makes[]": car_make,
            "maximum_distance": "all",
            "page_size": "100",
            "sort": "best_match_desc",
            "stock_type": stock_type,
            "zip": "60606",
            "models[]": car_model,
            "page": page
        }

        response = requests.get(self.default_url, headers=headers, proxies=proxies, params=params)

        if response.status_code != 200:
            raise ValueError("Ошибка:", response.status_code)

        soup = BeautifulSoup(response.content, "html.parser")
        cards = soup.find_all("div", {"class": "vehicle-card"})

        vehicle_hrefs = []
        skipped = 0
        for card in cards:
            link = card.find("a", {"class": "vehicle-card-link"})
            if not link or not link.get("href"):
                continue

            href = link["href"]
            vehicle_id = href.split("/")[2]

            price_elem = card.find("span", {"class": "primary-price"})
            price = self.get_number(price_elem.text) if price_elem else 0

            if vehicle_id in self.all_vehicle_ids:
                self.update_vehicle_price(vehicle_id, price)
                skipped += 1
            else:
                vehicle_hrefs.append(href)

        total_on_page = len(cards)

        return vehicle_hrefs, total_on_page, skipped
    

    def get_number(self, line: str):
        """ Преобразует строку с текстом в число (оставляет только цифры) """

        try:
            return int("".join(filter(str.isdigit, line)))
        except (ValueError, TypeError) as exc:
            logging.warning(f"Could not parse number from '{line}': {exc}")
            return 0


    def download_images(self, vehicle_id: str, images: List) -> None:
        """ Скачивает фотографии автомобиля и сохраняет их в папку с ID авто """

        folder_path = f"data/{vehicle_id}/images"
        if not os.path.isdir(folder_path):
            os.makedirs(folder_path)

            proxies, headers = self.get_random_proxies_and_headers()
            for i, image_path in enumerate(images):
                filepath = os.path.join(folder_path, f"{i}.jpg")
                image_path = image_path.replace("small", "xlarge")
                response = requests.get(image_path, stream=True, proxies=proxies, headers=headers)

                with open(filepath, "wb") as f:
                    for chunk in response.iter_content(1024):
                        f.write(chunk)


    def parse_basics_block(self, soup: BeautifulSoup, page_url: str) -> dict:
        """Парсит блок "Основная информация" на странице авто.

        Args:
            soup: Объект :class:`BeautifulSoup` страницы автомобиля.
            page_url: URL страницы, с которой производится парсинг.

        Returns:
            Словарь с ключами и значениями из блока "Основная информация".
        """

        basics_dict = {}

        basics_block = soup.find("section", {"class": "basics-section"})
        if basics_block is None:
            logging.info(f"Basics block not found for {page_url}")
            return basics_dict
        keys = basics_block.find_all("dt")
        keys = list(map(lambda key: key.text.strip(), keys))
        values = basics_block.find_all("dd")
        values = list(map(lambda value: value.text.strip(), values))
        
        for key, value in zip(keys, values):
            basics_dict[key] = value
        
        return basics_dict


    def parse_features_block(self, soup: BeautifulSoup, page_url: str) -> dict:
        """Парсит блок "Особенности/функции" на странице авто.

        Args:
            soup: Объект :class:`BeautifulSoup` страницы автомобиля.
            page_url: URL страницы, с которой производится парсинг.

        Returns:
            Словарь с ключами и значениями из блока "Особенности/функции".
        """

        features_dict = {}

        features_block = soup.find("section", {"class": "features-section"})
        if features_block is None:
            logging.info(f"Features block not found for {page_url}")
            return features_dict
        keys = features_block.find_all("dt")
        keys = list(map(lambda key: key.text.strip(), keys))
        values = features_block.find_all("dd")
        values = list(map(lambda value: [li.text.strip() for li in value.find_all("li")], values))

        for key, value in zip(keys, values):
            features_dict[key] = value
        
        return features_dict


    def parse_sellers_info_block(self, soup: BeautifulSoup, page_url: str) -> str:
        """Извлекает блок информации о продавце (дилер/частник).

        Args:
            soup: Объект :class:`BeautifulSoup` страницы автомобиля.
            page_url: URL страницы, с которой производится парсинг.

        Returns:
            Строку с информацией о продавце или пустую строку, если блок отсутствует.
        """

        try:
            return soup.find("cars-line-clamp").text.strip()
        except AttributeError as exc:
            logging.info(f"Seller info block not found for {page_url}: {exc}")
            return ""
        
    
    def get_all_vehicle_ids(self) -> List[str]:
        """ Получает ID всех автомобилей, полученных при парсинге """
        try:
            folder = "./data"
            vehicle_ids = [name for name in os.listdir(folder) if os.path.isdir(os.path.join(folder, name))]

            return vehicle_ids
        
        except FileNotFoundError:
            return []
        
    
    def update_vehicle_price(self, vehicle_id: str, new_price: int) -> None:
        """ Обновляет цену автомобиля в JSON-файле """

        try:
            url = f"data/{vehicle_id}/car_data.json"
            if os.path.exists(url):
                with open(url, "r") as f:
                    vehicle_info = json.load(f)
                vehicle_info["price"] = new_price

                with open(url, "w") as f:
                    json.dump(vehicle_info, f)
        except (FileNotFoundError, json.JSONDecodeError, OSError, ValueError) as exc:
            logging.error(f"Failed to update price for {vehicle_id}: {exc}")

    
    def get_vehicle_price_by_id(self, vehicle_id: str):
        """ Получает цену автомобиля по его ссылке """

        try:
            vehicle_href = f"/vehicledetail/{vehicle_id}"
            proxies, headers = self.get_random_proxies_and_headers()
            url = f"https://cars.com{vehicle_href}"
            response = requests.get(url, headers=headers, proxies=proxies)

            if response.status_code != 200:
                raise ValueError(f"Unexpected status code: {response.status_code}")

            soup = BeautifulSoup(response.content, "html.parser")
            price_elem = soup.find("span", {"class": "primary-price"})
            new_price = self.get_number(price_elem.text) if price_elem else 0

            self.update_vehicle_price(vehicle_id, new_price)

        except requests.exceptions.RequestException as exc:
            logging.error(f"Failed to fetch vehicle {vehicle_id}: {exc}")
        except (ValueError, AttributeError) as exc:
            logging.error(f"Error parsing price for vehicle {vehicle_id}: {exc}")
        

    def update_prices(
        self
    ):
        """ Обновляет цены автомобилей """

        # Цены автомобилей обновляем с переходом на страницу
        with Pool(self.processes) as pool:
            for _ in tqdm(
                pool.imap(self.get_vehicle_price_by_id, self.all_vehicle_ids),
                total=len(self.all_vehicle_ids),
                desc=f"Обновление цен автомобилей"
            ):
                pass
                
    
    def save_vehicle_info(self, vehicle_id: str, car_info: Dict[str, Any]) -> None:
        """ Сохранение информации об автомобиле """

        url = f"data/{vehicle_id}/car_data.json"
        with open(url, "w") as f:
            json.dump(car_info, f)
        

    def get_vehicle_info(self, vehicle_href: str, brand_words_num: int = 1):
        """Собирает всю информацию об автомобиле: год, бренд, модель, пробег, цена, блоки, фото.

        Передает URL страницы во вспомогательные функции разбора блоков, что позволяет
        логировать отсутствие блоков с привязкой к странице.
        """

        try:
            vehicle_id = vehicle_href.split("/")[2]
            if vehicle_id in self.all_vehicle_ids:
                return

            url = f"https://cars.com{vehicle_href}"
            proxies, headers = self.get_random_proxies_and_headers()
            response = requests.get(url, headers=headers, proxies=proxies)

            if response.status_code != 200:
                raise ValueError(f"Unexpected status code: {response.status_code}")

            soup = BeautifulSoup(response.content, "html.parser")
            title = soup.find("div", {"class": "title-row"}).find("h1").text.split(" ")

            info = {}
            info["id"] = vehicle_id
            info["year"] = int(title[0])
            info["brand"] = " ".join(title[1:brand_words_num + 1])
            info["model"] = " ".join(title[brand_words_num + 1:])
            info["mileage"] = self.get_number(
                soup.find("p", {"class": "listing-mileage"}).text
            )
            info["price"] = self.get_number(
                soup.find("span", {"class": "primary-price"}).text
            )
            info["basics_section"] = self.parse_basics_block(soup, url)
            info["features_section"] = self.parse_features_block(soup, url)
            info["seller_info"] = self.parse_sellers_info_block(soup, url)

            images_block = soup.find("gallery-filmstrip")
            images = images_block.find_all("img") if images_block else []
            images = list(map(lambda image: image["src"], images))

            self.download_images(vehicle_id, images)
            self.save_vehicle_info(vehicle_id, info)

        except requests.exceptions.RequestException as exc:
            logging.error(f"Failed to fetch vehicle info from {vehicle_href}: {exc}")
        except (ValueError, AttributeError, OSError) as exc:
            logging.error(f"Error processing vehicle info for {vehicle_href}: {exc}")
    

    def parse_data(
        self,
        car_stock_types: List[str],
        car_makes: List[str],
        car_models: List[str]
    ):
        """ Собирает данные обо всех автомобилях """
        self.all_vehicle_ids = self.get_all_vehicle_ids()

        for stock_type in car_stock_types:
            for car_make in car_makes:

                if car_make in ["mercedes_benz", "rolls_royce"]:
                    brand_words_num = 1
                else:
                    brand_words_num = len(car_make.split("_"))
                for car_model in car_models[stock_type][car_make]:
                    last_page = self.get_pages_num(stock_type, car_make, car_model)
                    for page in range(1, last_page + 1):
                        vehicle_hrefs, total_on_page, skipped = self.get_vehicle_page_hrefs(
                            stock_type, car_make, car_model, page
                        )

                        processing_func = partial(
                            self.get_vehicle_info, brand_words_num=brand_words_num
                        )
                        with Pool(self.processes) as pool:
                            for _ in tqdm(
                                pool.imap(processing_func, vehicle_hrefs),
                                total=len(vehicle_hrefs),
                                desc=(
                                    f"Загрузка: {stock_type} | {car_make} | {car_model} | "
                                    f"страница {page} | всего {total_on_page}"
                                ),
                            ):
                                pass

                        loaded = len(vehicle_hrefs)
                        print(
                            f"Страница {page}: загружено {loaded} авто, пропущено {skipped}"
                        )
    
    
    def run(self):
        """ Основной запуск парсера: либо обновление цен, либо полное скачивание данных """

        print("Парсер начал работу")

        self.get_proxies_user_agents()

        car_stock_types, car_makes = self.get_params()
        print("Парсер получил параметры")

        car_models = self.get_all_car_models(car_stock_types, car_makes)
        print("Парсер получил данные о моделях автомобилей")

        
        self.parse_data(car_stock_types, car_makes, car_models)
        print("Парсер загрузил данные обо всех автомобилях")


def main():
    load_dotenv()

    proxy_file = os.getenv("PROXY_FILE", "proxies.txt")
    proxies = load_proxies(proxy_file)
    default_url = os.getenv("DEFAULT_URL")
    processes = int(os.getenv("PROCESSES"))

    parser = CarsParser(
        proxies,
        default_url,
        processes,
    )

    parser.run()


if __name__ == "__main__":
    main()
