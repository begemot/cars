import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Any, Tuple
from tqdm import tqdm
import json
from urllib.request import urlretrieve
import os
from dotenv import load_dotenv
from fake_useragent import UserAgent
from random import randint
from multiprocess import Pool
from functools import partial
import time


class CarsParser:
    def __init__(
        self,
        proxy_hosts: List[str],
        proxy_ports_http: List[str],
        proxy_users: List[str],
        proxy_passwords: List[str],
        default_url: str,
        processes: int
    ):
        self.proxy_hosts = proxy_hosts
        self.proxy_ports_http = proxy_ports_http
        self.proxy_users = proxy_users
        self.proxy_passwords = proxy_passwords
        self.default_url = default_url
        self.processes = processes


    def get_proxies_user_agents(
        self
    ):
        """ Получает словарь {прокси-хост: user-agent} """

        if not (len(self.proxy_hosts) == len(self.proxy_ports_http) == len(self.proxy_users) == len(self.proxy_passwords)):
            raise ValueError("Убедитесь, что правильно передали переменные окружения (.env)")
        
        ua = UserAgent()

        user_agents_url = "proxies_user_agents.json"

        if os.path.exists(user_agents_url):
            with open(user_agents_url, "r") as f:
                user_agents = json.load(f)
        else:
            user_agents = {}

        for host, port_http, user, password in tqdm(zip(
            self.proxy_hosts, self.proxy_ports_http, self.proxy_users, self.proxy_passwords
        ), desc="Загрузка User-Agent для Proxies", total=len(self.proxy_hosts)):
            if not user_agents.get(host):
                flag = True
                while flag:
                    user_agent = ua.chrome

                    proxies = {
                        "http": f"http://{user}:{password}@{host}:{port_http}",
                        "https": f"http://{user}:{password}@{host}:{port_http}"
                    }
                    headers = {
                        "User-Agent": user_agent
                    }
                    try:
                        response = requests.get(self.default_url, headers=headers, proxies=proxies, timeout=15)
                        if response.status_code == 200:
                            user_agents[host] = user_agent
                            flag = False
                        else:
                            raise ValueError()
                    except:
                        continue
                    
        with open(user_agents_url, "w") as f:
            json.dump(user_agents, f)

        self.user_agents = user_agents

    
    def get_random_proxies_and_headers(self) -> Tuple[Dict[str, str], Dict[str, str]]:
        """ Получает случайный proxies и headers для GET-запроса"""

        rand_i = randint(0, len(self.proxy_hosts) - 1)
        proxy_host = self.proxy_hosts[rand_i]
        proxy_port_http = self.proxy_ports_http[rand_i]
        proxy_user = self.proxy_users[rand_i]
        proxy_pass = self.proxy_passwords[rand_i]

        proxies = {
            "http": f"http://{proxy_user}:{proxy_pass}@{proxy_host}:{proxy_port_http}",
            "https": f"http://{proxy_user}:{proxy_pass}@{proxy_host}:{proxy_port_http}"
        }

        headers = {
            "User-Agent": self.user_agents[proxy_host]
        }

        return proxies, headers
    

    def get_params(self):
        """ Получает список типов продаж (new, used, cpo) и список брендов с сайта """

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

        return ["used"], car_makes
    

    def get_models(self, stock_type: str, car_make: str) -> List[str]:
        """ Получает список моделей для конкретного бренда и типа продажи """

        proxies, headers = self.get_random_proxies_and_headers()
        params = {
            "include_shippable": "true",
            "makes[]": car_make,
            "maximum_distance": "all",
            "page_size": "20",
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

        if os.path.exists("car_models.json"):
            with open("car_models.json", "r") as f:
                car_models = json.load(f)
        else:
            car_models = {}

            for stock_type in tqdm(car_stock_types, desc="Загрузка моделей автомобилей..."):

                with Pool(self.processes) as pool:
                    # Создаем список аргументов для каждого процесса
                    args = [(stock_type, car_make) for car_make in car_makes]
                    
                    # Запускаем процессы параллельно
                    results = pool.starmap(self.get_models, args)
                    
                    # Собираем результаты в словарь
                    car_make_models = dict(zip(car_makes, results))
                
                car_models[stock_type] = car_make_models

            with open('car_models.json', 'w') as f:
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
            "page_size": "20",
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
        """ Извлекает список ссылок на карточки автомобилей с конкретной страницы """

        proxies, headers = self.get_random_proxies_and_headers()
        params = {
            "include_shippable": "true",
            "makes[]": car_make,
            "maximum_distance": "all",
            "page_size": "20",
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
        cards = soup.find_all("a", {"class": "vehicle-card-link"})
        vehicle_hrefs = list(map(lambda card: card["href"], cards))

        return vehicle_hrefs
    

    def get_number(self, line: str):
        """ Преобразует строку с текстом в число (оставляет только цифры) """

        try:
            number = int(''.join(filter(str.isdigit, line)))
            return number
        except:
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


    def parse_basics_block(self, soup: BeautifulSoup) -> dict:
        """ Парсит блок "Основная информация" на странице авто и возвращает словарь """

        basics_dict = {}

        basics_block = soup.find("section", {"class": "basics-section"})
        keys = basics_block.find_all("dt")
        keys = list(map(lambda key: key.text.strip(), keys))
        values = basics_block.find_all("dd")
        values = list(map(lambda value: value.text.strip(), values))
        
        for key, value in zip(keys, values):
            basics_dict[key] = value
        
        return basics_dict


    def parse_features_block(self, soup: BeautifulSoup) -> dict:
        """ Парсит блок "Особенности/функции" на странице авто и возвращает словарь """

        features_dict = {}

        features_block = soup.find("section", {"class": "features-section"})
        keys = features_block.find_all("dt")
        keys = list(map(lambda key: key.text.strip(), keys))
        values = features_block.find_all("dd")
        values = list(map(lambda value: [li.text.strip() for li in value.find_all("li")], values))

        for key, value in zip(keys, values):
            features_dict[key] = value
        
        return features_dict


    def parse_sellers_info_block(self, soup: BeautifulSoup) -> str:
        """ Извлекает блок информации о продавце (дилер/частник) """

        try:
            sellers_info_block = soup.find("cars-line-clamp").text.strip()
            return sellers_info_block
        except:
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
                self.all_vehicle_ids.remove(vehicle_id)

        except:
            print("Ошибка при обновлении цены")

    
    def get_vehicle_price_by_id(self, vehicle_id: str):
        """ Получает цену автомобиля по его ссылке """

        try:
            vehicle_href = f"/vehicledetail/{vehicle_id}"
            proxies, headers = self.get_random_proxies_and_headers()
            url = f"https://cars.com{vehicle_href}"
            response = requests.get(url, headers=headers, proxies=proxies)

            if response.status_code != 200:
                raise ValueError("Ошибка:", response.status_code)
            
            soup = BeautifulSoup(response.content, "html.parser")
            new_price = self.get_number(soup.find("span", {"class": "primary-price"}).text)

            self.update_vehicle_price(vehicle_id, new_price)

        except:
            print("Ошибка при обновлении цены")
        

    def update_prices(
        self
    ):
        """ Обновляет цены автомобилей """

        # Цены автомобилей обновляем с переходом на страницу
        with Pool(self.processes) as pool:
            for _ in tqdm(pool.imap(self.get_vehicle_price_by_id, self.all_vehicle_ids), 
                total=len(self.all_vehicle_ids),
                desc=f"Обновление цен автомобилей"
            ):
                time.sleep(0.1)
                
    
    def save_vehicle_info(self, vehicle_id: str, car_info: Dict[str, Any]) -> None:
        """ Сохранение информации об автомобиле """

        url = f"data/{vehicle_id}/car_data.json"
        with open(url, "w") as f:
            json.dump(car_info, f)
        

    def get_vehicle_info(self, vehicle_href: str, brand_words_num: int = 1):
        """ Собирает всю информацию об автомобиле: год, бренд, модель, пробег, цена, блоки, фото """

        try:
            vehicle_id = vehicle_href.split("/")[2]
            if vehicle_id in self.all_vehicle_ids:
                return

            url = f"https://cars.com{vehicle_href}"
            proxies, headers = self.get_random_proxies_and_headers()
            response = requests.get(url, headers=headers, proxies=proxies)

            if response.status_code != 200:
                raise ValueError("Ошибка:", response.status_code)
            
            soup = BeautifulSoup(response.content, "html.parser")
            title = soup.find("div", {"class": "title-row"}).find("h1").text.split(" ")

            info = {}
            info["id"] = vehicle_id
            info["year"] = int(title[0])
            info["brand"] = " ".join(title[1:brand_words_num + 1])
            info["model"] = " ".join(title[brand_words_num + 1:])
            info["mileage"] = self.get_number(soup.find("p", {"class": "listing-mileage"}).text)
            info["price"] = self.get_number(soup.find("span", {"class": "primary-price"}).text)
            info["basics_section"] = self.parse_basics_block(soup)
            info["features_section"] = self.parse_features_block(soup)
            info["seller_info"] = self.parse_sellers_info_block(soup)

            images = soup.find("gallery-filmstrip").find_all("img")
            images = list(map(lambda image: image["src"], images))

            self.download_images(vehicle_id, images)
            self.save_vehicle_info(vehicle_id, info)

        except:
            print("Ошибка при извлечении данных")
    

    def parse_data(
        self,
        car_stock_types: List[str],
        car_makes: List[str],
        car_models: List[str]
    ):
        """ Собирает данные обо всех автомобилях """
        self.all_vehicle_ids = self.get_all_vehicle_ids()
        self.update_prices()

        for stock_type in car_stock_types:
            for car_make in car_makes:

                if car_make in ["mercedes_benz", "rolls_royce"]:
                    brand_words_num = 1
                else:
                    brand_words_num = len(car_make.split("_"))
                for car_model in car_models[stock_type][car_make]:
                    last_page = self.get_pages_num(stock_type, car_make, car_model)
                    for page in range(1, last_page + 1):
                        vehicle_hrefs = self.get_vehicle_page_hrefs(stock_type, car_make, car_model, page)

                        processing_func = partial(self.get_vehicle_info, brand_words_num=brand_words_num)
                        with Pool(self.processes) as pool:
                            for _ in tqdm(pool.imap(processing_func, vehicle_hrefs), 
                                total=len(vehicle_hrefs),
                                desc=f"Загрузка: {stock_type} | {car_make} | {car_model} | страница {page}"
                            ):
                                time.sleep(0.1)
    
    
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

    # Получаем переменные
    proxy_hosts = os.getenv('PROXY_HOST').split(",")
    proxy_ports_http = os.getenv('PROXY_PORT_HTTP').split(",")
    proxy_users = os.getenv('PROXY_USER').split(",")
    proxy_passwords = os.getenv('PROXY_PASSWORD').split(",")
    default_url = os.getenv('DEFAULT_URL')
    processes = int(os.getenv('PROCESSES'))

    parser = CarsParser(
        proxy_hosts,
        proxy_ports_http,
        proxy_users,
        proxy_passwords,
        default_url,
        processes
    )

    parser.run()


if __name__ == "__main__":
    main()