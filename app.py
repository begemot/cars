import json
import os
from types import SimpleNamespace
from flask import Flask, render_template, request, abort

app = Flask(__name__)

DATA_DIR = os.path.join(os.path.dirname(__file__), 'car_data')


def _car_files():
    for name in os.listdir(DATA_DIR):
        if name.endswith('.json'):
            yield os.path.join(DATA_DIR, name)


def load_cars():
    cars = []
    for path in _car_files():
        with open(path, 'r') as f:
            data = json.load(f)
            cars.append(SimpleNamespace(**data))
    return cars


def load_car(car_id):
    path = os.path.join(DATA_DIR, f"{car_id}.json")
    if not os.path.exists(path):
        return None
    with open(path, 'r') as f:
        data = json.load(f)
    return SimpleNamespace(**data)


@app.route('/')
def index():
    cars = load_cars()

    def apply_filters(car):
        price_min = request.args.get('price_min', type=int)
        price_max = request.args.get('price_max', type=int)
        mileage_min = request.args.get('mileage_min', type=int)
        mileage_max = request.args.get('mileage_max', type=int)
        year_min = request.args.get('year_min', type=int)
        year_max = request.args.get('year_max', type=int)

        if price_min is not None and car.price < price_min:
            return False
        if price_max is not None and car.price > price_max:
            return False
        if mileage_min is not None and car.mileage < mileage_min:
            return False
        if mileage_max is not None and car.mileage > mileage_max:
            return False
        if year_min is not None and car.year < year_min:
            return False
        if year_max is not None and car.year > year_max:
            return False
        return True

    cars = [c for c in cars if apply_filters(c)]

    sort_key = request.args.get('sort')
    order = request.args.get('order', 'asc')
    if sort_key in {'price', 'mileage', 'year'}:
        reverse = order == 'desc'
        cars.sort(key=lambda c: getattr(c, sort_key, 0), reverse=reverse)

    return render_template('index.html', cars=cars)


@app.route('/view/<int:car_id>')
def view_car(car_id: int):
    car = load_car(car_id)
    if not car:
        abort(404)
    return render_template('view.html', car=car)


@app.route('/stat')
def stat():
    files = list(_car_files())
    count = len(files)
    total_size = sum(os.path.getsize(p) for p in files)
    return render_template('stat.html', count=count, total_size=total_size)


if __name__ == '__main__':
    app.run(debug=True)
