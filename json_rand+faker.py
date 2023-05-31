import json
from faker import Faker

fake = Faker('ru_RU')  # Создаем объект Faker с русской локализацией

orders = []

for i in range(20):
    order = {
            "index": "orders",
            "doc_type": "orders_info",
            "id": i+1
        }

    order_data = {
        "id_заказа": fake.random_number(digits=5),
        "дата_заказа": fake.date_between(start_date='-2y', end_date='today').strftime('%Y-%m-%d'),
        "id_заказчика": fake.random_number(digits=5),
        "сведения_о_заказчике": fake.name(),
        "данные_о_заказе": fake.paragraph(),
        "срок_выполнения_заказа": fake.date_between(start_date='today', end_date='+30d').strftime('%Y-%m-%d'),
        "фактическая_дата_выполнения": fake.date_between(start_date='today', end_date='today').strftime('%Y-%m-%d'),
        "стоимость_заказа": fake.random_number(digits=4),
        "id_бригады": fake.random_number(digits=3)
    }
    order_with_data = {
        **order,
        "body": order_data
    }
    orders.append(order_with_data)

# Сохраняем данные в JSON-файл
with open('orders1.json', 'w', encoding='utf-8') as f:
    json.dump(orders, f, indent=2, ensure_ascii=False)
