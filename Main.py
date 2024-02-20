import requests
from bs4 import BeautifulSoup
from model import House
import pymysql
from config import host, user, password, db_name
import pymysql.cursors

try:
    connection = pymysql.connect(
        host=host,
        user=user,
        password=password,
        database=db_name,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    print('Successfully connected')
    print('#' * 20)

    try:
        with connection.cursor() as cursor:

            cursor.execute('DROP TABLE IF EXISTS Apartments')

            create_table_query = (
                "CREATE TABLE Apartments (id int AUTO_INCREMENT, Name varchar(255), "
                "Price varchar(255), Address varchar(255), Date varchar(255), "
                "PRIMARY KEY (id)) "
                "DEFAULT CHARACTER SET utf8"
            )

            cursor.execute(create_table_query)
            print('Table create successfully')
            print('#' * 20)

    finally:
        connection.close()

except Exception as ex:
    print('Connection refused...')
    print(ex)


def parser(houses):
    list_house = []
    for house in houses:
        name = house.find('div', class_='sEnLiTitle').text.strip()
        price = house.find('div', class_='sEnLiPrice').text.strip()
        address = house.find('div', class_='sEnLiCity').text.strip()
        date = house.find('div', class_='sEnLiDate').text.strip()
        list_house.append(House(name=name, price=price, address=address, date=date))
    print(list_house)
    print('#' * 20)
    save_to_database(list_house)


def scrape(url: str):
    max_pages_number = find_max_pages_number(url)  # Находим максимальное значение числа страниц
    if max_pages_number is not None:
        page_number = 1
        while page_number <= max_pages_number:
            res = requests.get(f"{url}{page_number}")
            print(f"Scraping page {page_number}... URL: {res.url}")
            soup = BeautifulSoup(res.text, 'html.parser')
            houses = soup.find_all('div', class_='sEnLiDetails')
            print(f"Number of houses found on page {page_number}: {len(houses)}")
            print('#' * 20)
            parser(houses)
            page_number += 1
    else:
        print("Не удалось найти максимальное значение числа страниц.")


def find_max_pages_number(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'lxml')
            # Найдем ссылку на последнюю страницу
            link = soup.find('a', class_='last')['href']
            max_pages = int(link[-2:]) # Небольшой костыль)
            print(f"Всего страниц = {max_pages}")
            print('#' * 20)
            return max_pages
        else:
            print("Ошибка при получении страницы:", response.status_code)
            return None
    except Exception as e:
        print("Ошибка:", e)
        return None


def save_to_database(houses: list[House]):
    try:
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=db_name,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

        with connection.cursor() as cursor:
            for house in houses:
                sql = "INSERT INTO Apartments (Name, Price, Address, Date) VALUES (%s, %s, %s, %s)"
                cursor.execute(sql, (house.name, house.price, house.address, house.date))

        with connection.cursor() as cursor:
            cursor.execute('SELECT * FROM Apartments')
            rows = cursor.fetchall()
            for row in rows:
                print(row)
            print('#' * 20)

        connection.commit()
        print("Data successfully saved to the database.")
        print('#' * 20)

    except Exception as ex:
        print('Failed to save data to the database...')
        print(ex)

    finally:
        connection.close()


# Пример использования:
if __name__ == '__main__':
    scrape(url='https://kzn.bezposrednikov.ru/?site=')
