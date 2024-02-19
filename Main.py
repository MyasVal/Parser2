import requests
from bs4 import BeautifulSoup
from model import House
import re
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

            cursor.execute('DROP TABLE IF EXISTS Apartments;')

            create_table_query = (
                'CREATE TABLE Apartments (id int AUTO_INCREMENT, Name varchar(255), Price varchar(255), '
                'Address varchar(255), Date varchar(255), PRIMARY KEY (id))'
                'DEFAULT CHARACTER SET utf8;'
            )

            cursor.execute(create_table_query)
            print('Table create successfully')

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
    save_to_database(list_house)


def smotritel(url: str):
    max_site_number = find_max_site_number(url)  # Находим максимальное значение числа X
    if max_site_number is not None:
        page_number = 1
        while page_number <= max_site_number:
            res = requests.get(f"{url}{page_number}")
            print(f"Scraping page {page_number}... URL: {res.url}")
            soup = BeautifulSoup(res.text, 'html.parser')
            houses = soup.find_all('div', class_='sEnLiDetails')
            print(f"Number of houses found on page {page_number}: {len(houses)}")
            parser(houses)
            page_number += 1
    else:
        print("Не удалось найти максимальное значение числа X.")


def find_max_site_number(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'lxml')
            # Найдем все ссылки на странице
            links = soup.find_all('a', href=True)
            max_site_number = None
            for link in links:
                href = link['href']
                match = re.search(r'site=(\d+)', href)
                if match:
                    site_number = int(match.group(1))
                    if max_site_number is None or site_number > max_site_number:
                        max_site_number = site_number
            return max_site_number
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

    except Exception as ex:
        print('Failed to save data to the database...')
        print(ex)

    finally:
        connection.close()

# Пример использования:
if __name__ == '__main__':
    smotritel(url='https://kzn.bezposrednikov.ru/?site=')
