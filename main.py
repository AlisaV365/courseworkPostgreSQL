from utils import DBManager
import requests


def get_request(url):
    data = requests.get(url)
    return data.json()


def main():
    db = DBManager()
    db.creating_tables()
    employees_id = [4232036, 581458, 5657254, 1455, 2180, 12550, 15478, 5694, 1740, 1122462]

    for employer in employees_id:
        company = get_request(f'https://api.hh.ru/employers/{employer}')
        vacancies = get_request(f'https://api.hh.ru/vacancies?employer_id={employer}&per_page=50')
        db.insert_table(company, vacancies)



if __name__ == '__main__':
    main()
