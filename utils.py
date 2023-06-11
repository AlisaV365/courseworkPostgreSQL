import psycopg2


class DBManager:
    def __init__(self):
        self.conn = psycopg2.connect(host='localhost', database='north', user='alisavorotnikova', password='cxz3t55')

    def creating_tables(self):
        """создаем таблицы с информацией о компаниях и вакансиях"""

        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS companies (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                city TEXT NOT NULL,
                description TEXT,
                url TEXT NOT NULL
            );
            """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS vacancies (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                company_id INTEGER REFERENCES companies(id),
                salary_min INTEGER,
                salary_max INTEGER,
                url TEXT NOT NULL
            );                
        """)
        self.conn.commit()

    def insert_table(self, company, vacancies):
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO companies (id, name, city, description, url)
            VALUES (%s, %s, %s, %s, %s)""",
                    (int(company['id']), company['name'], company['area']['name'],
                     company['description'].replace('<strong>', '').replace('</strong>', '').replace('<p>', '').replace('</p>', ''),
                     company['alternate_url'])
                    )

        self.conn.commit()

        for item in vacancies['items']:
            from_ = None
            to_ = None
            if item['salary']:
                from_ = item['salary']['from']
                to_ = item['salary']['to']
            cur.execute(f"""
                    INSERT INTO vacancies (name, company_id, salary_min, salary_max, url)
                    VALUES (%s, %s, %s, %s, %s)""",
                        (item['name'], item['employer']['id'], from_, to_, item['alternate_url'])
                        )
            self.conn.commit()


    def delete_tables(self):
        """удаляем таблицы"""
        cur = self.conn.cursor()
        cur.execute("DROP TABLE IF EXIST vacancies;")
        cur.execute("DROP TABLE IF EXIST companies;")
        self.conn.commit()


    def get_companies_and_vacancies_count(self):
        """получаем список всех компаний и количество вакансий у каждой компании"""

        cur = self.conn.cursor()
        cur.execute("""
                SELECT companies.name, COUNT(vacancies.id)
                FROM companies JOIN vacancies ON vacancies.company_id = companies.id
                GROUP BY companies.name;
            """)
        return cur.fetchall()


    def get_all_vacancies(self):
        """получаем список всех вакансий с указанием названия компании, названия вакансии и
    зарплаты и ссылки на вакансию."""
        cur = self.conn.cursor()
        cur.execute("""
                SELECT companies.name, vacancies.name, vacancies.salary_min, vacancies.salary_max, vacancies.url
                FROM vacancies JOIN companies ON vacancies.company_id = companies.id;
            """)
        return cur.fetchall()


    def get_avg_salary(self):
        """получаем среднюю зарплату по вакансиям."""

        cur = self.conn.cursor()
        cur.execute("""
                SELECT AVG(vacancies.salary_max)
                FROM vacancies;
            """)
        return cur.fetchone()


    def get_vacancies_with_higher_salary(self):
        """получаем список всех вакансий, у которых зарплата выше средней по всем вакансиям."""

        cur = self.conn.cursor()
        cur.execute(f"""
                SELECT companies.name, vacancies.name, vacancies.salary_min, vacancies.salary_max, vacancies.url
                FROM vacancies JOIN companies ON vacancies.company_id = companies.id
                WHERE vacancies.salary_max > ({self.get_avg_salary()[0]});
            """)
        return cur.fetchall()


    def get_vacancies_with_keyword(self, keyword):
        """получаем список всех вакансий, в названии которых содержатся переданные в метод слова, например 'python'"""

        cur = self.conn.cursor()
        cur.execute(f"""
                SELECT *
                FROM vacancies WHERE vacancies.name ILIKE '%{keyword}%';
            """)
        return cur.fetchall()


if __name__ == '__main__':
    dbm = DBManager()
