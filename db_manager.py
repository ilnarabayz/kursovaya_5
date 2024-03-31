import psycopg2
from utils import get_employer, get_vacancies


class DBManager:
    '''Класс для подключения к БД'''
    def get_companies_and_vacancies_count(self):
        '''Метод получает список всех компаний и
        количество вакансий у каждой компании'''

        with psycopg2.connect(host="localhost", database="course_work_5",
                              user="postgres", password="123456") as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT company_name, COUNT(vacancies_name) AS count_vacancies  "
                            f"FROM employers "
                            f"JOIN vacancies USING (employer_id) "
                            f"GROUP BY employers.company_name")
                result = cur.fetchall()
            conn.commit()
        return result

    def get_all_vacancies(self):
        '''Метод получает список всех вакансий с указанием названия компании,
        названия вакансии и зарплаты и ссылки на вакансию'''
        with psycopg2.connect(host="localhost", database="course_work_5",
                              user="postgres", password="123456") as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT employers.company_name, vacancies.vacancies_name, "
                            f"vacancies.payment, vacancies_url "
                            f"FROM employers "
                            f"JOIN vacancies USING (employer_id)")
                result = cur.fetchall()
            conn.commit()
        return result

    def get_avg_salary(self):
        '''Метод получает среднюю зарплату по вакансиям'''
        with psycopg2.connect(host="localhost", database="course_work_5",
                              user="postgres", password="123456") as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT AVG(payment) as avg_payment FROM vacancies ")
                result = cur.fetchall()
            conn.commit()
        return result

    def get_vacancies_with_higher_salary(self):
        '''Метод получает список всех вакансий,
        у которых зарплата выше средней по всем вакансиям'''
        with psycopg2.connect(host="localhost", database="course_work_5",
                              user="postgres", password="123456") as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM vacancies "
                            f"WHERE payment > (SELECT AVG(payment) FROM vacancies) ")
                result = cur.fetchall()
            conn.commit()
        return result

    def get_vacancies_with_keyword(self, keyword):
        '''Метод получает список всех вакансий,
        в названии которых содержатся переданные в метод слова'''
        with psycopg2.connect(host="localhost", database="course_work_5",
                              user="postgres", password="123456") as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM vacancies "
                            f"WHERE lower(vacancies_name) LIKE '%{keyword}%' "
                            f"OR lower(vacancies_name) LIKE '%{keyword}'"
                            f"OR lower(vacancies_name) LIKE '{keyword}%';")
                result = cur.fetchall()
            conn.commit()
        return result

    def create_table(self):
        """Создание БД, созданение таблиц"""

        conn = psycopg2.connect(host="localhost", database="postgres",
                                user="postgres", password="123456")
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute("DROP DATABASE IF EXISTS course_work_5")
        cur.execute("CREATE DATABASE course_work_5")

        conn.close()

        conn = psycopg2.connect(host="localhost", database="course_work_5",
                                user="postgres", password="123456")
        with conn.cursor() as cur:
            cur.execute("""
                        CREATE TABLE employers (
                        employer_id INTEGER PRIMARY KEY,
                        company_name varchar(255),
                        open_vacancies INTEGER
                        )""")

            cur.execute("""
                        CREATE TABLE vacancies (
                        vacancy_id SERIAL PRIMARY KEY,
                        vacancies_name varchar(255),
                        payment INTEGER,
                        requirement TEXT,
                        vacancies_url TEXT,
                        employer_id INTEGER REFERENCES employers(employer_id)
                        )""")
        conn.commit()
        conn.close()

    def add_to_table(self, employers_list):
        """Заполнение базы данных компании и вакансии"""

        with psycopg2.connect(host="localhost", database="course_work_5",
                              user="postgres", password="123456") as conn:
            with conn.cursor() as cur:
                cur.execute('TRUNCATE TABLE employers, vacancies RESTART IDENTITY;')

                for employer in employers_list:
                    employer_list = get_employer(employer)
                    cur.execute('INSERT INTO employers (employer_id, company_name, open_vacancies) '
                                'VALUES (%s, %s, %s) RETURNING employer_id',
                                (employer_list['employer_id'], employer_list['company_name'],
                                 employer_list['open_vacancies']))

                for employer in employers_list:
                    vacancy_list = get_vacancies(employer)
                    for v in vacancy_list:
                        cur.execute('INSERT INTO vacancies (vacancy_id, vacancies_name, '
                                    'payment, requirement, vacancies_url, employer_id) '
                                    'VALUES (%s, %s, %s, %s, %s, %s)',
                                    (v['vacancy_id'], v['vacancies_name'], v['payment'],
                                     v['requirement'], v['vacancies_url'], v['employer_id']))

            conn.commit()
