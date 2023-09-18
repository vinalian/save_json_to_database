import json
from psycopg2.errors import UndefinedTable
from psycopg2.extras import DictCursor
from psycopg2 import connect


class Database:
    def __init__(self):
        # initialize the database connector
        with connect(
                host='',      # server ip address
                port='',           # server port
                user='',       # database username
                password='',       # database user password
                dbname=''  # database name
        ) as self.db:
            self.dict_cur = self.db.cursor(cursor_factory=DictCursor)

    def create_table(self):
        # function to create table
        self.dict_cur.execute(
            '''
            CREATE TABLE IF NOT EXISTS "data" (
                id           serial PRIMARY KEY,
                date         timestamp default now(),
                information  jsonb
            );
            '''
        )
        self.db.commit()

    def get_last_data(self):
        # function to get last appended data
        try:
            self.dict_cur.execute(
                '''
                SELECT * FROM "data" ORDER BY data.date DESC LIMIT 1
                ''')
            return self.dict_cur.fetchone()
        except UndefinedTable:
            # Error: table not found
            return 'UndefinedTable error'

        except Exception as error:
            # another errors
            self.db.rollback()
            raise Exception(error)
        finally:
            self.db.commit()

    def add_new_data(self, information: str) -> bool:
        # append new parsing results to the database
        try:
            self.dict_cur.execute(
                '''
                INSERT INTO "data" (information) VALUES (%s)
                ''',
                (information, )
            )
            return True
        except UndefinedTable:
            # Error: table not found
            return 'UndefinedTable error'

        except Exception as error:
            # another errors
            self.db.rollback()
            raise Exception(error)
        finally:
            self.db.commit()


class Main_script:
    def __init__(self):
        self.db = Database()  # initialize database class

    @staticmethod
    def __create_json_from_dict(information: dict) -> str:
        try:
            return json.dumps(information)
        except Exception as error:
            raise Exception(error)

    def save_data_to_database(self, information: dict) -> bool:
        # Try to save data
        answer = self.db.add_new_data(self.__create_json_from_dict(information))
        if answer == 'UndefinedTable error':
            print('[INFO] Undefined table!')
            # If return error UndefinedTable create table
            self.db.create_table()

            print('[INFO] Table created!')

            answer = self.db.add_new_data(self.__create_json_from_dict(information))

        if not answer:
            raise Exception('Error! Can not save data!')

        return 'Success!'

    def get_data_from_database(self) -> json or None:
        answer = self.db.get_last_data()

        if answer == 'UndefinedTable error':
            # If return error UndefinedTable create table
            self.db.create_table()
            answer = self.db.get_last_data()

        if not answer:
            raise Exception('Error! Have no data!')
        return answer['information']

    @staticmethod
    def write_data_to_json_file(json_data: dict, file_name: str) -> bool:
        if type(json_data) is not dict:
            raise Exception('Type Error!')

        try:
            with open(file_name, 'w') as file:
                json.dump(json_data, file)
            return f'Success! File name: {file_name}'
        except Exception as e:
            raise Exception(f'Writing error! Error: %s' % e)


if __name__ == "__main__":
    main_script = Main_script()
    res = ''
    ## get data to text (console) and variable data
    # res = main_script.get_data_from_database()

    ## write data to json file
    # json_data = main_script.get_data_from_database()
    # file_name = 'file_name.json'
    # res = Main_script().write_data_to_json_file(json_data, file_name)

    ## send data to database from your_variable (variable type = dict)
    # your_variable = {'423432': '432432423', '4234324': '321321', 'test': 'test'}
    # res = main_script.save_data_to_database(information=your_variable)

    ## send data to database from your_file (file type = json)
    # with open(file="data.json", mode='r') as file:
    #     file_data = file.read()
    #
    # file_data = json.loads(file_data)
    # res = main_script.save_data_to_database(information=file_data)
    print(f'[INFO] {res}')
