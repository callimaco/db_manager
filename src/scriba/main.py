from scriba.scriba.scriba import DbManager
from secret.secret_man.secret_man import SecretManager as sm
import mysql.connector
db = DbManager(table= 'ticker', db= 'finance')

list_of_dicts = [
    {'key1': 'aaa', 'key2': 'uuu', 'key3': 'value3', 'key4': 'value4', 'key5': 'value5'},
    {'key1': 1, 'key2': 2.0, 'key4': 'value4q1'},
    {'key1': 1, 'key3': 'value3q1', 'key4': 'value4q2', 'key5': 'value5q1'},
    {'key9': 'value1q2', 'key3': 'value3q1', 'key4': 'value4q2', 'key5': 4},
    {'key1': 1, 'key3': 'value3q2', 'key4': 'value4q3'},
    {'key1': 1, 'key2': 2.0, 'key4': 'value4q4'},
    {'key1': 1, 'key2': 2.0, 'key3': 'value3q3', 'key5': 3.1},
    {'key2': 2.0, 'key4': 'value4q5', 'key5': 'value5q3'},
    {'key121': 1, 'key2': 2.0, 'key4': 'value4q6'},
    {'key2': 2.0, 'key3': 'value3q4', 'key5': 'value5q4'},
    {'key1': 1, 'key3': 'value3q5', 'key5': 'value5q5'},
    {'key7': 'value1q7', 'key3': 'value3q5', 'key5': 'value5q5'},
    {'key8': 'hola'},
    {'y1234': 'dai', 'u35': 'pem'}
]

with mysql.connector.connect(**sm.config(db='finance')) as cnx:
    crs = cnx.cursor()

    print(db._table_existence(crs= crs))