import mysql.connector
from secret.secret_man.secret_man import SecretManager

values = [1, 1, 3, 1, 4]
db = 'calciopoli'
table = 'calciatori'

col = ['nome', 'cognome', 'ruolo']
one_row = f'({", ".join(["%s" for el in col])})'
multiple_rows = ',\n'.join([f'{one_row}' for el in values])

query = f'INSERT INTO {table} ({", ".join(col)})\nVALUES \n{multiple_rows};'


with mysql.connector.connect(**SecretManager.config(db= 'calciopoli')) as cnx:
    crs = cnx.cursor()
    crs.execute("DESCRIBE cal;")
    cnx.commit()


