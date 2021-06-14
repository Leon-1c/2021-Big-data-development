import os
import jaydebeapi

url = 'jdbc:hive2://bigdata129.depts.bingosoft.net:22129/user08_db'
user = 'user08'
password = 'pass@bingo8'
dirver = 'org.apache.hive.jdbc.HiveDriver'

# sql = 'show tables'


def get_jar():
    DIR = './lib/'
    jarFile = []
    for i in os.listdir(DIR):
        jarFile.append(DIR+i)
    return jarFile


def get_url(net, port, db):
    return 'jdbc:hive2://' + net + ':' + port + '/' + db


# jdbc连接
def search(user, password, url, sql):
    jarFile = get_jar()
    conn = jaydebeapi.connect(dirver, url, [user, password], jarFile)
    curs = conn.cursor()
    curs.execute(sql)
    result = curs.fetchall()
    tab = curs.description
    curs.close()
    conn.close()
    return [i[0] for i in tab], result


if __name__ == '__main__':
    user = ''
    password = ''
    a,b = search(user, password, url, 'show tables')
    print(a)
    for i in b:
        print(i)