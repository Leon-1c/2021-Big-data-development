import os
import jaydebeapi


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

