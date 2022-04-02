import psycopg2
from configparser import ConfigParser
import time


def getDBConfigs(filename='DBConfig.ini', section='postgresql'):
    parser = ConfigParser()
    parser.read(filename)

    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db


def loadData(dir):
    try:
        params = getDBConfigs()
        conn = psycopg2.connect(**params)

        cur = conn.cursor()

        # f = open(dir + '\\region.tbl', 'r')
        # start = time.time()
        # cur.copy_from(f, 'region', sep='|')
        # end = time.time()
        # region = end - start
        #
        # f = open(dir + '\\lineitem.tbl', 'r')
        # start = time.time()
        # cur.copy_from(f, 'lineitem', sep='|')
        # end = time.time()
        # lineitem = end - start
        #
        # f = open(dir + '\\customer.tbl', 'r')
        # start = time.time()
        # cur.copy_from(f, 'customer', sep='|')
        # end = time.time()
        # customer = end - start
        #
        # f = open(dir + '\\nation.tbl', 'r')
        # start = time.time()
        # cur.copy_from(f, 'nation', sep='|')
        # end = time.time()
        # nation = end - start

        f = open(dir + '\\orders.tbl', 'r')
        start = time.time()
        cur.copy_from(f, 'orders', sep='|')
        end = time.time()
        orders = end - start

        # f = open(dir + '\\part.tbl', 'r')
        # start = time.time()
        # cur.copy_from(f, 'part', sep='|')
        # end = time.time()
        # part = end - start
        #
        # f = open(dir + '\\partsupp.tbl', 'r')
        # start = time.time()
        # cur.copy_from(f, 'partsupp', sep='|')
        # end = time.time()
        # partsupp = end - start
        #
        # f = open(dir + '\\supplier.tbl', 'r')
        # start = time.time()
        # cur.copy_from(f, 'supplier', sep='|')
        # end = time.time()
        # supplier = end - start

        # total = customer + lineitem + nation + orders + part + partsupp + region + supplier

        # print(f"Customer load time: {customer}")
        # print(f"LineItem load time: {lineitem}")
        # print(f"Nation load time: {nation}")
        # print(f"Part load time: {part}")
        # print(f"Partsupp load time: {partsupp}")
        # print(f"Region load time: {region}")
        # print(f"Supplier load time: {supplier}")
        # print(f"Total load time: {total}")
        print(orders)

    except Exception as error:
        return error
    finally:
        conn.commit()
        conn.close()


def loadQuery(n):
    queryLoaded = ""
    with open("queries.txt", "r") as f:
        numberQuery = 0
        lines = f.readlines()
        for line in lines:
            if '#Q' in line:
                numberQuery += 1
            else:
                if numberQuery == n:
                    queryLoaded += line.replace('\n', ' ').replace('\t', ' ')
                elif numberQuery > n:
                    break
    return queryLoaded


def query(n):
    try:
        params = getDBConfigs()
        conn = psycopg2.connect(**params, options='-c statement_timeout=14400000')

        s = loadQuery(n)

        cur = conn.cursor()

        start = time.time()
        cur.execute(s)
        end = time.time()
        total = end - start

        print(f"Total for query {n}: {total}")

    except Exception as error:
        print(f"Total for query {n}: TimeOut")
        return error
    finally:
        conn.commit()
        conn.close()


def queries(ns):
    try:
        params = getDBConfigs()
        conn = psycopg2.connect(**params, options='-c statement_timeout=14400000')

        timeList = []
        for n in ns:
            s = loadQuery(n)

            cur = conn.cursor()

            start = time.time()
            cur.execute(s)
            end = time.time()
            total = end - start
            timeList.append(total)

        print(f"Total for queries {ns}: {timeList}")

    except Exception as error:
        print("TimeOut")
        return error
    finally:
        conn.commit()
        conn.close()


def loadFile(file):
    queryLoaded = ""
    with open(file, 'r') as f:
        lines = f.readlines()
        for line in lines:
            queryLoaded += line.replace('\n', ' ').replace('\t', ' ')
    return queryLoaded


def addKeys(file):
    try:
        params = getDBConfigs()
        conn = psycopg2.connect(**params)

        s = loadFile(file)

        cur = conn.cursor()

        start = time.time()
        cur.execute(s)
        end = time.time()
        total = end - start

        print(f"Total for addKeys: {total}")

    except Exception as error:
        return error

    finally:
        conn.commit()
        conn.close()


def dropKeys():
    try:
        params = getDBConfigs()
        conn = psycopg2.connect(**params)

        s = loadFile("dropKeys.txt")

        cur = conn.cursor()

        start = time.time()
        cur.execute(s)
        end = time.time()
        total = end - start

        print(f"Total for dropKeys: {total}")

    except Exception as error:
        return error

    finally:
        conn.commit()
        conn.close()


def dropTables():
    try:
        params = getDBConfigs()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        cur.execute("DELETE FROM customer")
        cur.execute("DELETE FROM lineitem")
        cur.execute("DELETE FROM nation")
        cur.execute("DELETE FROM orders")
        cur.execute("DELETE FROM part")
        cur.execute("DELETE FROM partsupp")
        cur.execute("DELETE FROM region")
        cur.execute("DELETE FROM supplier")

    except Exception as error:
        return error

    finally:
        conn.commit()
        conn.close()


def getExplain(n):  # lentas -> 9,18 | rapidas -> 11,16
    try:
        params = getDBConfigs()
        conn = psycopg2.connect(**params, options='-c statement_timeout=14400000')

        s = loadQuery(n)

        cur = conn.cursor()

        start = time.time()
        cur.execute(s)
        end = time.time()
        total = end - start

        print(f"Total for query {n}: {total}")

    except Exception as error:
        print(f"Total for query {n}: TimeOut")
        return error
    finally:
        conn.commit()
        conn.close()


for i in range(9, 23):
    query(i)
