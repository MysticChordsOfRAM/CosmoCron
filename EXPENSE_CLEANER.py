import psycopg2 as ps
from datetime import date
import sys
import supersecrets as shh

echobase = {'host': shh.db_ip,
            'port': shh.db_port,
            'dbname': shh.db_name,
            'user': shh.db_user,
            'password': shh.db_password}

def rollover():
    today = date.today()
    month = today.strftime('%m')
    year = today.strftime('%Y')
    day = today.strftime('%d')
    sql_date = f'{year}{month}{day}'

    print(f"----------ROLLING OVER FOR {sql_date}--------------")

    home = None
    cursor = None

    try:
        home = ps.connect(**echobase)
        cursor = home.cursor()

        ## Setting up status quo

        cursor.execute("SELECT budget, SUM(amt) FROM prd.discbudget GROUP BY budget;")
        new_balances = cursor.fetchall()

        ## Archiving History

        cursor.execute("CREATE TABLE IF NOT EXISTS prd.budget_archive (LIKE prd.discbudget INCLUDING ALL);")

        query_archive = """
        INSERT INTO prd.budget_archive
        SELECT * FROM prd.discbudget
        WHERE bb = false;
        """
        cursor.execute(query_archive)

        ## Snapshotting Month End Balances

        cursor.execute("CREATE TABLE IF NOT EXISTS prd.budget_snaps (tpd VARCHAR(50), budget VARCHAR(50), amt DECIMAL(10, 2));")

        query_snap = """
        INSERT INTO prd.budget_snaps
        (tpd, budget, amt)
        VALUES (%s, %s, %s);
        """

        for budget, amt in new_balances:
            cursor.execute(query_snap, (sql_date, budget, amt))

        ## Wiping the Working Table

        cursor.execute("DELETE FROM prd.discbudget")

        ## Insert the new BB

        query_bb = """
        INSERT INTO prd.discbudget
        (tpd, budget, amt, bb)
        VALUES (%s, %s, %s, true)
        """

        for budget, amt in new_balances:
            cursor.execute(query_bb, (sql_date, budget, amt))

        home.commit()

    except Exception as e:
        if home:
            home.rollback()
        print(f"ERROR: {e}")

    finally:
        if home:
            cursor.close()
            home.close()

if __name__ == "__main__":
    rollover()
