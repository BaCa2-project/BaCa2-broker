import sqlite3 as sq3

from settings import BASE_DIR


def setup_db():
    conn = sq3.connect(BASE_DIR / 'submit_control.db')
    c = conn.cursor()
    with open(BASE_DIR / 'db' / 'creator.sql') as f:
        c.executescript(f.read())
    conn.commit()
    conn.close()


if __name__ == '__main__':
    setup_db()
