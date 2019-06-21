import os
import psycopg2
# from psycopg2.errors import UniqueViolation
import praw
import requests
from psycopg2._psycopg import AsIs

from config import config


def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        # execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)

        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


def find_in(start: str, end: str, string: str) -> [str]:
    open = [i for i, letter in enumerate(post.title) if letter == start]
    close = [i for i, letter in enumerate(post.title) if letter == end]
    insides = []
    while len(open) > len(close):
        open.pop(len(open) - 1)
    while len(close) > len(open):
        close.pop(len(close) - 1)
    for i in range(len(open)):
        insides.append(string[open[i]+1:close[i]])
    return insides

def get_year_res(alls:list) -> (int, int):
    year_found = False
    res_found = False
    year = None
    res = None
    for num in alls:
        if not (year_found and res_found):
            if len(num) == 4:
                try:
                    year = int(num)
                    year_found = True
                except:
                    print("not a year")
            if num[-1:].lower() == "p" and (len(num) == 4 or len(num) == 5):
                try:
                    res = int(num[:-1])
                except ValueError as e:
                    continue
                except Exception as e:
                    raise e
                res_found = True
        else:
            break
    if not res_found:
        for num in alls:
            if num == "1080" or num == "720" or num == "1440":
                res = int(num)
    return year, res

if __name__ == '__main__':
    # connect()
    print(os.getcwd())
    sub = "fullmoviesongoogle"
    sub = "python"
    # r = requests.get(f"https://www.reddit.com/r/{sub}/new.json?limit=999999")
    # posts = r.content["data"]["children"]
    reddit = praw.Reddit("movie")
    sub = reddit.subreddit("fullmoviesongoogle")
    conn = psycopg2.connect(**config())
    cur = conn.cursor()
    db_id = 0
    insertcmd = "INSERT INTO posts (%s) VALUES %s RETURNING ID,TITLE;"
    for post in sub.new():
        # if post.removal_reason is None:
        #     continue
        pdict = {}
        # pdict['id'] = db_id
        pdict['reddit_title'] = post.title
        pdict['reddit_link'] = "reddit.com" + post.permalink
        pdict['title'] = post.title.split("(", 1)[0].split("[", 1)[0]
        extras = [num for num in post.title.split() if len(num) < 6]
        parentheses = find_in("(", ")", post.title)
        brackets = find_in("[", "]", post.title)
        both = parentheses + brackets + extras
        year, res = get_year_res(both)
        pdict['year'] = year
        pdict['resolution'] = res
        pdict['link'] = post.url
        website = post.domain.split(".")[1]
        keys = pdict.keys()
        mogrified = cur.mogrify(insertcmd, (AsIs(','.join(keys)), tuple([pdict[key] for key in keys])))
        print(mogrified)
        try:
            cur.execute(mogrified)
        except Exception as e:
            raise e
        passed = False
        # while not passed:
        #     try:
        #         cur.execute(mogrified)
        #         passed = True
        #     except psycopg2.errors.UniqueViolation as e:
        #         db_id += 1
        #     except Exception as e:
        #         raise e
        print(post.title, year, res)
        db_id += 1
    conn.commit()
    cur.close()
    conn.close()
