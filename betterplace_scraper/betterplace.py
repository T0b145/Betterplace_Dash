import os
from argparse import ArgumentParser
import logging
from datetime import datetime, timezone, timedelta
import time
import dateutil.parser
import pandas as pd
import sqlite3
import sqlalchemy
import requests

logging.basicConfig(filename='./logs/betterplace.log', filemode='w', level=logging.INFO)

class betterplace(object):
    """Betterplace handler"""

    def __init__(self):
        self.df_projects = pd.DataFrame()
        self.download_time = datetime.now()

    def get_projects(self, url, full_search=True):
        per_page = 50
        page = 1
        max_pages = 2
        dt_last_week = datetime.now()-timedelta(days = 7)

        while page <= max_pages:
            url_project_query = url.format(per_page, page)
            for i in range(5):
                try:
                    r = requests.get(url_project_query, timeout=30)
                    break
                except:
                    print ("Internet Error: ", i)
                    time.sleep(60)
            if r.status_code ==200:
                r_js = r.json()
                self.total_projects = r_js["total_entries"]
                for project in r_js["data"]:
                    try:
                        self.parse_overview_data_original(project)
                        updated_at = dateutil.parser.parse(project["updated_at"]).replace(tzinfo=None)
                    except Exception as e:
                        logging.error(e)
            if full_search is True:
                if dt_last_week > updated_at:
                    max_pages = page
                    logging.info("Stopping processing, update complete")
                else:
                    max_pages = r_js["total_pages"]
            page += 1
            logging.info("Progress {}|{} ({})".format(max_pages,page-1,updated_at))

    def parse_overview_data(self, data):
        tags = self.get_tags(data["links"])

        new_project = {
        "id": data["id"],
        "created_at":data["created_at"],
        "updated_at":data["updated_at"],
        "latitude":data["latitude"],
        "longitude":data["longitude"],
        "zip":data["zip"],
        "city":data["city"],
        "country":data["country"],
        "content_updated_at":data["content_updated_at"],
        "activated_at":data["activated_at"],
        "title":data["title"],
        "description":data["description"],
        "summary":data["summary"],
        "tax_deductible":data["tax_deductible"],
        "donations_prohibited":data["donations_prohibited"],
        "completed_at":data["completed_at"],
        "closed_at":data["closed_at"],
        "open_amount_in_cents": data["open_amount_in_cents"],
        "donated_amount_in_cents":data["donated_amount_in_cents"],
        "positive_opinions_count":data["positive_opinions_count"],
        "donations_count":data["donations_count"],
        "negative_opinions_count":data["negative_opinions_count"],
        "comments_count":data["comments_count"],
        "donor_count":data["donor_count"],
        "progress_percentage":data["progress_percentage"],
        "incomplete_need_count":data["incomplete_need_count"],
        "completed_need_count":data["completed_need_count"],
        "carrier_id": (data["carrier"] if data["carrier"] == None else data["carrier"]["id"]),
        "carrier_name":(data["carrier"] if data["carrier"] == None else data["carrier"]["name"]),
        "carrier_city":(data["carrier"] if data["carrier"] == None else data["carrier"]["city"]),
        "active_matching_fund":data["active_matching_fund"],
        "closed_notice":data["closed_notice"],
        "tags": tags
        }
        self.df_projects = self.df_projects.append(new_project, ignore_index=True)


    def parse_overview_data_original(self, data):
        data["carrier_name"] = (data["carrier"] if data["carrier"] == None else data["carrier"]["name"])
        data["contact_name"] = (data["contact"] if data["contact"] == None else data["contact"]["name"])

        data["tags"] = self.get_tags(data["links"])

        data["downloaded_at"] = self.download_time.isoformat()

        self.df_projects = self.df_projects.append(data, ignore_index=True)

    def get_tags(self, links):
        for l in links:
            if l["rel"] == "categories":
                for i in range(5):
                    try:
                        r = requests.get(l["href"], timeout=30)
                        break
                    except:
                        print ("Internet Error: ", i)
                        time.sleep(60)
                if r.status_code ==200:
                    tag = []
                    r_js = r.json()
                    if r_js["total_entries"] > 0:
                        for t in r_js["data"]:
                            tag.append(t["name"])
                else:
                    tag = ["error"]
            else:
                continue
        return tag


    def save_to_excel(self):
        now = datetime.now()
        datetime_now = now.strftime("%Y%m%d_%H%M")
        path = "./Output/"+datetime_now+".xlsx"
        self.df_projects.to_excel(path)

    def save_to_sql_lite(self):
        conn = sqlite3.connect('./Output/betterplace.db')
        df_sql = self.df_projects
        df_sql[["closed_notice","active_matching_fund","carrier", "contact", "links", "profile_picture","tags"]] = df_sql[["closed_notice", "active_matching_fund", "carrier", "contact", "links", "profile_picture","tags"]].astype(str)
        df_sql.to_sql("projects", con=conn, if_exists='append', index=False)
        conn.commit()
        conn.close()

    def save_to_sql(self, table):
        DATABASES = {
            'betterplace':{
                'NAME': os.environ['BETTERPLACE_DB_NAME'],
                'USER': os.environ['BETTERPLACE_DB_USER'],
                'PASSWORD': os.environ['BETTERPLACE_DB_PASSWORD'],
                'HOST': os.environ['BETTERPLACE_DB_HOST'],
                'PORT': os.environ['BETTERPLACE_DB_PORT'],
            },
        }

        # choose the database to use
        db = DATABASES['betterplace']

        # construct an engine connection string
        engine_string = "postgresql://{user}:{password}@{host}:{port}/{database}".format(
            user = db['USER'],
            password = db['PASSWORD'],
            host = db['HOST'],
            port = db['PORT'],
            database = db['NAME'],
        )

        # create sqlalchemy engine
        engine = sqlalchemy.create_engine(engine_string)

        df_sql = self.df_projects
        df_sql["links"] = None
        data_type = {
        'carrier': sqlalchemy.types.JSON,
        'active_matching_fund': sqlalchemy.types.JSON,
        'contact': sqlalchemy.types.JSON,
        'links': sqlalchemy.types.ARRAY(sqlalchemy.types.JSON),
        'profile_picture': sqlalchemy.types.JSON,
        'closed_notice': sqlalchemy.types.JSON,
        'tags': sqlalchemy.types.ARRAY(sqlalchemy.types.String),
        "activated_at": sqlalchemy.types.TIMESTAMP,
        "updated_at": sqlalchemy.types.TIMESTAMP,
        "closed_at": sqlalchemy.types.TIMESTAMP,
        "completed_at": sqlalchemy.types.TIMESTAMP,
        "content_updated_at": sqlalchemy.types.TIMESTAMP,
        "created_at": sqlalchemy.types.TIMESTAMP,
        "downloaded_at":sqlalchemy.types.TIMESTAMP
        }
        df_sql.to_sql(table, con=engine, if_exists='append', index=False, dtype=data_type)
        #conn.commit()
        #conn.close()


if __name__ == '__main__':
    p = ArgumentParser()
    p.add_argument('-a', '--all', action='store_true')
    p.add_argument('-t', '--test', action='store_false')
    args  = p.parse_args()
    update_all = args.all
    full_search_flag = args.test

    betterplace = betterplace()
    try:
        if update_all is True:
            url_all = "https://api.betterplace.org/de/api_v4/projects.json?facets=closed%3Atrue&order=updated_at%3ADESC&per_page={}&page={}"
            betterplace.get_projects(url_all, full_search=full_search_flag)
        else:
            url_update = "https://api.betterplace.org/de/api_v4/projects.json?facets=closed%3Afalse&order=updated_at%3ADESC&per_page={}&page={}"
            betterplace.get_projects(url_update, full_search=full_search_flag)
        logging.info("Saving Files ...")

        if full_search_flag is False:
            betterplace.save_to_sql("projects_vf_backup")
            logging.info("Saved to Backup_Table at: {}".format(datetime.now()))
        else:
            betterplace.save_to_sql("projects_vf")
            logging.info("Saved to DB at: {}".format(datetime.now()))
        logging.info("Downloaded Projects: {}".format(betterplace.total_projects))
        logging.info("Time needed: {}".format(datetime.now() - betterplace.download_time))
    except Exception as e:
        logging.error(e)

    # ETF print ("Time needed: ", ((datetime.now() - betterplace.download_time)/10)*100+datetime.now()+timedelta(hours=1))
