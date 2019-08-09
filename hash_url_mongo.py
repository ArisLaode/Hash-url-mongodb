
from flask_script import Manager
from flask import Flask, request, jsonify
from PIL import Image
from flask_pymongo import PyMongo, pymongo
from datetime import datetime
from logging import FileHandler, WARNING
from urllib.request import urlopen
import urllib.request
import io
import imagehash
import json
import certifi
import time

app = Flask(__name__)

manager = Manager(app)

app.config['MONGO_DBNAME'] = 'news'
app.config['MONGO_URI'] = 'mongodb://192.168.20.189:27017/news'
mongo = PyMongo(app)

app.config['MONGO_DBNAME'] = 'online_news'
app.config['MONGO_URI'] = 'mongodb://192.168.20.189:27017/online_news'
mongo_insert = PyMongo(app)

@manager.command
def hash_url():
        dict_date = []

        fo = open('date.txt', 'r')
        doc = fo.readlines()
        fo.close()
    
        for date_url in doc:

            i = date_url.strip()
            dict_date.append(i)
    
        st_date = dict_date[0]    
        en_date = dict_date[1]

        start = datetime.strptime(st_date, '%Y-%m-%d %H:%M:%S')
        end = datetime.strptime(en_date, '%Y-%m-%d %H:%M:%S')
    
        process = 0
        
        #Query to collection online_news_result
        online_news_result = mongo.db.online_news_result
        
        while True:
                try:
                        hash_mongo = online_news_result.find({"pubDate": {"$gte": start, "$lt": end}}, no_cursor_timeout=True).skip(process)
                        
                        for i in hash_mongo:
                                
                                id_image = i['_id']
                                url_image = i['url']
                                pubDate = i['pubDate']
                                
                                if url_image is None:
                                        continue
                                
                                try:
                                        opener = urllib.request.build_opener()
                                        opener.addheaders = [('User-agent', 'Mozilla/5.0 (Windows NT 5.1; rv:43.0) Gecko/20100101 Firefox/43.0')]
                                        urllib.request.install_opener(opener)
                                        resp = urllib.request.urlopen(url_image, timeout=10, cafile=certifi.where())
                                        
                                        image_file = io.BytesIO(resp.read())
                                        image = Image.open(image_file)
                                        value_hash = imagehash.phash(image, hash_size=8)
                                        img_hash = str(value_hash)

                                        hash_url ={
                                                '_id' : id_image,
                                                'hash' : img_hash,
                                                'pubdate' : pubDate,
                                                'url' : url_image
                                        }

                                        print(hash_url)

                                        news_hash_05_2019 = mongo_insert.db.news_hash_05_2019
                                        
                                        insert_data = news_hash_05_2019.insert(hash_url)
                                        
                                        
                                
                                except Exception as e:
                                        print(e)
                                        time.sleep(10)
                                        pass
                        
                        hash_mongo.close()
                except CursorNotFound as ec:
                        print(ec)
                        time.sleep(10)
                        pass
                
                print('Data Selesai diproses')
    
if __name__ == '__main__':
    manager.run()