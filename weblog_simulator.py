"""
Weblog Simulator
"""
from datetime import datetime
from time import sleep, time, ctime
from random import random, randint, choice
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

TOPIC = 'weblog'

def get_weblog_msg():
    datevalue = (datetime.now()).strftime("%Y/%m/%d %H:%M:%S")
    ipaddress = "100."+str(randint(128,132))+"."+str(choice([0,2]))+".1"
    master = "nhmasternode"
    slave = "nhslave"+choice(['1br','2br','3br','4br'])+choice(['1vm','2vm','3vm'])+str(choice([32,64]))
    host = choice([master, slave])
    request = choice(['POST','GET','POST','PUT','GET','PUT','POST','PUT','GET','GET','PUT','GET'])
    link = choice(["ccs/font-awesome.min.css","/process.php","/details.php?id=99","js/chart.min.js","/home.php",
	       "ccs/bootstrap.min.css","/login.php","js/vendor/modernizr-2.8.3.min.js",
	       "js/vendor/jquery-1.0.2.min.js","/description.php?id=55","/pcompile.php"])
    extn = " HTTP/1.1"
    url = request+" "+link+extn
    responsecode = str(choice([200,302,304,200]))
    data = f"{datevalue},{ipaddress},{host},{url},{responsecode}"
    return data

for _ in range(30):
    message = get_weblog_msg()
    message = bytearray(message.encode("utf-8"))
    print(message)
    producer.send(TOPIC, message)
    sleep(random() * 10)
