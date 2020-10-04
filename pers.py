import paho.mqtt.client as mqtt
import json
import datetime
import sqlite3
from sqlite3 import Error

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn

    

filename ="/root/grootbe/db.sqlite3"
conn = create_connection(filename)
cur = conn.cursor()


#ON_CONNECT 
def on_connect(mqttc, obj, flags, rc):
    print("rc: " + str(rc))

# ON_MESSAGE 
# Example: 
#def on_message(mqttc, obj, msg):
#    print(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))

def on_message(mqttc, obj, msg):
#TO INSERT FOR A PARTICULAR TOPIC BASED ON DB 
    if msg.topic =='test':
        print("sscsdd",msg.payload.decode("utf-8"))
        d=json.loads(msg.payload.decode("utf-8"))
        # ts = datetime.datetime.now()
        setup = d.get("setup")
        temp = d.get("temp")
        humidity = d.get("humidity")
        gas = d.get("gas")
        ec = d.get("ec")
        ph = d.get("ph")
        # ts = datetime.datetime.now()
        #sql = "INSERT INTO api_sensorsval(id,setup,temp,humidity,gas,ec,ph) VALUES (NULL,%s,%s, %s,%s,%s,%s)"
        sql = "INSERT INTO api_sensorsval VALUES (NULL,?,?,?,?,?,?)"
        val = (ec,gas,humidity,ph,temp,setup)
        print("sql is ",sql)
        print("val is ",val)
        cur.execute(sql, val)
        conn.commit()
        print(cur.rowcount, "record inserted.")
    else:
        print(msg.payload.decode("utf-8"))
 #INSERT INTO api_sensorsval VALUES (NULL, 10.0, 10.0, 10.0, 10.0, 10.0,1)

# ON_PUBLISH 
def on_publish(mqttc, obj, mid):
    print("mid: " + str(mid))

#ON_ SUBSCRIBE
def on_subscribe(mqttc, obj, mid, granted_qos):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))
#ON_LOG
def on_log(mqttc, obj, level, string):
    print(string)



# If you want to use a specific client id, use
# mqttc = mqtt.Client("client-id")
# but note that the client id must be unique on the broker. Leaving the client
# id parameter empty will generate a random id for you.
mqttc = mqtt.Client()
mqttc.on_message = on_message
mqttc.on_connect = on_connect
mqttc.on_publish = on_publish
mqttc.on_subscribe = on_subscribe
# Uncomment to enable debug messages
# mqttc.on_log = on_log
mqttc.connect("localhost", 1883, 60)
mqttc.subscribe("test", 0)

mqttc.loop_forever()