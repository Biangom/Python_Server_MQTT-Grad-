# -*- coding: utf-8 -*-
import pymysql
import paho.mqtt.client as mqtt


def on_connect(client, userdata, flags, rc):
    print("connected with result code " + str(rc))
    client.subscribe("data/temper")


def on_message(client, userdata, msg):
    data = str(msg.payload)
    # MySQL Connection 연결
    conn = pymysql.connect(host='localhost', user='root', password='123456',
                           db='ngn_db', charset='utf8')

    # Connection 으로부터 Cursor 생성
    curs = conn.cursor()

    # SQL문 실행
    sql = """ update lifejacket set EmergencyOn=55 where SN='sds1'""";

#    sql = """update lifejacket set EmergencyOn="""+data+""" where SN='sds1'""";
    print (sql)

    result = curs.execute(sql)

    # 데이타 Fetch
    #rows = curs.fetchall()
    #print(rows)  # 전체 rows
    # print(rows[0])  # 첫번째 row: (1, '김정수', 1, '서울')
    # print(rows[1])  # 두번째 row: (2, '강수정', 2, '서울')

    # SQL문 실행
    sql = "insert into  lifejacket(SN,EmergencyOn,warningOn) values ('test', 0, 1)"
    curs.execute(sql)




    # Connection 닫기
    conn.close()
    print(result)
    print("Topic: " + msg.topic + " Message: " + str(msg.payload))


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.username_pw_set("zqgmlwjg","uQietis53KPr")
client.connect("m12.cloudmqtt.com", 13288)

try:
    client.loop_forever()
except KeyboardInterrupt:
    print("Finished!")
    client.unsubscribe(["data/temper"])
    #client.unsubscribe(["room309/temperature", "room309/humidity"])
    client.disconnect()