# -*- coding: utf-8 -*-

#mqtt와 mysql 연동이 가능한 소스파일
#간단한 웹페이지와의 테스트 파일

import pymysql
import paho.mqtt.client as mqtt


def on_connect(client, userdata, flags, rc):
    print("connected with result code " + str(rc))
    client.subscribe("/data/emer")


def on_message(client, userdata, msg):
    data = str(msg.payload)
    data = int(data)
    # MySQL Connection 연결
    conn = pymysql.connect(host='localhost', user='root', password='123456',
                           db='ngn_db', charset='utf8mb4')

    # Connection 으로부터 Cursor 생성
    curs = conn.cursor()

    #if data != 1 and data != 0:
    #    print("Unvalid data")
    #    return;



    # SQL문 실행

    sql = "update lifejacket set EmergencyOn = %d where SN = '12e11'" %data
    #sql = "update lifejacket set EmergencyOn = 33 where SN = '12e11'"

    print (sql)

    result = curs.execute(sql)

    # 이거해줘야됌
    conn.commit()

    # 데이타 Fetch
    #rows = curs.fetchall()
    #print(rows)  # 전체 rows
    # print(rows[0])  # 첫번째 row: (1, '김정수', 1, '서울')
    # print(rows[1])  # 두번째 row: (2, '강수정', 2, '서울')

    # SQL문 실행
    #sql = "insert into  lifejacket(SN,EmergencyOn,warningOn) values ('test', 0, 1)"
    #curs.execute(sql)




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
    client.unsubscribe(["/data/emer"])
    #client.unsubscribe(["room309/temperature", "room309/humidity"])
    client.disconnect()