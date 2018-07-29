import paho.mqtt.client as mqtt


def on_connect(client, userdata, flags, rc):
    print("connected with result code " + str(rc))
    client.subscribe("data/temper")


def on_message(client, userdata, msg):
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