import paho.mqtt.client as mqtt
import time
import json
import numpy as np
import datetime


def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe("test")


def on_disconnect(client, userdata, rc):
    print("Disconnected with result code " + str(rc))


def on_message(client, userdata, msg):
    print(
        msg.topic,
        str(msg.payload),
        "retain",
        msg.retain,
        "qos",
        msg.qos,
        str(userdata),
    )


client = mqtt.Client()

client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect

f = open("config.json")
config = json.loads(f.read())
f.close()

device_config = []
for devices in config["devices"]:
    for n in range(devices["device_count"]):
        dev = {}
        dev["device_id"] = devices["type"] + "_" + str(n)
        dev["device_type"] = devices["type"]
        dev["publish_frequency"] = devices["publish_frequency"]
        dev["std_val"] = devices["std_val"]
        dev["publish_topic"] = devices["publish_topic"]
        device_config.append(dev)

client.connect(
    host=config["broker_host"], port=config["broker_port"], keepalive=60
)

client.loop_start()

clock = 0
while True:
    try:
        time.sleep(1)
        clock = clock + 1
        for devices in device_config:
            if clock % devices["publish_frequency"] == 0:
                print("Published to devices/" + devices["device_type"])
                message = {}
                message["timestamp"] = datetime.datetime.now().strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                message["device_id"] = devices["device_id"]
                message["device_type"] = devices["device_type"]
                message["value"] = round(
                    np.random.normal(devices["std_val"], 2), 2
                )
                client.publish(
                    devices["publish_topic"], json.dumps(message)
                )
    except KeyboardInterrupt:
        client.disconnect()
        client.loop_stop()
        break
