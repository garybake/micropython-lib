from uqmtt import MQTTClient

c = MQTTClient("mcli", "localhost")
c.connect()
c.publish(b"foo", b"hello")
c.subscribe(b"foo")
while 1:
    #print(c.wait_msg())
    print(c.check_msg())
    time.sleep(1)
c.disconnect()
