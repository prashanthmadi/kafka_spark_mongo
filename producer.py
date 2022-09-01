from confluent_kafka import Producer
import sys
import json

if __name__ == '__main__':
    if len(sys.argv) != 2:
        sys.stderr.write('Usage: %s <topic>\n' % sys.argv[0])
        sys.exit(1)
    topic = sys.argv[1]

    conf = {
        'bootstrap.servers': 'prmongoeh.servicebus.windows.net:9093', #replace
        'security.protocol': 'SASL_SSL',
        'ssl.ca.location': '/usr/lib/ssl/certs/ca-certificates.crt',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': '$ConnectionString',
        'sasl.password': 'Endpoint=sb://prmongoeh.servicebus.windows.net/;SharedAccessKeyName=t2;SharedAccessKey=mgS8U4GgXdsp8HXD00FDQH3PM6me9UnYI0fO0/LrD7I=;EntityPath=testhub2',          #replace
        'client.id': 'python-example-producer'
    }

    p = Producer(**conf)

    def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d] @ %o\n' % (msg.topic(), msg.partition(), msg.offset()))

    for i in range(100, 200):
        try:
            p.produce(topic,json.dumps({"count": str(i)}), callback=delivery_callback)
        except BufferError as e:
            sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' % len(p))
        p.poll(0)

    # Wait until all messages have been delivered
    sys.stderr.write('%% Waiting for %d deliveries\n' % len(p))
    p.flush()