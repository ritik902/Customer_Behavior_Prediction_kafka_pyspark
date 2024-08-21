from kafka import KafkaProducer
import csv

producer = KafkaProducer(bootstrap_servers='kafka:9092')

with open('/data/Reviews.csv', mode='r', encoding='utf-8') as file:
    reader = csv.reader(file)
    for row in reader:
        producer.send('reviews', value=','.join(row).encode('utf-8'))

producer.flush()

print("Data ingested successfully.")
