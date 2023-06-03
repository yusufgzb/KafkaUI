from kafka import KafkaProducer
import time

# Kafka sunucusu bilgilerini ayarlayın
bootstrap_servers = 'localhost:9092'

# Kafka producer'ı oluşturun
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Mesajları Kafka topiğine göndermek için bir for döngüsü kullanın
for i in range(1, 200):
    message = f"Sayı: {i}"
    producer.send('ornek', value=message.encode('utf-8'))
    print(f"Mesaj gönderildi: {message}")
    time.sleep(1)  # Her mesaj arasında 0.1 saniye bekleme

# Kafka producer'ı kapatın
producer.close()
