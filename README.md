# Kafka + Spark Structured Streaming Example

Project ini merupakan contoh pipeline **stream processing** menggunakan:

* **Apache Kafka** sebagai message broker
* **Apache Spark Structured Streaming** untuk memproses data secara real-time
* **Python Producer** untuk mengirim data ke Kafka

Pipeline alurnya:

Producer → Kafka Topic → Spark Streaming → Aggregation → Console Output

---

# 1. Prerequisites

Pastikan sudah terinstall:

* Docker
* Docker Compose
* Python 3
* pip

Python library yang dibutuhkan:

```
pip install confluent-kafka
```

---

# 2. Menjalankan Infrastruktur

Jalankan semua service menggunakan Docker Compose.

```
docker compose up -d
```

Service yang akan berjalan:

* Zookeeper
* Kafka
* Kafka UI
* Spark Master
* Spark Worker

Cek container yang berjalan:

```
docker ps
```

---

# 3. Membuat Kafka Topic

Masuk ke container Kafka:

```
docker exec -it kafka bash
```

Buat topic baru:

```
docker exec kafka kafka-topics --create --topic transactions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

docker exec kafka kafka-topics --create --topic transactions_valid --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

docker exec kafka kafka-topics --create --topic transactions_dlq --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

Cek topic:

```
kafka-topics --list --bootstrap-server localhost:9092
```

---

# 4. Menjalankan Producer

Producer bertugas mengirim data transaksi ke Kafka.

Start container kafka producer maka producer.py otomatis berjalan.

Producer akan mengirim data seperti:

```
{
  "user_id": 12,
  "amount": 45000,
  "event_time": "2026-03-06T10:12:01"
}
```

Data akan terus dikirim ke topic **transactions**.

---

# 5. Menjalankan Spark Streaming

Masuk ke container Spark:

```
docker exec -it spark-master bash
```

Jalankan script Spark:

```
/opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
--conf spark.jars.ivy=/tmp/.ivy2 \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
/opt/spark-apps/spark_streaming_job.py
```

Spark akan membaca data dari Kafka lalu melakukan agregasi:

* Window: 1 menit
* Aggregation: SUM(amount)

---

# 6. Output Streaming

Hasil streaming akan tampil di console seperti berikut:

```
-------------------------------------------
Batch: 16
-------------------------------------------
+---------------------+-------------+
|timestamp            |running_total|
+---------------------+-------------+
|2026-03-06 07:08:01  |7118909      |
|2026-03-06 07:08:01  |318676       |
|2026-03-06 07:08:01  |4448013      |
+---------------------+-------------+
```

Penjelasan:

* **Batch** → micro-batch Spark Streaming
* **timestamp** → waktu saat batch diproses
* **running_total** → total amount hasil agregasi

---

# 7. Melihat Log Streaming

Untuk melihat log Spark:

```
docker logs spark-master
```

Untuk mengikuti log secara realtime:

```
docker logs -f spark-master
```

---

# 8. Menghentikan Sistem

Untuk menghentikan semua container:

```
docker compose down
```

---

# 9. Arsitektur Sistem

```
Python Producer
      │
      ▼
   Kafka Topic
      │
      ▼
Spark Structured Streaming
      │
      ▼
Aggregation (Window 1 minute)
      │
      ▼
Console Output
```

---

# 10. Catatan

Jika output menampilkan banyak baris dengan timestamp yang sama, itu karena:

* Spark menggunakan **window aggregation**
* Setiap window menghasilkan **1 hasil agregasi**
* `outputMode("complete")` akan menampilkan seluruh hasil agregasi setiap batch.
