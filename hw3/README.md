
## Block 1

Для запуска producer и consumer, используйте

`python consumer_1.py`

`python producer_1.py`

Для запуска flink job c сохранением checkpoints локально, используйте

`docker compose exec jobmanager ./bin/flink run -py /opt/pyflink/device_job_local_dir.py -d`

Поднял в docker-compose hdfs, пытался сохранять checkpoints в hdfs, но не смог побороть ошибку:

"*Caused by: org.apache.flink.core.fs.UnsupportedFileSystemSchemeException: Could not find a file system implementation for scheme 'hdfs'. The scheme is not directly supported by Flink and no Hadoop file system to support this scheme could be loaded. For a full list of supported file systems, please see https://nightlies.apache.org/flink/flink-docs-stable/ops/filesystems/*"

## Block 2

Для запуска flink job с разными Flink Windows, используйте

`docker compose exec jobmanager ./bin/flink run -py /opt/pyflink/device_job_tumbling.py -d`

`docker compose exec jobmanager ./bin/flink run -py /opt/pyflink/device_job2_sliding.py -d`

`docker compose exec jobmanager ./bin/flink run -py /opt/pyflink/device_job3_session.py -d`

## Block 3

Для запуска consumer c backoff-механизмом, используйте

`python consumer_with_backoff.py`

В функции message_handler эмулируется ошибка деления на 0
