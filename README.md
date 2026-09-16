# Chess Streaming Pipeline

## Requirements

- Docker Desktop with Docker Compose
- A bash, zsh or PowerShell terminal; the commands do not work in Windows Command Prompt
- At least 16 GB of RAM for the default settings; see [Resource Customization](#resource-customization) to reduce this
- At least 12 logical CPU cores for the default settings; see [Resource Customization](#resource-customization) to reduce this
- Internet access, as data is collected from the chess.com public API and Spark downloads its Kafka connector on the first run

No other packages or datasets are needed. All Python packages are installed inside the Docker images from `requirements.txt`, and the pipeline collects its own data.

## Setup

**1.** Create a `.env` file in the project root:

```
CHESS_API_CONTACT=you@example.com
MONGO_ROOT_USER=admin
MONGO_ROOT_PASS=changeme
MONGO_URI=mongodb://admin:changeme@mongodb:27017/?authSource=admin
```

**2.** Build and start the containers:

```bash
docker compose up -d --build
```

**3.** Create the Kafka topics:

```bash
docker compose exec kafka sh -c 'cd /opt/kafka/bin && ./kafka-topics.sh --bootstrap-server kafka:9092 --create --topic players.raw; ./kafka-topics.sh --bootstrap-server kafka:9092 --create --topic games.raw'
```

## Running

**1.** Start the processing job:

```bash
docker compose exec -d python spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 --master spark://spark-master:7077 --total-executor-cores 2 --driver-memory 4g processing/processor.py
```

**2.** Once the processing job shows as running at http://localhost:8080, start the evaluation job. Starting both jobs at the same time makes them fail on the first run.

```bash
docker compose exec -d python spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 --master spark://spark-master:7077 --total-executor-cores 10 processing/evaluator.py
```

**3.** Collect players, giving comma-separated country codes (2 letters) and the number of players per country:

```bash
docker compose exec python python -m ingestion.collect_players NO,SE,DK 10
```

**4.** Collect games, giving the number of players to collect games for:

```bash
docker compose exec python python -m ingestion.collect_games 1
```

Up to 20 games are collected for each player, and every collected game is then evaluated move by move with Stockfish, which is slow. For testing, keep this number small.

Results are written to MongoDB on `localhost:27017`, in the database `chess_db` with the collections `chess_players`, `chess_games` and `chess_move_evals`.

## Stopping

```bash
docker compose stop
```

## Resource Customization

By default Spark uses up to 12 CPU cores and 12 GB of memory: two workers with 6 cores and 4 GB each, plus a 4 GB driver for the processing job. To use less:

- In `docker-compose.yaml`, before starting the containers, lower `SPARK_WORKER_CORES` and `SPARK_WORKER_MEMORY`, which apply to each of the two workers. Keep `SPARK_WORKER_MEMORY` at `2g` or more so each worker can run both jobs.
- In the two `spark-submit` commands, keep the `--total-executor-cores` values adding up to no more than twice `SPARK_WORKER_CORES`, and lower or remove `--driver-memory`.

For example, `SPARK_WORKER_CORES=2` with `--total-executor-cores 1` for the processing job and `3` for the evaluation job. Fewer cores make the Stockfish evaluation slower.