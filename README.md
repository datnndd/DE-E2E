# DE-E2E Orchestration

Apache Airflow chạy bằng Docker Compose trong container riêng để điều phối pipeline.

## Thành phần

- `airflow-postgres`: metadata database cho Airflow.
- `airflow-init`: migrate DB và tạo user admin.
- `airflow-api-server`: UI/API tại `http://localhost:8080`.
- `airflow-scheduler`: lập lịch DAG bằng `LocalExecutor`.
- `airflow-dag-processor`: parse DAG riêng theo kiến trúc Airflow 3.
- `ingestion-worker`: container code Python, cung cấp JSON qua `GET /data` để Airflow lấy và ghi S3.

## Dockerfile boundaries

- `airflow/Dockerfile`: Airflow runtime và cấu hình mặc định cho scheduler/api-server/dag-processor/init.
- `ingestion-worker/Dockerfile`: Python runtime, dependencies và app command của ingestion worker.
- `docker-compose.yml`: nối service, network, ports, volumes, healthcheck và runtime secrets.

Không đưa `AWS_SECRET_ACCESS_KEY` hoặc `DOUYIN_COOKIE` vào Dockerfile vì secret sẽ bị bake vào image. Compose inject biến này lúc chạy.

## Khởi chạy

```powershell
Copy-Item .env.example .env
docker compose up airflow-init
docker compose up -d ingestion-worker airflow-api-server airflow-scheduler airflow-dag-processor
```

Đăng nhập Airflow UI:

- URL: `http://localhost:8080`
- Username: `airflow`
- Password: `airflow`

## Ingestion Worker

Worker API chạy tại:

- Nội bộ Docker network: `http://ingestion-worker:8000`
- Máy host: `http://localhost:8000`

Endpoint:

- `GET /health`: kiểm tra service.
- `GET /data`: trả JSON cho Airflow.
- `POST /douyin/fetch`: gọi thủ công với body `{"link":"...","mode":"post","limit":20}`.

## Douyin ingestion

`ingestion-worker` đã port phần gọi dữ liệu từ `datnndd/douyin-download`, gồm endpoint registry, `a_bogus`, `X-Bogus`, và chỉ giữ fetch JSON thô:

- Bỏ chuẩn hóa dữ liệu.
- Bỏ download video/music/cover/avatar.
- Bỏ database incremental.
- Bỏ n8n workflow.

Module chính:

- `ingestion-worker/douyin/urls.py`: lưu endpoint Douyin.
- `ingestion-worker/douyin/abogus.py`: thuật toán `a_bogus`.
- `ingestion-worker/douyin/xbogus.py`: thuật toán `X-Bogus`.
- `ingestion-worker/douyin_client.py`: client gọi API raw.

Cấu hình trong `.env`:

```powershell
DOUYIN_LINK=https://www.douyin.com/user/...
DOUYIN_COOKIE="msToken=...; ttwid=...; odin_tt=...; passport_csrf_token=...; sid_guard=...;"
DOUYIN_MODE=post
DOUYIN_LIMIT=20
```

## AWS S3 Data Lake

Cấu hình trong `.env`:

```powershell
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_DEFAULT_REGION=ap-southeast-1
S3_BUCKET=your-lakehouse-bucket
S3_LANDING_PREFIX=landing/douyin/api_raw/json
```

Nếu `S3_BUCKET` trống, DAG ghi S3 sẽ skip task lưu dữ liệu.

Landing raw path:

```text
s3://your-lakehouse-bucket/landing/douyin/api_raw/json/year=YYYY/month=MM/day=DD/{run_id}.json
```

## Lakehouse Layers

- `landing/`: dữ liệu gốc từ API, giữ nguyên JSON để audit/debug/replay.
- `bronze/`: dữ liệu đã đọc từ landing, ép schema nhẹ, lưu Parquet.
- `silver/`: dữ liệu clean/deduplicate/conformed.
- `gold/`: bảng phục vụ dashboard/analytics.

Flow hiện tại:

```text
Douyin API -> ingestion-worker -> Airflow -> S3 landing JSON
```

Flow bước sau:

```text
S3 landing JSON -> Spark -> S3 bronze Parquet
```
## DAGs

Đặt DAG tại `airflow/dags`.

- `orchestration_healthcheck`: kiểm tra scheduler và task runtime.
- `ingestion_worker_to_s3_landing`: lấy JSON gốc từ `ingestion-worker`, rồi ghi vào AWS S3 Landing.

## Lệnh hữu ích

```powershell
docker compose ps
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-api-server
docker compose down
```

Xóa cả metadata database khi cần reset sạch:

```powershell
docker compose down -v
```