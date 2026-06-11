# DE-E2E Orchestration

Apache Airflow chạy bằng Docker Compose trong container riêng để điều phối pipeline.

## Thành phần

- `airflow-postgres`: metadata database cho Airflow.
- `airflow-init`: migrate DB và tạo user admin.
- `airflow-api-server`: UI/API tại `http://localhost:8080`.
- irflow-scheduler: lập lịch DAG bằng LocalExecutor.
- irflow-dag-processor: parse DAG riêng theo kiến trúc Airflow 3.
- `ingestion-worker`: container code Python, cung cấp JSON qua `GET /data` để Airflow lấy và chuyển lên cloud.

## Dockerfile boundaries

- `airflow/Dockerfile`: Airflow runtime và cấu hình mặc định cho scheduler/webserver/init.
- `ingestion-worker/Dockerfile`: Python runtime, dependencies và app command của ingestion worker.
- `docker-compose.yml`: nối service, network, ports, volumes, healthcheck và runtime secrets.

Không đưa `CLOUD_API_KEY` vào Dockerfile vì token sẽ bị bake vào image. Compose inject biến này lúc chạy.
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

## Douyin ingestion

`ingestion-worker` đã port phần gọi dữ liệu từ `datnndd/douyin-download`, gồm endpoint registry, `a_bogus`, `X-Bogus`, và chỉ giữ fetch JSON thô:

- Bỏ chuẩn hóa dữ liệu.
- Bỏ download video/music/cover/avatar.
- Bỏ database incremental.
- Bỏ n8n workflow.

Module chính:

- ingestion-worker/douyin/urls.py: lưu endpoint Douyin.
- ingestion-worker/douyin/abogus.py: thuật toán _bogus.
- ingestion-worker/douyin/xbogus.py: thuật toán X-Bogus.
- ingestion-worker/douyin_client.py: client gọi API raw.

Cấu hình trong `.env`:

```powershell
DOUYIN_LINK=https://www.douyin.com/user/...
DOUYIN_COOKIE="msToken=...; ttwid=...; odin_tt=...; passport_csrf_token=...; sid_guard=...;"
DOUYIN_MODE=post
DOUYIN_LIMIT=20
```

Endpoint worker:

- `GET /data`: Airflow gọi endpoint này để lấy JSON theo `.env`.
- `POST /douyin/fetch`: gọi thủ công với body `{"link":"...","mode":"post","limit":20}`.
## Cloud ingest

Cấu hình trong `.env`:

```powershell
CLOUD_INGEST_URL=https://your-cloud-endpoint.example.com/ingest
CLOUD_API_KEY=your-token
```

Nếu `CLOUD_INGEST_URL` trống, DAG chuyển cloud sẽ skip task gửi dữ liệu.

## DAGs

Đặt DAG tại `airflow/dags`.

- `orchestration_healthcheck`: kiểm tra scheduler và task runtime.
- `ingestion_worker_to_cloud_transfer`: lấy JSON từ `ingestion-worker`, rồi `POST` lên `CLOUD_INGEST_URL`.

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
