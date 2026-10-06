# 🚚 Fleet Radar — Realtime Distributed Fleet Tracking System

> **Distributed event-driven fleet telemetry platform** built with Go microservices, Apache Kafka, and a Next.js realtime command center dashboard.
> Designed to track, monitor, and analyze vehicle fleets in real-time — from GPS ingestion to geofence breach alerting — with sub-second latency at scale.

---

## 📋 Overview

**Fleet Radar** adalah sistem pelacakan armada kendaraan (*fleet tracking*) berbasis **arsitektur event-driven** yang dibangun menggunakan **Go microservices** dan **Apache Kafka** sebagai message broker terpusat.

Sistem ini dirancang untuk menjawab satu permasalahan nyata di industri logistik dan transportasi:

> *"Bagaimana sebuah platform dapat menerima jutaan data posisi GPS dari ribuan kendaraan secara bersamaan, memproses setiap event secara real-time, mendeteksi pelanggaran wilayah (geofence), dan menyajikan semua informasi tersebut ke operator armada — tanpa sistem kolaps?"*

**Fleet Radar** menjawab tantangan tersebut dengan memisahkan tanggung jawab setiap komponen secara ketat, menggunakan pola **CQRS (Command-Query Responsibility Segregation)** dan **event streaming**, sehingga sistem tetap responsif dan elastis bahkan di bawah beban tinggi.

---

## ✨ Key Features

| Feature | Detail |
|---|---|
| 🛰️ **High-Throughput GPS Ingestion** | Bidirectional gRPC streaming dari kendaraan/perangkat GPS ke message broker |
| 📨 **Kafka Event Streaming** | Decoupled ingestion dari processing, buffer ribuan event per detik |
| 📍 **Realtime Location Tracking** | Sub-second telemetry update via WebSocket ke dashboard operator |
| 🛡️ **Geofence Engine** | Lingkaran (Haversine) + Poligon arbitrer (Ray-Casting) dengan deteksi ENTER/EXIT |
| ⚡ **Dual-Tier Storage** | Redis (< 5ms last-position) + MySQL (historical audit trail) |
| 🗺️ **Live Command Center Dashboard** | Peta interaktif Next.js dengan marker rotasi, breadcrumb trail, alert real-time |
| 🔐 **Auth Middleware** | Bearer token API key pada gRPC interceptor (stream & unary) |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                       Fleet Radar System                        │
│                                                                 │
│  [GPS Device / Mobile App / Simulator]                          │
│           │                                                     │
│           │  gRPC Bidirectional Streaming                       │
│           ▼                                                     │
│  ┌─────────────────┐                                            │
│  │ Ingestion Svc   │  ← Bearer Token Auth (gRPC Interceptor)    │
│  │   (port 50051)  │    Async Kafka AsyncProducer               │
│  └────────┬────────┘                                            │
│           │  Publish → "location.events"                        │
│           ▼                                                     │
│  ┌─────────────────┐                                            │
│  │  Apache Kafka   │  KRaft mode (no Zookeeper)                 │
│  │   (port 9092)   │                                            │
│  └────────┬────────┘                                            │
│           │  Consumer Group                                     │
│           ▼                                                     │
│  ┌───────────────────────────────────────────────────────┐      │
│  │              Tracking Service (port 8080)              │      │
│  │                                                       │      │
│  │  Kafka Consumer → LocationService                     │      │
│  │       ├─ Redis GEOADD  (last position, < 5ms)        │      │
│  │       ├─ MySQL INSERT  (location history)             │      │
│  │       ├─ GeofenceEngine                               │      │
│  │       │      ├─ Haversine (circle zones)             │      │
│  │       │      └─ Ray-Casting (polygon zones)          │      │
│  │       └─ WebSocket Hub Broadcast                     │      │
│  │                                                       │      │
│  │  REST: /drivers  /geofences  /dashboard/stats  /ws   │      │
│  └──────────────────────┬────────────────────────────────┘      │
│                         │  WebSocket + REST API                  │
│                         ▼                                        │
│  ┌───────────────────────────────────────────────────────┐      │
│  │       Next.js 14 Dashboard (port 3000)                │      │
│  │                                                       │      │
│  │  ┌──────────┐  ┌────────────┐  ┌──────────────────┐  │      │
│  │  │FleetMap  │  │  Sidebar   │  │  Alert Drawer    │  │      │
│  │  │(Leaflet) │  │(Telemetry) │  │(Geofence Breach) │  │      │
│  │  └──────────┘  └────────────┘  └──────────────────┘  │      │
│  └───────────────────────────────────────────────────────┘      │
└─────────────────────────────────────────────────────────────────┘
```

### Data Flow (Per GPS Event)

```
GPS Device
    └─► gRPC Stream ─► Ingestion Svc ─► Kafka ─► Tracking Svc ──► Redis       (hot: last position)
                                                               ──► MySQL       (cold: history)
                                                               ──► Geofence Engine ─► WS Alert
                                                               ──► WebSocket Hub ─► Dashboard
```

---

## 🛠️ Tech Stack

### Backend (Go 1.26+)

| Layer | Technology | Peran |
|---|---|---|
| **Transport** | `gRPC` + Protocol Buffers | High-throughput bidirectional streaming dari perangkat GPS |
| **Message Broker** | `Apache Kafka` (KRaft, no Zookeeper) | Event buffer & decoupler antara ingestion dan tracking |
| **HTTP Framework** | `Gin` | REST API untuk drivers, geofences, dashboard, health |
| **ORM / DB** | `GORM` + MySQL 8 | Persistent historical route data & geofence definitions |
| **Cache** | `Redis` + `go-redis/v9` | Last-known position (< 5ms latency) + Geospatial index |
| **WebSocket** | `gorilla/websocket` | Realtime push ke frontend dashboard |
| **Auth** | Bearer Token gRPC Interceptor | Stream & unary auth middleware pada Ingestion service |

### Frontend (Next.js 14)

| Layer | Technology | Peran |
|---|---|---|
| **Framework** | `Next.js 14` (App Router) + TypeScript | Dashboard SPA |
| **Map** | `Leaflet` | Peta interaktif dengan marker kustom & geofence overlay |
| **Styling** | `Tailwind CSS` | Dark glassmorphism command center UI |
| **Realtime** | Native `WebSocket` + auto-reconnect | Live telemetry stream dari Tracking Service |
| **Icons** | `Lucide React` | HUD icons |

### Infrastructure

| Komponen | Teknologi |
|---|---|
| **Database** | MySQL 8 |
| **Cache / Geo** | Redis 7 |
| **Message Broker** | Apache Kafka (KRaft mode) |
| **Containerization** | Docker Compose |

---

## 📁 Project Structure

```
Fleet-Tracking/                          ← Monorepo Root
│
├── ingestion/                           ← Microservice 1: GPS Ingestion (gRPC)
│   ├── config/                          ← Env config loader
│   ├── handler/                         ← gRPC LocationService handler + auth interceptor
│   ├── kafka/                           ← Async Kafka producer
│   └── main.go                          ← gRPC server entrypoint (port 50051)
│
├── tracking/                            ← Microservice 2: Core Tracking Engine
│   ├── config/                          ← DB, Redis, Kafka, App config
│   ├── controllers/                     ← REST HTTP handlers (Gin)
│   ├── dto/                             ← Request/Response DTOs
│   ├── kafka/                           ← Kafka consumer
│   ├── models/                          ← GORM models + WebSocket event structs
│   ├── repositories/                    ← Data layer interfaces & implementations
│   ├── routes/                          ← Gin route registration
│   ├── services/                        ← Business logic (LocationService, GeofenceService)
│   ├── utils/                           ← Error handling, response helpers
│   ├── websocket/                       ← Hub, broadcaster, WS upgrade handler
│   └── main.go                          ← HTTP server entrypoint (port 8080)
│
├── simulator/                           ← GPS Simulator (dev/demo only)
│   └── main.go                          ← 3 virtual drivers di Kota Medan
│
├── proto/                               ← Protocol Buffer schema
│   └── location.proto                   ← LocationRequest, LocationAck, LocationService
│
├── frontend/                            ← Next.js 14 Dashboard
│   └── src/
│       ├── app/page.tsx                 ← Orchestrator: state, WebSocket, data fetch
│       ├── components/
│       │   ├── Header/Navbar.tsx        ← Telemetry HUD + Map style switcher
│       │   ├── Map/FleetMap.tsx         ← Leaflet map, animated markers, trails, geofences
│       │   ├── Sidebar/FleetSidebar.tsx ← Driver list + realtime telemetry cards
│       │   └── Alerts/AlertToastDrawer.tsx ← Geofence breach realtime feed
│       ├── lib/api.ts                   ← REST API fetch helpers
│       └── types/index.ts               ← TypeScript model interfaces
│
├── docker-compose.yml                   ← MySQL, Redis, Kafka stack
├── go.mod                               ← Go module (shared monorepo)
└── .env.example                         ← Environment variable template
```

---

## 🚀 Getting Started

### Prerequisites

- Go 1.22+
- Node.js 18+
- Docker Desktop (untuk MySQL, Redis, Kafka)
- Git

### 1. Clone Repository

```bash
git clone https://github.com/Tento03/fleet-tracking.git
cd fleet-tracking
```

### 2. Setup Environment Variables

```bash
cp .env.example .env
# Edit .env sesuai konfigurasi lokal Anda
```

### 3. Jalankan Infrastructure

```bash
docker-compose up -d
```

> **Windows tanpa Docker** — Jalankan Kafka secara standalone:
> ```powershell
> # Format storage (hanya pertama kali)
> .\bin\windows\kafka-storage.bat format -t (.\bin\windows\kafka-storage.bat random-uuid) -c .\config\server.properties --standalone
>
> # Start Kafka
> .\bin\windows\kafka-server-start.bat .\config\server.properties
> ```

### 4. Buat Kafka Topic

```bash
.\bin\windows\kafka-topics.bat --create --topic location.events `
  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### 5. Jalankan Backend Services

```bash
# Terminal 1 — Ingestion Service (gRPC :50051)
cd ingestion && go run .

# Terminal 2 — Tracking Service (REST/WS :8080)
cd tracking && go run .
```

### 6. Jalankan Simulator (opsional)

```bash
# Build & run GPS simulator (3 virtual drivers)
go build -o simulator.exe ./simulator && .\simulator.exe
```

### 7. Jalankan Frontend Dashboard

```bash
cd frontend
npm install
npm run dev
```

Dashboard tersedia di: **[http://localhost:3000](http://localhost:3000)**

---

## 🗺️ API Reference

### Drivers

| Method | Endpoint | Deskripsi |
|---|---|---|
| `POST` | `/drivers` | Register driver baru (idempotent by `code`) |
| `GET` | `/drivers` | List semua driver |
| `GET` | `/drivers/active` | Driver aktif + posisi terakhir dari Redis |
| `GET` | `/drivers/:id` | Detail driver by UUID |
| `PATCH` | `/drivers/:id/status` | Update status driver |
| `GET` | `/drivers/:id/location` | Posisi terakhir driver |
| `GET` | `/drivers/:id/history` | Riwayat lokasi (time-series) |

### Geofences

| Method | Endpoint | Deskripsi |
|---|---|---|
| `POST` | `/geofences` | Buat zona geofence (circle atau polygon) |
| `GET` | `/geofences` | List semua geofence aktif |
| `GET` | `/geofences/:id` | Detail geofence |
| `PUT` | `/geofences/:id` | Update geofence |
| `DELETE` | `/geofences/:id` | Hapus geofence |
| `GET` | `/geofences/alerts` | Riwayat pelanggaran batas wilayah |

### Dashboard & System

| Method | Endpoint | Deskripsi |
|---|---|---|
| `GET` | `/dashboard/stats` | Statistik ringkasan armada |
| `GET` | `/ws` | WebSocket endpoint (realtime telemetry stream) |
| `GET` | `/health` | Health check (MySQL, Redis, Kafka status) |
| `GET` | `/ping` | Liveness check |

### gRPC — Ingestion Service (port 50051)

```protobuf
service LocationService {
  rpc StreamLocation(stream LocationRequest) returns (stream LocationAck);
}
```

### WebSocket Event Payloads

```jsonc
// Emitted on every GPS update
{
  "event": "location_updated",
  "driver_id": "59f62aa8-...",
  "driver_code": "driver-001",
  "latitude": 3.5952,
  "longitude": 98.6722,
  "speed": 45.0,
  "heading": 120.0,
  "status": "online",
  "timestamp": "2026-10-06T03:00:00Z"
}

// Emitted when a driver crosses a geofence boundary
{
  "event": "geofence_alert",
  "driver_code": "driver-001",
  "geofence_id": "...",
  "geofence_name": "Zona Gudang Belawan",
  "alert_type": "enter",
  "latitude": 3.591,
  "longitude": 98.671,
  "triggered_at": "2026-10-06T03:00:01Z"
}
```

---

## 🧠 Engineering Decisions

### Mengapa gRPC untuk Ingestion?

Kendaraan mengirim data GPS setiap 1–3 detik. Dengan HTTP/REST, setiap request memiliki overhead TCP handshake + HTTP headers baru. **gRPC bidirectional streaming** mempertahankan satu koneksi TCP terbuka per kendaraan, sehingga latensi per event turun dari ~100ms → < 5ms dan penggunaan bandwidth jauh lebih efisien.

### Mengapa Kafka sebagai Buffer?

Tanpa Kafka, lonjakan tiba-tiba (1000 kendaraan masuk sekaligus) akan langsung membebani database MySQL hingga kolaps. Kafka berperan sebagai **shock absorber** — menerima event dengan kecepatan tulis (ingestion rate) dan menyerahkannya ke consumer sesuai kemampuan proses (consumption rate). Jika Tracking Service down sekalipun, event tidak hilang karena Kafka menyimpannya hingga consumer aktif kembali.

### Mengapa Redis untuk Last Position?

Query "di mana posisi driver sekarang?" adalah query **paling sering dipanggil** di sistem fleet tracking. Mengambil dari MySQL yang harus scan ribuan baris history tiap request adalah pemborosan sumber daya. Redis menyimpan snapshot posisi terakhir setiap driver dengan TTL otomatis, menghasilkan response < 5ms vs MySQL yang bisa > 50ms.

### Dual Geofence Algorithm

- **Circle → Haversine Formula** — Efisien untuk zona radius sederhana (bandara, gudang, kantor). O(1) per evaluasi.
- **Polygon → Ray-Casting Algorithm** — Mendukung zona tidak beraturan (kawasan industri, batas kota, area terlarang tidak simetris). Tidak bisa digantikan oleh algoritma lingkaran.

---

## 📊 Skenario Penggunaan Nyata

Sistem ini dibangun untuk menjawab kebutuhan umum di industri **logistik, ride-hailing, dan manajemen armada korporat**:

1. **Perusahaan Logistik & Ekspedisi** — Pantau armada truk pengiriman secara realtime, deteksi penyimpangan rute, dan rekam waktu tiba/berangkat dari gudang.
2. **Platform Ride-Hailing** — Cari driver terdekat dari titik pickup customer menggunakan Redis Geospatial dalam hitungan milidetik.
3. **Manajemen Armada Korporat** — Monitor armada operasional perusahaan berikut laporan idle terlalu lama, overspeed, dan pelanggaran zona yang sudah ditentukan.
4. **Keamanan & Pemantauan Aset** — Notifikasi otomatis jika kendaraan berharga meninggalkan area yang diizinkan (geofence exit alert).

---

## 👨‍💻 Author

**Tento** — [@Tento03](https://github.com/Tento03)

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.
