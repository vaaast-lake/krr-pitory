# krr-pitory

> **Kafka + RabbitMQ + Redis** 기반 멀티 셀러 이커머스 풀필먼트 데이터 파이프라인

세 미들웨어를 단순히 조합하는 것이 아니라, **각각의 역할이 겹치지 않도록 설계**하는 것이 목표다.
Olist Brazilian E-Commerce 데이터셋(10만 건)을 k6로 재생해 실제 서비스처럼 동작하는 파이프라인을 구축한다.

---

## 미들웨어 역할 분리

| 미들웨어 | 역할 | 선택 이유 |
| -------- | ---- | --------- |
| **Kafka** | 이벤트 스트리밍, 서비스 간 디커플링 | 같은 이벤트를 fulfillment / analytics가 독립적으로 소비. 한 group 장애가 다른 group에 영향 없음 |
| **RabbitMQ** | 셀러별 작업 dispatch, delayed 알림 | ACK 기반 처리 보장, 셀러별 큐 격리, delayed message로 N시간 후 리뷰 요청 알림 |
| **Redis** | 재고 원자 차감, 주문 버퍼, idempotency | Lua script로 oversell 방지, TTL 기반 결제 대기 버퍼, 중복 이벤트 방지 |

---

## 아키텍처

```
k6 (외부 클라이언트)
  └─ 주문 / 결제 / 취소 / 리뷰 ──→ source-server
  └─ 출하 / 배송 완료           ──→ fulfillment-server

source-server
  POST /orders          → Redis pending (결제 대기 버퍼, TTL)
  POST /orders/:id/approve → Kafka: order.created
  PATCH /orders/:id/cancel → Kafka: order.canceled (customer_request)
  POST /reviews         → Kafka: review.created
  RabbitMQ consumer     → notification.review → notification_log 기록

                Kafka: order.created
                      │
          ┌───────────┴───────────┐
          ▼                       ▼
  fulfillment-server        analytics-server
  (fulfillment-group)       (analytics-group)
  Redis Lua DECR            주문 생성 집계
  RabbitMQ dispatch
  → seller.{id}.orders

  k6: PATCH /orders/:id/ship
  k6: PATCH /orders/:id/deliver
          │
          ▼
  Kafka: order.processed ──→ analytics-server (배송 통계, SLA 분석)
  RabbitMQ: notification.review delayed ──→ source-server (N시간 후 소비)
```

**3개 서버, 각자 독립적인 PostgreSQL DB 소유. 서버 간 직접 DB 참조 없음.**

---

## 주문 상태 흐름

```
주문 클릭
  → Redis pending (TTL 내 결제 미완료 시 자동 소멸)
  → 결제 승인 → Kafka: order.created
      → 재고 차감 성공 → RabbitMQ: seller 큐 dispatch → processing
          → PATCH_ORDER_SHIP  → shipped
          → PATCH_ORDER_DELIVER → delivered → Kafka: order.processed
      → 재고 부족 → Kafka: order.canceled (stock_unavailable) → 환불
  → 고객 취소 → Kafka: order.canceled (customer_request) → 재고 복구 + 환불
```

---

## 빠른 시작

**요구사항:** Docker, Node.js 20+, pnpm

```bash
# 1. 환경변수 설정
cp .env.example .env
# SPEED_FACTOR=10000  → 2년치 데이터를 약 1.7시간에 재생
# SAMPLE_SIZE=10000   → 개발/검증용 (미설정 시 전체 99k)

# 2. 인프라 실행
docker-compose up -d

# 3. 데이터 적재
pnpm seed:reference   # 셀러, 상품, 고객 마스터 데이터
pnpm seed:inventory   # 재고 초기값 (Redis + DB)

# 4. 시뮬레이션 데이터 준비
pnpm prepare-simulation  # CSV → events.json 변환

# 5. 시뮬레이션 실행
pnpm simulate

# 6. 모니터링
open http://localhost:3000  # Grafana
```

---

## 시뮬레이션 설정

| 옵션 | 설명 | 권장값 |
| ---- | ---- | ------ |
| `SPEED_FACTOR` | 시간 압축 배율. 클수록 빠르게 재생 | 개발: 1000 / 검증: 10000 |
| `SAMPLE_SIZE` | 처리할 주문 수 | 개발: 1000 / 검증: 10000 / 전체: 미설정 |

**SPEED_FACTOR별 소요 시간 (전체 99k 주문 기준)**

| SPEED_FACTOR | 소요 시간 | 용도 |
| ------------ | --------- | ---- |
| 1000x | 약 17시간 | 장기 안정성 테스트 |
| 10000x | 약 1.7시간 | 병목 관찰 (권장) |

---

## 관찰 포인트 (Grafana)

| 항목 | 확인할 현상 |
| ---- | ----------- |
| **Kafka consumer lag** | SPEED_FACTOR 고속 시 fulfillment-group이 처리를 따라가지 못하는 구간 |
| **RabbitMQ queue depth** | 특정 셀러 큐에 메시지 적체 (처리 속도 불균형) |
| **RabbitMQ DLQ** | 3회 실패 후 DLQ로 이동한 메시지 수 |
| **Redis oversell 방지** | Lua DECR 거부 건수 — 재고 부족으로 취소된 주문 비율 |
| **PostgreSQL connection pool** | 동시 INSERT 폭증 시 pool exhaustion |
| **notification_log vs review.created** | 리뷰 요청 발송 건수 대비 실제 리뷰 작성 전환율 |

---

## 프로젝트 구조

```
krr-pitory/
  apps/
    source-server/
      src/
        kafka/        KafkaModule, cancel-group, producer
        redis/        RedisModule, pending TTL
        rabbitmq/     RabbitMQModule, notification.review consumer
    fulfillment-server/
      src/
        kafka/        KafkaModule, fulfillment-group, producer/consumer
        redis/        RedisModule, Lua script (재고 DECR/INCR)
        rabbitmq/     RabbitMQModule, seller dispatch, DLQ 초기화
    analytics-server/
      src/
        kafka/        KafkaModule, analytics-group, consumer
  packages/
    event-types/      Zod 스키마 (3개 서버 공유 — 불일치 시 파싱 실패)
    kafka-client/     토픽 이름 상수 (3개 서버 공유 — 불일치 시 이벤트 미전달)
    rabbitmq-client/  notification.review 큐 이름 상수 (source/fulfillment 공유)
    tsconfig/         공통 TypeScript 설정
  scripts/
    seed-reference.ts     마스터 데이터 적재
    seed-inventory.ts     재고 초기값 적재
    prepare-simulation.ts CSV → events.json 변환
  simulation/k6/          k6 시뮬레이션 스크립트
  monitoring/             Prometheus + Grafana 설정
  docker-compose.yml
  .env
```

packages/에는 **여러 서버가 동시에 같은 값을 참조해야 하고 달라지면 런타임 버그가 생기는 것**만 넣는다. 연결 설정, Lua script, DLQ 초기화는 각 서버 내부 모듈에 위치한다.

---

## 데이터셋

[Olist Brazilian E-Commerce Public Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — 2016~2018년 실제 거래 데이터, 익명화 처리.

주문~결제 승인 간격이 최대 3일(볼레토 결제)에 달하므로 Redis pending TTL을 `SPEED_FACTOR`에 맞춰 동적으로 계산한다.

---

## 상세 설계

[PLAN_ARCHITECTURE.md](./PLAN_ARCHITECTURE.md)
