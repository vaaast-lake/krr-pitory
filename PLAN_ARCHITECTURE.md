# krr-pitory 아키텍처 계획서

> **Kafka + RabbitMQ + Redis** 기반 멀티 셀러 이커머스 풀필먼트 데이터 파이프라인

---

## 1. 프로젝트 개요

| 항목        | 내용                                                                               |
| ----------- | ---------------------------------------------------------------------------------- |
| 프로젝트명  | krr-pitory                                                                         |
| 네이밍      | Kafka + RabbitMQ + Redis (KRR) + Pipeline Factory (pitory)                         |
| 도메인      | 멀티 셀러 이커머스 주문 풀필먼트                                                   |
| 데이터셋    | Olist Brazilian E-Commerce Public Dataset (Kaggle)                                 |
| 핵심 목표   | Kafka / RabbitMQ / Redis 각 미들웨어의 역할을 명확히 분리한 데이터 파이프라인 구축 |
| 트래픽 특성 | 지속적·정기적 주문 처리 (burst 없는 steady-state 도메인)                           |

---

## 2. 데이터셋

**Olist Brazilian E-Commerce Dataset** (Kaggle)

실제 상업 데이터 기반, 익명화 처리됨. 2016~2018년 10만 건 주문 데이터.

| 테이블        | 주요 컬럼                                                                                | 소유 서버          |
| ------------- | ---------------------------------------------------------------------------------------- | ------------------ |
| orders        | order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at         | source-server      |
| customers     | customer_id, zip_code_prefix, city, state                                                | source-server      |
| payments      | order_id, payment_sequential, payment_type, payment_installments, payment_value          | source-server      |
| order_reviews | order_id, review_id, review_score, review_creation_date, review_answer_timestamp         | source-server      |
| geolocation   | geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, city, state              | source-server      |
| order_items   | order_id, order_item_id, product_id, seller_id, price, freight_value, shipping_limit_date| fulfillment-server |
| sellers       | seller_id, seller_zip_code_prefix, seller_city, seller_state                            | fulfillment-server |
| products      | product_id, product_category_name, product_weight_g, product_length_cm                  | fulfillment-server |
| category_name | product_category_name, product_category_name_english                                    | fulfillment-server |

---

## 3. 서버 구성

3-server 구성. Kafka의 multi consumer group 특성을 제대로 활용하기 위해 처음부터 분리.

```
source-server              fulfillment-server      analytics-server
─────────────              ──────────────────      ────────────────
주문 / 리뷰 API             주문 처리 · 셀러 연동    집계 · 리포트
Kafka Producer             Kafka Consumer          Kafka Consumer
Kafka Consumer               (fulfillment-group)     (analytics-group)
  (cancel-group)           Kafka Producer
RabbitMQ Consumer          RabbitMQ Producer
  (notification.review)    RabbitMQ Consumer
Redis 클라이언트             Redis 클라이언트
```

- source-server: `order.canceled` 이벤트 소비(cancel-group) → 환불 처리, 주문 상태 업데이트, 알림 기록
- source-server: `notification.review` 큐 소비 → 리뷰 요청 알림 `notification_log` 기록
- fulfillment-server: `order.created` / `order.canceled` 소비 + `order.processed` / `order.canceled(stock_unavailable)` 발행

### Kafka consumer group 구성

```
order.created 토픽
  ├── fulfillment-group  → fulfillment-server (재고 차감, RabbitMQ dispatch)
  └── analytics-group   → analytics-server   (주문 생성 집계)

order.processed 토픽
  └── analytics-group   → analytics-server   (배송 완료 집계, SLA 분석)

order.canceled 토픽
  ├── fulfillment-group  → fulfillment-server
  │     customer_request: fulfillment_events에 processing 레코드 있으면 재고 INCR 복구
  │     stock_unavailable: 내부에서 이미 처리됨, 스킵
  ├── cancel-group       → source-server      (환불 처리, 주문 상태 업데이트, 알림 기록)
  └── analytics-group   → analytics-server   (취소율 집계)

review.created 토픽
  └── analytics-group   → analytics-server   (seller_performance 평점 집계)
```

**order.canceled 발행 주체**

| 취소 원인           | 발행 주체           | 비고                                              |
| ------------------- | ------------------- | ------------------------------------------------- |
| stock_unavailable   | fulfillment-server  | Lua DECR 실패 시 즉시 발행. 서버 간 API 호출 없음 |
| customer_request    | source-server       | PATCH /orders/:id/cancel 수신 시 발행             |

fulfillment-server가 재고 부족 시 source-server API를 직접 호출하면 동기 결합이 생겨 이벤트 드리븐 설계 목적에 반한다. fulfillment-server가 Kafka에 직접 발행하고, cancel-group(source-server)이 소비해 주문 상태를 업데이트하는 것이 올바른 흐름이다.

같은 이벤트를 여러 목적으로 독립 소비하는 구조 — Kafka의 핵심 가치 활용.

### 서버별 DB 소유권

각 서버는 독립적인 PostgreSQL DB를 가짐. 서버 간 직접 DB 참조 없음.

| 서버               | 소유 테이블                                                                      | 비고                                          |
| ------------------ | -------------------------------------------------------------------------------- | --------------------------------------------- |
| source-server      | orders, customers, payments, order_reviews, geolocation, notification_log        | 주문·결제·리뷰·알림의 원점. order_id 발행 주체 |
| fulfillment-server | order_items, sellers, products, category_name, fulfillment_events, **inventory** | inventory는 Redis 카운터의 영속성 백업         |
| analytics-server   | delivery_stats, seller_performance                                               | Kafka 이벤트 소비 후 자체 집계 테이블 생성     |

**inventory 테이블 (fulfillment-server)**

```sql
inventory (
  product_id   VARCHAR PRIMARY KEY,  -- FK → products
  quantity     INTEGER NOT NULL,      -- 현재 재고 수량
  updated_at   TIMESTAMP
)
```

Redis가 동시성 제어(Lua script atomic DECR)를 담당하고, PostgreSQL inventory가 영속성을 담당한다. Redis 재시작 시 inventory 테이블에서 재고를 복구한다.

**fulfillment_events 테이블 (fulfillment-server)**

```sql
fulfillment_events (
  id                    SERIAL PRIMARY KEY,
  order_id              VARCHAR NOT NULL,
  seller_id             VARCHAR NOT NULL,
  event_type            VARCHAR NOT NULL,   -- processing / shipped / delivered / canceled / unavailable
  occurred_at           TIMESTAMP NOT NULL,
  estimated_delivery_at TIMESTAMP,          -- SLA 기준 (order_estimated_delivery_date)
  notes                 TEXT                -- 실패 사유 등
)
```

**notification_log 테이블 (source-server)**

```sql
notification_log (
  id                SERIAL PRIMARY KEY,
  order_id          VARCHAR NOT NULL,
  recipient_type    VARCHAR NOT NULL,  -- buyer / seller
  recipient_id      VARCHAR NOT NULL,  -- customer_id or seller_id
  notification_type VARCHAR NOT NULL,  -- review_request / delivery_complete / review_received / order_canceled
  sent_at           TIMESTAMP NOT NULL,
  channel           VARCHAR            -- 시뮬레이션에서는 log만 기록
)
```

source-server가 `order.canceled` 이벤트 소비 시 notification_log에 취소 알림을 기록한다. RabbitMQ `notification.review` delayed message를 소비할 때도 source-server가 기록한다. notification_log는 파이프라인 검증 지표("리뷰 요청 발송 건수 vs 실제 review.created 건수")와 중복 발송 방지에 사용된다.

**delivery_stats 테이블 (analytics-server)**

```sql
delivery_stats (
  id                    SERIAL PRIMARY KEY,
  order_id              VARCHAR NOT NULL UNIQUE,
  seller_id             VARCHAR NOT NULL,
  shipped_at            TIMESTAMP NOT NULL,
  delivered_at          TIMESTAMP NOT NULL,
  estimated_delivery_at TIMESTAMP,
  delivery_days         INTEGER,              -- delivered_at - shipped_at (일 단위)
  sla_breached          BOOLEAN NOT NULL DEFAULT FALSE,  -- delivered_at > estimated_delivery_at
  created_at            TIMESTAMP NOT NULL DEFAULT NOW()
)
```

**seller_performance 테이블 (analytics-server)**

```sql
seller_performance (
  seller_id             VARCHAR PRIMARY KEY,
  total_orders          INTEGER NOT NULL DEFAULT 0,      -- order.created
  completed_orders      INTEGER NOT NULL DEFAULT 0,      -- order.processed
  canceled_orders       INTEGER NOT NULL DEFAULT 0,      -- order.canceled
  stock_unavailable_cnt INTEGER NOT NULL DEFAULT 0,      -- order.canceled(stock_unavailable)
  avg_review_score      NUMERIC(3,2),
  review_count          INTEGER NOT NULL DEFAULT 0,
  avg_delivery_days     NUMERIC(5,2),
  sla_breach_count      INTEGER NOT NULL DEFAULT 0,
  updated_at            TIMESTAMP NOT NULL DEFAULT NOW()
)
```

seller_performance는 이벤트마다 직접 UPSERT하지 않는다. analytics-server 내부 인메모리 버퍼에 누적 후 5초마다 배치 flush한다(아래 참고).

### 서버 간 데이터 참조 규칙

- 서버 간 직접 DB 참조 없음
- 모든 데이터 교환은 Kafka 이벤트 페이로드로 필요한 정보를 담아 전달
- geolocation 조회가 필요한 경우 source-server API 호출

---

## 4. 주문 상태 FSM

Olist `order_status` 실제값 기반.

```
                    [Redis 취소 가능 구간]
                    ┌──────────────────┐
주문 버튼 클릭 ──→  │  pending (Redis) │ ──→ TTL 만료 or 결제 미완료 → 자동 소멸
                    └────────┬─────────┘     (DB status = 'pending' 유지, 파이프라인 미진입)
                             │ 결제 승인 확인 (source-server 내부 처리)
                             │   → payments 테이블 저장
                             │   → Redis pending DEL
                             ↓
                        Kafka produce
                             │
                    ┌────────▼─────────┐
                    │     created      │  Kafka: order.created
                    └────────┬─────────┘  (페이로드에 items 배열, payment 정보 포함)
                             │
              ┌──────────────┴──────────────┐
              ↓ fulfillment-group            ↓ analytics-group
     Redis: idempotency key 체크       주문 생성 집계
     Redis Lua: 재고 atomic DECR
              │
              │ 재고 부족 시
              ├──────────────────────────────→ order.canceled 발행 (reason: 'stock_unavailable')
              │                                  └→ cancel-group (source-server)
              │                                       → orders status: 'unavailable'
              │                                       → payments 환불 처리
              │                                       → notification_log 기록
              │                                  └→ analytics-group: 취소율 집계
              │
              │ 재고 차감 성공
              ↓
     ┌────────▼─────────┐
     │   processing     │  RabbitMQ: seller.{id}.orders dispatch
     └────────┬─────────┘  fulfillment_events 기록 (event_type: processing)
              │
              │ k6: PATCH /orders/:id/ship → fulfillment-server
              ↓   (Olist: order_delivered_carrier_date — 택배사 인수, 출하 완료)
     ┌────────▼─────────┐
     │    shipped       │  Redis: 주문 상태 캐시 SET
     └────────┬─────────┘  fulfillment_events 기록 (event_type: shipped)
              │
              │ k6: PATCH /orders/:id/deliver → fulfillment-server
              ↓   (Olist: order_delivered_customer_date — 고객 수령 완료)
     ┌────────▼──────────┐
     │    delivered      │  Kafka: order.processed 발행
     └────────┬──────────┘  fulfillment_events 기록 (event_type: delivered)
              │             RabbitMQ: notification.review delayed message 발행
              │             (shippedAt, deliveredAt, estimatedDeliveryAt 페이로드 포함)
              ↓
     [analytics-group consume]
       → delivery_stats INSERT (배송 소요일, SLA 초과 여부)
       → seller_performance 버퍼 누적

     [source-server RabbitMQ consumer]
       → notification.review delayed message 소비 (N시간 후)
       → notification_log 기록 (리뷰 요청 알림)
```

### 취소 처리

| 취소 시점 | 원인 | 처리 방식 |
| --------- | ---- | --------- |
| 결제 미완료 (Redis pending 구간) | 결제 타임아웃, 카드 거절 | Redis TTL 만료 → 자동 소멸. DB status 'pending' 유지. 파이프라인 미진입 |
| 결제 승인 후 재고 부족 | stock_unavailable | fulfillment-server: `order.canceled` 발행 → 보상 파이프라인 (환불, notification_log) |
| 결제 승인 후 고객 취소 | 고객 요청 | source-server: `order.canceled` 발행 → 보상 파이프라인 동일, fulfillment-group이 재고 INCR |

**고객 취소 시 재고 INCR 처리**

fulfillment-group이 `order.canceled(customer_request)` 수신 시, `fulfillment_events` 테이블에서 해당 `order_id`의 `event_type = 'processing'` 레코드 존재 여부를 확인한다. 레코드가 있으면 재고 차감이 완료된 것이므로 INCR 복구를 수행하고, 없으면 재고가 차감되지 않은 것이므로 스킵한다. (결제 승인 직후 ~ fulfillment-server 처리 완료 전 취소 요청 케이스 대응)

**시뮬레이션에서 취소 주문(Olist) 처리 방식**

Olist 데이터에서 `order_status == 'canceled' AND order_approved_at IS NULL`인 주문은 결제 미완료 취소다. prepare-simulation.ts가 이 주문들의 `POST_ORDER_APPROVE` 이벤트를 생성하지 않는다. k6는 `POST /orders`만 호출하고 approve를 호출하지 않으며, Redis TTL 만료로 자연 소멸하여 실제 결제 미완료 시나리오를 재현한다.

### order_reviews 처리

배송 완료 후 구매자가 직접 작성하는 별도 프로세스. 파이프라인 내부 단계가 아님.

```
delivered 이후
  → RabbitMQ notification.review delayed message → N시간 후 source-server가 소비
  → notification_log 기록 (리뷰 요청 알림 발송 기록)
  → 구매자가 POST /reviews 호출 → source-server가 order_reviews 저장
  → Kafka: review.created 발행 → analytics-group이 seller_performance 집계에 반영
```

`notification.review`는 배송 완료 후 일정 시간 뒤 구매자에게 "구매 후기를 남겨주세요" 형태의 알림을 발송하는 delayed message다. 실제 알림 발송은 없고 notification_log에 기록만 한다.

---

## 5. 미들웨어 역할 배분

### Kafka

| 항목           | 내용                                                                                                          |
| -------------- | ------------------------------------------------------------------------------------------------------------- |
| 역할           | 이벤트 스트리밍, 디커플링, 재처리 보장, multi consumer group                                                   |
| 주요 토픽      | `order.created` (source-server 발행), `order.processed` (fulfillment-server 발행), `order.canceled` (source-server 또는 fulfillment-server 발행), `review.created` (source-server 발행) |
| 파티셔닝 키    | `order_id` — order 단위 순서 보장, 멀티 셀러 주문에서도 단일 기준 유지                                        |
| Consumer Group | `fulfillment-group` (fulfillment-server), `cancel-group` (source-server), `analytics-group` (analytics-server) |
| 핵심 가치      | 같은 이벤트를 fulfillment / cancel / analytics가 독립적으로 소비. 한 group 장애가 다른 group에 영향 없음       |

seller_id 파티셔닝은 하나의 주문에 여러 seller의 상품이 포함될 경우 키 선택이 모호해진다. seller 단위 순서 보장은 RabbitMQ의 셀러별 큐가 담당하므로 Kafka에서 seller_id 파티셔닝은 불필요하다.

---

### RabbitMQ

| 항목         | 내용                                                                                   |
| ------------ | -------------------------------------------------------------------------------------- |
| 역할         | 셀러별 작업 dispatch, ACK 기반 처리 보장, 배송 완료 후 delayed 리뷰 요청 알림          |
| Exchange     | Direct exchange, routing_key: seller_id                                                |
| 주요 큐      | `seller.{seller_id}.orders` (셀러별 격리), `notification.review` (delayed 리뷰 요청)   |
| 핵심 패턴    | Dead Letter Queue (DLQ) — 3회 실패 시 DLQ 이동 후 수동 검토 또는 재스케줄              |
| Kafka와 차이 | Kafka는 이벤트 스트림(로그, 소비 후 보존), RabbitMQ는 작업 단위 dispatch (ACK 후 소멸) |

**셀러별 큐 동적 생성 전략**

Olist 데이터에 셀러가 약 3,000명이다. 서버 시작 시 전체 큐를 미리 생성하면 메모리 낭비다. 첫 주문 dispatch 시점에 동적으로 생성한다.

```typescript
// 주문 dispatch 시 - assertQueue는 이미 존재하면 그냥 반환 (idempotent)
await channel.assertQueue(`seller.${sellerId}.orders`, {
  durable: true,
  arguments: {
    'x-expires': 1800000,          // 30분간 비활성 시 자동 삭제
    'x-dead-letter-exchange': 'dlx',
  }
})
```

`x-expires`로 주문이 없는 셀러의 큐는 자동 정리된다.

**DLQ 초기화**

서버 시작 시 `packages/rabbitmq-client`에서 1회 선언한다. `dlx` exchange가 선언되지 않은 상태에서 셀러 큐를 생성하면 오류가 발생한다.

```typescript
// packages/rabbitmq-client/src/init.ts
await channel.assertExchange('dlx', 'direct', { durable: true })
await channel.assertQueue('dlq', { durable: true })
await channel.bindQueue('dlq', 'dlx', '')
```

3회 retry는 RabbitMQ가 자동으로 처리하지 않으므로 consumer에서 메시지 헤더로 관리한다.

```typescript
const retryCount = (msg.properties.headers?.['x-retry-count'] ?? 0) as number
if (retryCount >= 3) {
  channel.nack(msg, false, false)  // DLQ로 이동
} else {
  // 처리 재시도 후 헤더 incrementing republish
}
```

---

### Redis

| 역할                | 구현                                                                             | 시점             |
| ------------------- | -------------------------------------------------------------------------------- | ---------------- |
| 주문 취소 가능 버퍼 | `SETEX order:{id} {TTL} pending` — TTL = 동적 계산(아래 참고), 결제 승인 시 DEL  | Kafka 앞 (상시)  |
| Idempotency key     | `SET idem:{event_id} 1 NX EX 86400` — 중복 이벤트 방지                           | 처리 중간 (상시) |
| 재고 atomic DECR    | Lua script: 주문 내 전체 product 일괄 검사 후 전체 DECR — all-or-nothing, 성공 시 inventory 배치 UPDATE | 처리 중간 (상시) |
| 주문 상태 캐시      | `SET order:status:{id} shipped EX 3600` — DB 부하 절감                           | 처리 중간 (상시) |

**Redis pending TTL 계산**

Olist 데이터에는 브라질 볼레토(은행 전표) 결제 방식이 약 20%를 차지하며, 주문~결제 승인 간격이 최대 3 영업일(약 259,200초)에 달한다. TTL을 1,800초 고정으로 설정하면 SPEED_FACTOR 고속 시 볼레토 주문의 approve 이벤트가 TTL 만료 후 도달해 정상 주문이 소멸된다.

```typescript
// source-server 환경변수 기반 동적 계산
const SPEED_FACTOR = parseInt(process.env.SPEED_FACTOR ?? '1')
const MAX_APPROVAL_WAIT_SEC = 3 * 24 * 60 * 60  // 259,200초 (3일)
const PENDING_TTL = Math.ceil(MAX_APPROVAL_WAIT_SEC / SPEED_FACTOR)

// SPEED_FACTOR=1     → TTL 259,200초 (3일)
// SPEED_FACTOR=1000  → TTL 260초
// SPEED_FACTOR=10000 → TTL 26초
```

k6도 동일 SPEED_FACTOR로 이벤트 간격을 압축하므로 TTL과 이벤트 도달 시각의 비율이 일치한다. SPEED_FACTOR는 docker-compose `.env` 파일에서 단일 관리한다.

**재고 차감 Lua script (멀티 product 원자 처리)**

하나의 주문에 상품 A, B, C가 있을 때 A, B DECR 성공 후 C가 실패하면 A, B에 대한 보상이 필요하다. 이를 피하기 위해 단일 Lua script 안에서 전체 검사 후 전체 DECR을 실행한다. Redis Lua script는 실행 중 원자성이 보장되므로 별도 rollback 로직이 불필요하다.

```lua
-- KEYS[1..n]: stock:{product_id}
-- ARGV[1..n]: 차감 수량 (order_items 기준, 통상 1)

-- Phase 1: 전체 재고 검사
for i = 1, #KEYS do
  local stock = redis.call('GET', KEYS[i])
  if stock == false or tonumber(stock) < tonumber(ARGV[i]) then
    return {-1, i}  -- i번째 product 재고 부족, 전체 거부
  end
end

-- Phase 2: 전체 통과 시 원자적으로 전부 DECR
for i = 1, #KEYS do
  redis.call('DECRBY', KEYS[i], tonumber(ARGV[i]))
end

return {0, 0}  -- 전체 성공 → inventory 테이블 배치 UPDATE
```

**재고 복구 Lua script (취소 보상)**

order.canceled(customer_request) 수신 시 items 배열을 그대로 사용해 일괄 INCR한다.

```lua
-- KEYS[1..n]: stock:{product_id}
-- ARGV[1..n]: 복구 수량
for i = 1, #KEYS do
  redis.call('INCRBY', KEYS[i], tonumber(ARGV[i]))
end
return 1
```

`packages/redis-client`에 두 함수로 추상화한다.

```typescript
decrementStock(items: { productId: string; quantity: number }[]): Promise<{ success: boolean; failedIndex?: number }>
incrementStock(items: { productId: string; quantity: number }[]): Promise<void>
```

---

## 6. 시뮬레이션

실제 서비스 구조로 구현하되, k6가 Olist 데이터셋 기반으로 자동 트래픽을 생성해 실제 서비스처럼 동작.

### 데이터 활용 전략

Olist 데이터는 이미 처리된 결과 데이터다. 용도에 따라 두 가지로 분리한다.

**고정 참조 데이터 — 초기 DB seed (서버 구동 전 1회)**

실제 서비스에서 시뮬레이션 이전에 이미 존재하는 마스터 데이터로 취급. 이벤트와 함께 생성되는 데이터(orders, payments, order_items, order_reviews)는 seed 대상이 아니다.

| CSV 파일                              | 적재 위치              |
| ------------------------------------- | ---------------------- |
| olist_sellers_dataset.csv             | fulfillment-server DB  |
| olist_products_dataset.csv            | fulfillment-server DB  |
| product_category_name_translation.csv | fulfillment-server DB  |
| olist_customers_dataset.csv           | source-server DB       |
| olist_geolocation_dataset.csv         | source-server DB       |

**재고 초기값 seed — Redis + inventory 테이블**

`olist_products_dataset.csv`에 재고 수량 컬럼이 없으므로 합성. `olist_order_items_dataset.csv`에서 product별 주문 빈도를 집계하고, **product마다 다른 랜덤 배율**을 적용해 초기 재고를 계산한다.

```typescript
// seed-inventory.ts
const multiplier = 0.8 + Math.random() * 0.5  // 0.8x ~ 1.3x (product마다 다름)
const quantity = Math.floor(orderCount * multiplier)
```

```
product "A" → 주문 50건, 배율 1.2 → 재고 60  (여유 있음, 전량 성공)
product "B" → 주문 30건, 배율 0.85 → 재고 25  (후반 5건 거부 가능)
product "C" → 주문 10건, 배율 0.9 → 재고 9   (마지막 1건 거부 가능)

→ Redis: SET stock:{product_id} {quantity}
→ inventory 테이블: INSERT (product_id, quantity)
```

**k6와 source-server 역할 분리**

```
k6 (외부 클라이언트 역할)
  - events.json을 SharedArray로 로드
  - 언제 어떤 API를 호출할지 결정 (occurredAt 필드 기반)
  - 필요한 데이터를 JSON 페이로드로 담아 HTTP 요청
  - source-server: 주문, 결제 승인, 취소, 리뷰 API
  - fulfillment-server: 출하(ship), 배송 완료(deliver) API
  - 시스템 내부 구조를 모름

source-server (시스템 경계)
  - HTTP 요청 수신 및 유효성 검증
  - 받은 데이터를 DB에 저장
  - Redis 처리, Kafka 이벤트 발행
  - CSV를 직접 읽지 않음, 시뮬레이션을 모름

fulfillment-server (시스템 경계)
  - PATCH /orders/:id/ship: fulfillment_events 기록(shipped), Redis 캐시 SET
  - PATCH /orders/:id/deliver: fulfillment_events 기록(delivered), Kafka order.processed 발행,
                               RabbitMQ notification.review delayed message 발행
```

API gateway는 두지 않는다. k6는 내부 테스트 클라이언트로 두 서버의 URL을 직접 참조하며, 환경변수(`SOURCE_SERVER_URL`, `FULFILLMENT_SERVER_URL`)로 관리한다.

**이벤트 재생 데이터 — prepare-simulation.ts → events.json → k6**

```
scripts/prepare-simulation.ts (Node.js, k6 아님)
  → orders.csv + payments.csv + order_items.csv + order_reviews.csv 읽기
  → 조인 및 필터링
      - order_approved_at IS NULL AND status = 'canceled' 주문:
        POST_ORDER 이벤트만 생성, POST_ORDER_APPROVE 미포함
      - SAMPLE_SIZE 환경변수로 주문 수 제한 (미설정 시 전체)
  → timestamp 컬럼 기반 이벤트 시퀀스 변환 + 전체 시간순 정렬
  → simulation/data/events.json 출력

k6
  → events.json을 SharedArray로 로드 (메모리 효율, VU 간 공유)
  → SPEED_FACTOR 배율로 시간 간격 압축
  → source-server / fulfillment-server HTTP API 순차 호출
```

**SAMPLE_SIZE 제어**

```bash
# 개발 / 디버깅
SAMPLE_SIZE=1000 pnpm prepare-simulation

# 파이프라인 기능 검증
SAMPLE_SIZE=10000 pnpm prepare-simulation

# 대용량 시나리오 (전체)
pnpm prepare-simulation
```

events.json 구조:

```json
[
  {
    "occurredAt": "2017-10-02 10:56:33",
    "type": "POST_ORDER",
    "target": "source-server",
    "payload": {
      "order_id": "e481f51c...",
      "customer_id": "9ef432eb...",
      "items": [
        { "product_id": "...", "seller_id": "...", "price": 58.90 }
      ]
    }
  },
  {
    "occurredAt": "2017-10-02 11:07:15",
    "type": "POST_ORDER_APPROVE",
    "target": "source-server",
    "payload": {
      "order_id": "e481f51c...",
      "payment": { "type": "credit_card", "value": 99.90, "installments": 1 }
    }
  },
  {
    "occurredAt": "2017-10-04 19:55:00",
    "type": "PATCH_ORDER_SHIP",
    "target": "fulfillment-server",
    "payload": { "order_id": "e481f51c..." }
  },
  {
    "occurredAt": "2017-10-07 14:30:00",
    "type": "PATCH_ORDER_DELIVER",
    "target": "fulfillment-server",
    "payload": { "order_id": "e481f51c..." }
  }
]
```

timestamp 컬럼 → 이벤트 타입 매핑:

```
order_purchase_timestamp       → POST_ORDER          (target: source-server)
order_approved_at              → POST_ORDER_APPROVE  (target: source-server, payment 정보 포함)
order_delivered_carrier_date   → PATCH_ORDER_SHIP    (target: fulfillment-server, 출하 완료)
order_delivered_customer_date  → PATCH_ORDER_DELIVER (target: fulfillment-server, 고객 수령 완료)
order_status == 'canceled'     → PATCH_ORDER_CANCEL  (target: source-server, approved_at 있는 경우만)
delivered 이후 일정 시간 후    → POST_REVIEW         (target: source-server, order_reviews.csv 기반)
```

### k6 시나리오 구성

```
[초기화]
scripts/seed-reference.ts      → 마스터 데이터 각 서버 DB 적재
scripts/seed-inventory.ts      → 재고 초기값 Redis + inventory 테이블 적재
scripts/prepare-simulation.ts  → CSV 조인 + 이벤트 시퀀스 변환 → events.json

[시뮬레이션]
k6 실행
  → events.json SharedArray 로드
  → SPEED_FACTOR 배율로 시간 간격 압축
  → target 필드 기반으로 source-server / fulfillment-server HTTP API 순차 호출
  → 파이프라인 자동 동작
```

### 시뮬레이션 컨트롤

| 옵션         | 내용                                                  |
| ------------ | ----------------------------------------------------- |
| SAMPLE_SIZE  | 주문 수 제한 (미설정 시 전체 99k)                     |
| SPEED_FACTOR | 시간 압축 배율 (아래 표 참고)                         |
| 시작점       | 특정 날짜부터 replay 가능                             |
| 필터         | 특정 seller_id만 집중 테스트 가능                     |
| 재고 배율    | seed 시 배율 조정으로 oversell 발생 빈도 제어         |

**전체 데이터(99k) 기준 SPEED_FACTOR별 소요 시간**

Olist 데이터 기간: 2016년 10월 ~ 2018년 9월 (약 2년)

| SPEED_FACTOR | 시뮬레이션 소요 시간 | Redis pending TTL | 용도 |
| ------------ | -------------------- | ----------------- | ---- |
| 1000x        | 약 17시간            | 260초             | 장기 안정성 테스트 |
| 10000x       | 약 1.7시간           | 26초              | 대용량 병목 관찰 (권장) |

---

## 7. analytics-server 집계 전략

### seller_performance 인메모리 배치

이벤트마다 직접 UPSERT하면 동일 seller_id 행에 lock 경쟁이 생겨 I/O 병목이 발생한다. analytics-server는 인메모리 버퍼에 델타를 누적하고 5초마다 배치 flush한다.

```typescript
// analytics-server 내부
const buffer = new Map<string, SellerDelta>()

function onEvent(event: KafkaEvent) {
  const delta = buffer.get(event.sellerId) ?? zeroDelta()
  applyDelta(delta, event)
  buffer.set(event.sellerId, delta)
}

setInterval(async () => {
  if (buffer.size === 0) return
  const snapshot = [...buffer.entries()]
  buffer.clear()
  await batchUpsert(snapshot)  // 단일 트랜잭션, INSERT ON CONFLICT DO UPDATE
}, 5000)
```

5초 동안 같은 seller의 이벤트가 메모리에서 합산되어 DB에는 seller당 최대 1회/5초 UPSERT가 발생한다. 서버 재시작 시 버퍼 손실이 가능하나 이 프로젝트 범위에서는 허용한다.

`delivery_stats`는 order 단위 INSERT로 행 충돌이 없으므로 이벤트마다 직접 INSERT한다.

---

## 8. 모니터링

### 스택

```
각 서버 (NestJS)  → Prometheus 메트릭 노출 (/metrics)
Kafka             → kafka-exporter           → Prometheus
RabbitMQ          → 내장 Prometheus 플러그인 → Prometheus
Redis             → redis_exporter           → Prometheus
k6                → 실시간 메트릭 스트리밍    → Grafana
                                                    ↓
                                             Grafana 대시보드
```

### 확인 가능한 메트릭

| 대상     | 메트릭                                                       |
| -------- | ------------------------------------------------------------ |
| Kafka    | consumer lag, partition offset, throughput, 토픽별 메시지 수 |
| RabbitMQ | queue depth, message rate, DLQ 적재량, 셀러별 처리량         |
| Redis    | hit rate, memory usage, command/sec, eviction                |
| 서버     | API latency, error rate, 처리량, DB connection pool          |
| k6       | VU 수, req/s, p95 응답시간, 시나리오별 성공률                |

---

## 9. 레포 전략

**모노레포 (pnpm workspace)**

이벤트 페이로드 타입을 `packages/event-types`로 공유. 서버 간 타입 불일치 방지.

```
krr-pitory/
  apps/
    source-server/
    fulfillment-server/
    analytics-server/
  packages/
    event-types/        이벤트 페이로드 Zod 스키마 + 타입 추론 (3개 서버 공유)
    kafka-client/       공통 Kafka 설정, 토픽 상수
    rabbitmq-client/    공통 RabbitMQ 설정, DLQ 초기화
    redis-client/       공통 Redis 설정, Lua script
    tsconfig/           공통 TypeScript 설정
  scripts/
    seed-reference.ts      고정 참조 데이터 각 서버 DB 적재
    seed-inventory.ts      product별 초기 재고 Redis + inventory 테이블 적재
    prepare-simulation.ts  CSV 조인 + 이벤트 시퀀스 변환 → events.json
  simulation/
    data/
      events.json          prepare-simulation.ts 출력물 (k6 입력)
    k6/
  monitoring/
    prometheus/
    grafana/
  docker-compose.yml
  .env                     SPEED_FACTOR, SAMPLE_SIZE 등 공통 환경변수
```

**packages/event-types 구조**

Zod 스키마를 정의하면 타입 추론과 런타임 검증을 동시에 확보한다. Kafka/RabbitMQ 메시지 파싱 시 형태가 맞지 않으면 즉시 에러 → DLQ 연동 가능.

```typescript
// packages/event-types/src/events.ts
export const OrderCreatedSchema = z.object({
  eventId:    z.string().uuid(),   // source-server가 Kafka 발행 시 생성
  orderId:    z.string(),
  items: z.array(z.object({
    productId: z.string(),
    sellerId:  z.string(),
    price:     z.number(),
  })),
  payment: z.object({
    type:         z.string(),   // credit_card | boleto | voucher | debit_card
    value:        z.number(),
    installments: z.number().int(),
  }),
  occurredAt: z.string().datetime(),
  type:       z.literal('order.created'),
})
export type OrderCreatedEvent = z.infer<typeof OrderCreatedSchema>

export const OrderCanceledSchema = z.object({
  eventId:   z.string().uuid(),
  orderId:   z.string(),
  reason:    z.enum(['customer_request', 'stock_unavailable']),
  items: z.array(z.object({
    productId: z.string(),
    sellerId:  z.string(),
    quantity:  z.number().int(),
  })),
  occurredAt: z.string().datetime(),
  type:       z.literal('order.canceled'),
})
export type OrderCanceledEvent = z.infer<typeof OrderCanceledSchema>

// order.processed, review.created 동일 패턴
```

**pnpm 이슈 대응**

```
# .npmrc
node-linker=hoisted   # 기본 설정. NestJS CLI 호환성 확보
```

---

## 10. 전체 아키텍처 흐름

**정상 주문 흐름 (happy path)**

```
k6 (외부 클라이언트)
  events.json 기반 시간순 HTTP 호출
          │
          ├─ 주문/결제/취소/리뷰 → source-server
          └─ 출하/배송완료       → fulfillment-server
          │
          ↓ (source-server)
┌──────────────────────────────────────────────────────────────────┐
│                         source-server                            │
│                                                                  │
│  POST /orders          → Redis pending (동적 TTL)                │
│  POST /orders/:id/approve → payments 저장 + Kafka order.created  │
│  PATCH /orders/:id/cancel → Kafka order.canceled(customer_req)   │
│  POST /reviews         → order_reviews 저장 + Kafka review.created│
│  RabbitMQ consumer     → notification.review → notification_log  │
└──────────┬──────────────────────────────────────────────────────┘
           │ Kafka: order.created                │ Kafka: review.created
     ┌─────┴──────┐                              │
     ↓            ↓                              ↓
┌─────────────┐  ┌──────────────────────────────────────────────┐
│ fulfillment │  │           analytics-server                   │
│   -server   │  │           (analytics-group)                  │
│             │  │                                              │
│ idem key    │  │  order.created  → seller_performance 버퍼    │
│ Lua         │  │  order.processed→ delivery_stats INSERT      │
│ multi-DECR  │  │                   seller_performance 버퍼    │
│ RabbitMQ    │  │  order.canceled → seller_performance 버퍼    │
│ dispatch    │  │  review.created → seller_performance 버퍼    │
│             │  │                                              │
│ order.      │  │                   (5초마다 배치 flush)        │
│ processed ──┼──────────────────────────────────────────────→  │
│ RabbitMQ    │  └──────────────────────────────────────────────┘
│ notification│
│ .review     │
│ delayed msg │
└──────┬──────┘
       │ k6: PATCH /orders/:id/ship
       │ k6: PATCH /orders/:id/deliver
       ↑ (fulfillment-server가 직접 수신)
```

**취소 / 보상 흐름**

```
[재고 부족 시] — fulfillment-server가 order.canceled 발행
fulfillment-server
  → 멀티키 Lua DECR 실패 감지 (내부 처리, INCR 불필요)
  → order.canceled 발행 (reason: 'stock_unavailable')
      ├── cancel-group (source-server)
      │     → orders status: 'unavailable'
      │     → payments 환불 처리
      │     → notification_log 기록
      └── analytics-group → 취소율 집계

[고객 취소 시] — source-server가 order.canceled 발행
k6 → PATCH /orders/:id/cancel → source-server
  → order.canceled 발행 (reason: 'customer_request')
      ├── cancel-group (source-server)
      │     → orders status: 'canceled'
      │     → payments 환불 처리
      │     → notification_log 기록
      ├── fulfillment-group (fulfillment-server)
      │     → fulfillment_events에서 processing 레코드 확인
      │     → 있으면: 재고 INCR 복구
      │     → 없으면: 스킵 (재고 차감 전 취소)
      └── analytics-group → 취소율 집계
```

---

## 11. 설계 범위와 의도적 단순화

### 실제 풀필먼트 환경과의 비교

| 항목 | 실제 환경 | 이 설계 | 판단 |
| ---- | --------- | ------- | ---- |
| 재고 원자 예약 | order-level all-or-nothing | 멀티키 Lua, 동일 | 일치 |
| 멀티 아이템 재고 처리 | 전체 검사 후 전체 차감 | 멀티키 Lua, 동일 | 일치 |
| Seller dispatch | 메시지 큐 기반 | RabbitMQ, 동일 패턴 | 일치 |
| 서비스 간 디커플링 | Kafka 이벤트 드리븐 | 동일 | 일치 |
| Idempotency, DLQ | 표준 패턴 | 동일 | 일치 |
| Fulfillment Order 엔티티 | seller/창고별 독립 엔티티 존재 | 없음 | 의도적 생략 |
| Ship/Deliver 단위 | seller별 독립 추적 | order 단위 | Olist 데이터 맞춤 단순화 |
| 재고 예약/차감 분리 | soft reservation → hard deduction | DECR 단일 처리 | 단순화, 핵심 동작 동일 |
| Partial fulfillment | 일부만 출고 가능 | 미지원, all-or-nothing | 의도적 생략 |

### Fulfillment Order를 도입하지 않는 이유

실제 대형 커머스(Amazon, Shopify)는 하나의 주문을 seller/창고별 **Fulfillment Order**로 분리해 각각 독립적인 상태 FSM을 갖는다. 우리 설계에는 이 개념이 없다.

도입하지 않는 이유:

- Olist 데이터셋의 `order_delivered_carrier_date`, `order_delivered_customer_date`가 **order 단위 컬럼**이다. 데이터 자체가 seller별 배송 추적을 지원하지 않으므로 도입해도 활용할 데이터가 없다.
- seller별 독립 dispatch는 RabbitMQ `seller.{id}.orders` 큐가 이미 담당한다. Fulfillment Order 엔티티가 없어도 물리적 분리는 동작한다.
- 학습 목표인 "Kafka / RabbitMQ / Redis 역할 분리" 검증에 필요하지 않다.

### 멀티 아이템 처리 방식 결정 배경

멀티 아이템 주문에서 일부 상품만 재고 차감 후 다른 상품이 실패하는 케이스를 처리하는 세 가지 옵션을 검토했다.

- **옵션 A (채택)**: 단일 Lua script 내 전체 검사 후 전체 DECR — 실제 OMS 패턴과 동일, rollback 불필요, Redis 원자성 활용
- **옵션 B**: order_item 단위 이벤트 분리 — 실무에서 쓰지 않는 방식, 집계 복잡도 과도 증가
- **옵션 C**: 단일 seller 가정으로 단순화 — 2~4%의 멀티 셀러 주문에서 oversell 방지 기능이 무력화됨

옵션 A는 실제 재고 예약 패턴과 일치하면서 Redis Lua 원자성을 더 잘 보여주는 구현이기도 하다.

---

## 12. 인프라 구성

```
docker-compose 서비스 목록

애플리케이션:
  source-server       NestJS, PostgreSQL (source-db)
  fulfillment-server  NestJS, PostgreSQL (fulfillment-db)
  analytics-server    NestJS, PostgreSQL (analytics-db)

미들웨어:
  kafka               KRaft 모드 (Zookeeper 없음, bitnami/kafka 또는 confluentinc/cp-kafka)
  rabbitmq            management 플러그인 + Prometheus 플러그인 + delayed_message_exchange 플러그인
  redis               redis:7

모니터링:
  prometheus
  grafana
  kafka-exporter      danielqsj/kafka-exporter (JMX Exporter 대신 사용)
                      Kafka Consumer Group API 직접 폴링 → Java agent 불필요, 설정 단순
                      consumer lag, topic offset, partition 메트릭 제공

시뮬레이션:
  k6                  Olist 데이터 기반 시나리오 실행

공통 환경변수 (.env):
  SPEED_FACTOR        시간 압축 배율 (source-server Redis TTL, k6 이벤트 간격에 동일 적용)
  SAMPLE_SIZE         prepare-simulation.ts 주문 수 제한
```

---

## 13. 향후 로드맵

| 단계         | 내용                                                     |
| ------------ | -------------------------------------------------------- |
| 1단계 (현재) | 모노레포 + docker-compose, 파이프라인 구현 및 검증       |
| 2단계        | k3s 도입, 각 서버 독립 Pod 배포, 스케일링 경험           |
| 3단계        | 모노레포 → 멀티레포 분리, 서비스 디스커버리 적용         |
| 4단계        | 프론트엔드 — 이벤트 흐름 실시간 시각화 (SSE / WebSocket) |

---

## 14. 구현 순서

- [ ] 모노레포 초기 세팅 (pnpm workspace, tsconfig 공통화, .npmrc node-linker=hoisted, .env 공통 환경변수)
- [ ] docker-compose 인프라 구성 (Kafka KRaft, RabbitMQ + 플러그인, Redis, PostgreSQL × 3) + 헬스체크 검증
- [ ] packages/event-types 정의 (Zod 스키마 + 타입 추론: order.created / order.processed / order.canceled / review.created)
- [ ] packages/kafka-client, rabbitmq-client (DLQ 초기화 포함), redis-client (멀티키 DECR/INCR Lua script, decrementStock/incrementStock 함수) 공통 설정
- [ ] scripts/seed-reference.ts (sellers, products, customers, geolocation → 각 서버 DB)
- [ ] scripts/seed-inventory.ts (order_items 빈도 기반 초기 재고 → Redis + inventory 테이블)
- [ ] scripts/prepare-simulation.ts (CSV 조인 + 필터링 + 이벤트 시퀀스 변환 → events.json, target 필드 포함)
- [ ] source-server 구현
  - 주문 API, Redis pending 버퍼 (SPEED_FACTOR 기반 동적 TTL)
  - 결제 승인 API (payments 테이블 저장 + Kafka order.created 발행, eventId UUID 생성)
  - 취소 API (Kafka order.canceled(customer_request) 발행)
  - 리뷰 API (order_reviews 저장 + Kafka review.created 발행)
  - Kafka cancel-group: order.canceled 소비 → 환불 처리 + notification_log 기록
  - RabbitMQ consumer: notification.review 소비 → notification_log 기록
- [ ] fulfillment-server 구현
  - Kafka fulfillment-group: order.created 소비
  - Redis 멀티키 Lua script 재고 차감 (all-or-nothing) + inventory 배치 UPDATE
  - RabbitMQ 셀러별 동적 큐 dispatch (x-expires 설정) + DLQ
  - PATCH /orders/:id/ship: fulfillment_events(shipped) 기록 + Redis 캐시 SET
  - PATCH /orders/:id/deliver: fulfillment_events(delivered) 기록 + Kafka order.processed 발행 + RabbitMQ notification.review delayed 발행
  - Kafka order.canceled(customer_request) 소비 → fulfillment_events processing 레코드 확인 후 조건부 멀티키 INCR
  - Kafka order.canceled(stock_unavailable) 소비 → 스킵 (내부에서 이미 처리 완결)
- [ ] analytics-server 구현
  - Kafka analytics-group: 모든 토픽 소비
  - delivery_stats: order.processed 소비 시 직접 INSERT
  - seller_performance: 인메모리 버퍼 누적 + 5초마다 배치 flush
- [ ] k6 시뮬레이션 스크립트 (events.json SharedArray 로드, SPEED_FACTOR 시간 압축 재생, target 필드 기반 라우팅)
- [ ] Prometheus + Grafana 모니터링 구성
- [ ] 통합 테스트 및 파이프라인 검증 (oversell 시나리오, DLQ 적재, consumer lag, 취소 보상 파이프라인, 재고 차감 전 취소 케이스 확인)
