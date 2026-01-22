## ClickHouse Sink Exactly-once 구현의 어려움
```declarative
# JDBC Sink 성능 한계
JDBC Sink는 행 기반 + DB 커밋 중심 구조라 ClickHouse의 컬럼 '지향 + 대량 블록 입력' 과 맞지 않아 효율성이 떨어짐

# Exactly-once가 안 되는 이유
ClickHouse는 '트랜잭션 롤백과 체크포인트 연동 커밋'이 불가능한 DB

# 해결 방안
ClickHouse 테이블 설계로 “결과적으로 exactly-once” 가능
1) ReplacingMergeTree engine 활용하여 동일 PK → 최신 row만 유지
2) SummingMergeTree engine 활용하여 중복 INSERT → 자동 합산
3) 항상 고유 key값 포함
```


## ClickHouse Sink 비교: HTTP API vs JDBC
```declarative
| 항목                 | Kafka → Flink → **HTTP API** → ClickHouse | Kafka → Flink → **JDBC** → ClickHouse |
| ------------------   | ----------------------------------------- | ------------------------------------- |
| 처리 속도 (Throughput) | ⭐⭐⭐⭐                                 | ⭐⭐⭐                               |
| 지연(latency)         | ⭐⭐⭐                                   | ⭐⭐⭐⭐                             |
| 확장성 (Scale-out)    | ⭐⭐⭐⭐                                  | ⭐⭐                                 |
| ClickHouse 친화도     | ⭐⭐⭐⭐                                  | ⭐⭐                                 |
| 장애 복원력            | ⭐⭐⭐⭐                                  | ⭐⭐                                 |
| backpressure 대응     | ⭐⭐⭐⭐                                 | ⭐⭐                                  |
| 구현 난이도            | ⭐⭐                                      | ⭐                                    |
| 운영 난이도            | ⭐⭐                                      | ⭐⭐                                  |
| exactly-once         | ❌                                        | ❌                                    |
| 실무 대규모 사용        | 많음                                       | 제한적                                 |

# 실무에서 사용
작을 때 JDBC
커지면 HTTP
아주 크면 Kafka Engine -> ClickHouse
```


## Flink SinkFunction vs RichSinkFunction
사용 목적
- 외부 시스템과 연결하여 Connection생성/재사용/Close
- 리소스 생명주기
- 병렬성(subtask) 기반 제어가 필요한 경우
```declarative
SinkFunction의 실행 흐름
TaskManager 시작
└─ Sink 인스턴스 생성 (직렬화/역직렬화)
└─ invoke()   ← 레코드 1건
└─ invoke()
└─ invoke()
(끝났는지 모름)
Task 종료

RichSinkFunction의 실행 흐름
TaskManager 시작
 └─ Sink 인스턴스 생성 (직렬화/역직렬화)
 └─ open()         ← 여기서 커넥션 생성
     └─ invoke()   ← 레코드 1건
     └─ invoke()
     └─ invoke()
 └─ close()        ← 여기서 flush / close
Task 종료

-> 한 번 연결해서 오래 쓰고, subtask 마다 lifecycle hook 관리가 가능
```