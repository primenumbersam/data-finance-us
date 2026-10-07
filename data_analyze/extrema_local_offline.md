# Offline Local Extrema Detection & Ground-Truth Labeling Algorithm

## 1. 목적 및 분석 배경 (Background & Objectives)
뉴스 텍스트 데이터와 거시 경제 지표를 활용하는 사전적 인공지능 의사결정 모델(AI Decision Models, e.g. OpenJev, Nimble)의 주간 지수 방향성(Week-Ahead Direction) 예측 성능을 엄밀하게 검증하기 위해 사후적 국소 극값(Local Extrema) 라벨링을 수행한다.

실시간 매매 시점에서는 미래 데이터를 참조할 수 없으나, 머신러닝 모델의 사전적 예측 타깃(Ground Truth Target)을 구축하는 오프라인(Offline) 환경에서는 사후적 전체 시계열을 기반으로 명확한 파동의 고점(Peak)과 저점(Trough)을 노이즈 없이 분리해내는 것이 중요하다. 본 알고리즘은 21개 일봉 윈도우와 ATR(Average True Range) 동적 임계치를 결합하여 통계적으로 유의미한 거시 스윙 변곡점을 라벨링한다.

---

## 2. 알고리즘 수식 및 상태 전이 규칙 (Mathematical Formulation)

### 2.1 기본 파라미터 (Hyperparameters)
- Left Bars (L): 10 (좌측 확인 일봉 수)
- Right Bars (R): 10 (우측 확인 일봉 수)
- ATR Length (N): 10 (변동성 측정 기간)
- ATR Multiplier (M): 1.5 (최소 변동폭 임계치 배수)

### 2.2 지표 계산식
1. True Range (TR):
   TR_t = max(High_t - Low_t, |High_t - Close_{t-1}|, |Low_t - Close_{t-1}|)

2. Average True Range (ATR, Wilder Smoothing):
   ATR_t = (ATR_{t-1} * (N - 1) + TR_t) / N   (alpha = 1 / N)

3. 피벗 고점/저점 후보 판정 (Window = L + R + 1 = 21):
   - Pivot High: High_p >= High_k  (for all k in [p - L, p + R])
   - Pivot Low: Low_p <= Low_k   (for all k in [p - L, p + R])
   (현재 시점 i에서 R일 지연된 p = i - R 시점의 봉에 대해 확정 판정)

### 2.3 상태 머신 및 탐욕적 갱신 규칙 (State Machine & Greedy Update)
극점 이력 배열 ExtremaHistory = [(t_0, P_0, Type_0), (t_1, P_1, Type_1), ...] 을 유지하며 고점(+1)과 저점(-1)이 반드시 교대로 나타나도록 통제한다.

1. 고점 후보(Candidate High, P_c) 수신 시:
   - 이력이 비어있으면: (p, P_c, +1) 등록.
   - 마지막 극점이 고점(+1)인 경우:
     기존 등록 고점보다 더 높은 고점(P_c > P_last)이 나타나면 해당 극점의 위치와 가격을 새 고점으로 갱신(Greedy Peak Elevation).
   - 마지막 극점이 저점(-1)인 경우:
     가격 변동폭 Delta = |P_c - P_last| >= ATR_p * M 조건을 만족할 때만 새로운 고점(+1)으로 확정 등록. 미달 시 미세 반등 노이즈로 간주하여 기각.

2. 저점 후보(Candidate Low, P_c) 수신 시:
   - 이력이 비어있으면: (p, P_c, -1) 등록.
   - 마지막 극점이 저점(-1)인 경우:
     기존 등록 저점보다 더 낮은 저점(P_c < P_last)이 나타나면 해당 극점의 위치와 가격을 새 저점으로 갱신(Greedy Trough Lowering).
   - 마지막 극점이 고점(+1)인 경우:
     가격 변동폭 Delta = |P_c - P_last| >= ATR_p * M 조건을 만족할 때만 새로운 저점(-1)으로 확정 등록. 미달 시 미세 조정 노이즈로 간주하여 기각.

---

## 3. 라벨링 컬럼 규격 (Dataset Label Schema)
본 알고리즘 적용 시 일봉 지수 테이블(indices.parquet)에 다음 피처가 생성된다.

1. extrema_type (INT8):
   - +1: 확인된 국소 고점 (Local Peak)
   - -1: 확인된 국소 저점 (Local Trough)
   - 0: 일반 구간 (Non-extrema)

2. extrema_price (FLOAT64):
   - 극점 발생일의 극값 가격 (High 또는 Low), 비극점일은 NaN.

3. swing_regime (INT8):
   - +1: 직전 저점에서 다음 고점으로 향하는 거시 상승 확장 국면 (Bullish Swing)
   - -1: 직전 고점에서 다음 저점으로 향하는 거시 하락 수축 국면 (Bearish Swing)

4. next_extrema_type (INT8):
   - 현재 시점 이후 가장 먼저 도달할 차기 극점의 성격 (+1 또는 -1).

5. bars_to_next_extrema (INT32):
   - 차기 극점 도달까지 남은 잔여 거래일수.

6. return_to_next_extrema (FLOAT64):
   - 현재 종가 대비 차기 극점 가격까지의 누적 기대 수익률: (P_next - Close_t) / Close_t.

7. week_ahead_direction (INT8):
   - 5거래일(1주일) 후 종가 방향성: sign(Close_{t+5} - Close_t).
   - +1: 상승 (Close_{t+5} > Close_t)
   - -1: 하락 (Close_{t+5} < Close_t)
   - 0: 보합

---

## 4. 실증 통계 특성 (Empirical Properties, 1999~2026)
S&P 500 (^GSPC), 나스닥 100 (^NDX), 다우존스 (^DJI)에 적용한 실증 결과:
- 극점 간 평균 간격: 약 22.5 거래일 (약 4.5주)
- 극점 간 중앙값 간격: 16.0 거래일 (약 3.2주)
- 5거래일(1주일) 미만 미세 파동 제거율: 98.4%
- 연평균 극점 발생 빈도: 약 11~12회 (주 단위 예측 모델의 타깃으로 적합한 매크로 사이클 형성)
