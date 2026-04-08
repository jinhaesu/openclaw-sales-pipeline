# OpenClaw Sales API Strategy (2026-04-08)

## 1. API 전환 우선순위

### A. 바로 API 전환 가능한 채널
- 스마트스토어
  - 근거: 네이버 커머스API는 주문, 정산, 판매자정보, 통계(API데이터솔루션) 문서를 공식 제공한다.
  - 확보 방법:
    1. 커머스API센터 가입
    2. 애플리케이션 등록
    3. OAuth 2.0 client credentials 토큰 발급
    4. 판매자 인증/솔루션 사용 승인 진행
  - 비고: 일부 통계/마케팅 데이터는 솔루션 구독 또는 브랜드스토어 조건이 붙을 수 있다.

- 카페24
  - 대상 채널: 카페24, 카페24 공동구매, 파트너스몰(카페24)
  - 근거: Cafe24 Admin API는 OAuth 2.0 기반이며 Orders, Sales statistics 등을 공식 제공한다.
  - 확보 방법:
    1. Cafe24 Developers에서 앱 생성
    2. 쇼핑몰별 OAuth 승인
    3. Admin API 토큰 발급
    4. 주문/통계 엔드포인트 연결

- 쿠팡
  - 대상 채널: 쿠팡 WING, 쿠팡 로켓프레시
  - 근거: Coupang OPEN API는 WING 판매자 대상으로 OpenAPI Key 발급 절차와 주문/상품 API를 공식 제공한다.
  - 확보 방법:
    1. WING 판매자 인증 완료
    2. WING에서 OPEN API Key 발급
    3. access key / secret key / vendorId 확보
    4. HMAC 서명 방식으로 호출
  - 비고: 로켓프레시는 일반 WING API 범위와 세부 권한이 다를 수 있으므로 실제 조회 가능 범위는 별도 확인 필요

- 11번가
  - 근거: 11번가 OPEN API CENTER를 통한 API KEY 발급 흐름이 공식/연동 문서에서 확인된다.
  - 확보 방법:
    1. 11번가 셀러 가입 및 셀러 전환
    2. OPEN API CENTER 서비스 등록
    3. API KEY 발급
    4. 필요시 접속 권한/IP 등록

- G마켓 / 옥션
  - 근거: ESM Trading API에서 주문/정산 관련 API와 사용 신청 가이드를 공식 제공한다.
  - 확보 방법:
    1. G마켓/옥션 판매자 및 ESM+ 마스터 ID 준비
    2. API 사용 신청 메일 또는 절차 진행
    3. 승인 후 API 범위 부여
    4. 주문/정산 조회 API 연결
  - 비고: “누구나 즉시 발급”이 아니라 승인형에 가깝다.

### B. 공식 API는 있으나 제휴/승인형 성격이 강한 채널
- 카카오선물하기
- 카카오톡스토어
  - 근거: 카카오쇼핑 Open API는 선물하기/톡스토어의 상품/주문/문의/정산 API를 공식 제공한다.
  - 확보 방법:
    1. 카카오쇼핑 API 연동 검토 요청
    2. 카카오계정 2개 준비
    3. 판매채널 입점
    4. 카카오 디벨로퍼스에서 REST API KEY / ADMIN KEY 발급
    5. 판매자 API 키와 판매채널 연결
  - 비고: “모든 판매자에게 즉시 제공”이 아니라 검토/계약/진행 가능여부 회신 구조다.

### C. API 가능성이 있으나 현재 공식 공개 경로를 바로 확인하지 못한 채널
- SSG
- GS SHOP
- NS mall
- 신세계TV쇼핑
- CJ온스타일
- 롯데온
- 롯데홈쇼핑
- 올리브영
- 컬리
- B마트
- 알리익스프레스

비고:
- 이 그룹은 실제 판매자센터 안의 API 메뉴 또는 제휴 계약 기반 API일 가능성이 높다.
- 다만 2026-04-08 기준 공개 웹에서 공식 문서를 바로 확인한 것은 아니므로, 운영상 “있을 수 있음”으로 보고 판매자센터에서 우선 확인하는 것이 안전하다.

### D. 매출 취합 관점에서 API 전환 우선순위가 낮거나 비현실적인 채널
- GS25
- CU
- 세븐일레븐
- 홈플러스
- 삼성웰스토리
- 아워홈
- CJ프레시웨이
- 이마트(노브랜드) 발주시스템
- 파르나스(호텔)
- 토스
- 카카오스타일
- 이지웰
- 삼성카드
- 농협
- 베네피아
- 에이블리
- 히티
- T딜
- 올웨이즈

비고:
- 이 채널들은 로그인 후 화면/엑셀 다운로드/정산 파일 기반 운영일 가능성이 높다.
- 공개 API보다 파일 자동 수집, 브라우저 세션 자동화, RPA/API 혼합형 구조가 현실적이다.

## 2. 가장 먼저 확보할 API

### 1순위
- 스마트스토어
- 카페24 계열
- 쿠팡 WING
- 11번가
- G마켓/옥션

이유:
- 주문/정산/매출 데이터가 구조화돼 있을 가능성이 높다.
- 인증만 확보하면 브라우저 OCR/스크린샷 의존도를 크게 줄일 수 있다.
- 40개 채널 중 거래량 비중이 큰 채널일 가능성이 높다.

### 2순위
- 카카오선물하기
- 카카오톡스토어
- SSG

이유:
- API가 있거나 있을 가능성이 높지만 승인/제휴/설정 장벽이 있다.

## 3. 병렬 취합 구조 제안

### 현재보다 나은 구조
1. 채널별 수집기를 분리한다.
   - `collector_naver`
   - `collector_cafe24`
   - `collector_coupang`
   - `collector_11st`
   - `collector_esm`
   - `collector_browser_manual`

2. 수집 실행을 병렬화한다.
   - API 채널은 동시 실행
   - 브라우저 채널은 인증 충돌을 피하기 위해 제한 병렬
   - 예: API 8개 동시, 브라우저 2~3개 동시

3. 결과를 공통 스키마로 적재한다.
   - `channel`
   - `business_date`
   - `gross_sales`
   - `net_sales`
   - `orders`
   - `cancel_amount`
   - `refund_amount`
   - `fees`
   - `download_source`
   - `evidence_path`
   - `status`
   - `error_message`

4. 마지막에 집계기 하나가 전체 합산/검증을 한다.

### 권장 실행 계층
- 1층: 스케줄러/오케스트레이터
  - 채널별 작업 큐 생성
- 2층: 채널 수집기
  - API 수집기
  - 브라우저 수집기
  - 파일 파서
- 3층: 정규화/검증
- 4층: 집계/보고

## 4. 스크린샷 + LLM 의존도를 줄이는 방법

### 바꿔야 할 원칙
- 반복 메뉴 탐색은 매번 “보고 판단”하지 않는다.
- 한 번 성공한 경로는 스크립트와 선택자(selector)로 고정한다.
- 화면 이해가 필요한 예외에만 LLM을 사용한다.

### 추천 방식
- Playwright로 브라우저 자동화
  - 로그인
  - 메뉴 이동
  - 다운로드 버튼 클릭
  - 파일 저장 확인
- 다운로드 파일은 Python 파서로 정제
  - xlsx
  - csv
  - xls
  - html table
- LLM은 예외 처리에만 사용
  - 페이지 구조 변경 감지
  - 사람이 만든 텍스트 설명을 실행 계획으로 바꾸기
  - 실패 원인 요약

### 경로 학습 방식
- 채널별 YAML/JSON 플레이북 작성
  - 로그인 URL
  - 메뉴 경로
  - 필수 입력값
  - 인증 필요 여부
  - 다운로드 버튼 selector
  - 결과 파일명 패턴
- 예:
  - `playbooks/gs25.yaml`
  - `playbooks/coupang_wing.yaml`
  - `playbooks/smartstore.yaml`

## 5. 40개 채널을 더 안정적으로 돌리는 핵심 아이디어

### A. 채널을 4개 군으로 나눈다
- API 채널
- 다운로드 채널
- 조회 전용 채널
- 인증/수동 개입 채널

이렇게 나눠야 “전 채널 동일 방식”에서 오는 불안정성을 줄일 수 있다.

### B. 브라우저는 세션 저장을 기본으로 한다
- 매일 다시 로그인하지 말고 채널별 storage state를 저장한다.
- 문자 인증은 세션 만료 채널에만 쓰게 만든다.

### C. 다운로드는 중앙 폴더로 모은다
- 날짜별 다운로드 폴더
- 채널별 하위 폴더
- 파일명 규칙 통일

예:
- `downloads/2026-04-08/smartstore/...`
- `downloads/2026-04-08/coupang/...`

### D. 결과 검증 규칙을 만든다
- 전일 대비 증감률 임계치
- 주문수 0인데 매출 있음 여부
- 음수 정산 여부
- 다운로드 파일 없음 여부

이상치만 사람이 보면 된다.

### E. 실패를 숨기지 말고 상태값으로 남긴다
- success
- partial
- auth_required
- selector_changed
- api_denied
- data_missing

## 6. 현실적인 로드맵

### Phase 1
- 스마트스토어 API
- 카페24 API
- 쿠팡 WING API
- 11번가 API
- G마켓/옥션 API 조사 및 신청

### Phase 2
- 브라우저 자동화 플레이북 작성
- 다운로드 자동 분류
- 세션 저장
- Telegram 인증 코드 승인 흐름 연결

### Phase 3
- 병렬 수집 오케스트레이터 구축
- 결과 검증/대사 자동화
- 공헌이익 계산 파이프라인 연결

## 7. 공식 문서 링크
- 네이버 커머스API 소개: https://apicenter.commerce.naver.com/docs/introduction
- 네이버 커머스API 문서: https://apicenter.commerce.naver.com/docs/commerce-api/current
- Cafe24 Admin API: https://developers.cafe24.com/docs/en/api/admin/
- Coupang OPEN API: https://developers.coupangcorp.com/hc/en-us/articles/360033917473-Coupang-OPEN-API
- Coupang API 키 발급 안내: https://developers.coupangcorp.com/hc/en-us/articles/360033980613-Issue-OPEN-API-Key
- 11번가 OPEN API 센터 참고 링크(연동 문서에서 확인): https://openapi.11st.co.kr/openapi/OpenApiFrontMain.tmall
- ESM Trading API: https://etapi.gmarket.com/
- ESM API 가이드: https://etapi.gmarket.com/pages/API-%EA%B0%80%EC%9D%B4%EB%93%9C
- 카카오쇼핑 Open API 안내: https://shopping-developers.kakao.com/hc/ko/articles/4681097907087-%EC%B9%B4%EC%B9%B4%EC%98%A4%EC%87%BC%ED%95%91-Open-API-%EC%95%88%EB%82%B4
- 카카오쇼핑 API 시작 가이드: https://shopping-developers.kakao.com/hc/ko/sections/4413918981263-%EC%B9%B4%EC%B9%B4%EC%98%A4%EC%87%BC%ED%95%91-API-%EC%8B%9C%EC%9E%91-%EA%B0%80%EC%9D%B4%EB%93%9C
