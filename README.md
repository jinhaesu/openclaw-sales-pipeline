# OpenClaw Sales Pipeline

OpenClaw가 이미 가지고 있는 채널 마스터를 읽어서, 판매 채널 매출 취합 작업을 `API 우선 + 병렬 실행 + 공통 결과 스키마` 구조로 옮기기 위한 로컬 보조 프로젝트다.

## 목표
- 40개 안팎 채널을 동일한 방식으로 다루지 않는다.
- API 가능 채널은 API 수집기로 보낸다.
- 브라우저/다운로드 중심 채널은 플레이북 기반 수집기로 보낸다.
- LLM은 예외 상황에만 쓰고, 반복 동작은 스크립트로 옮긴다.
- OpenClaw의 기존 채널 구조화 파일을 그대로 재사용한다.

## 현재 포함 범위
- OpenClaw 채널 마스터 로더
- 채널별 실행 전략 분류기
- 병렬 실행 계획기
- 공통 결과 스키마
- 플레이북 예시
- Dry-run CLI
- API 키 로더
- Playwright 브라우저 수집기 뼈대
- 채널별 collector 레지스트리

## 디렉터리 구조
```text
src/openclaw_sales_pipeline/
  cli.py
  config.py
  models.py
  orchestrator.py
  secrets.py
  collectors/
    base.py
    api.py
    browser.py
    registry.py
playbooks/
  smartstore.json
  cafe24.json
  coupang_wing.json
config/
  runtime.example.json
  secrets.example.json
run_outputs/
```

## 빠른 시작
```bash
cd "/Users/joinerhs/Documents/New project"
python3 -m src.openclaw_sales_pipeline.cli plan --date 2026-04-08
python3 -m src.openclaw_sales_pipeline.cli run --date 2026-04-08 --dry-run
python3 -m src.openclaw_sales_pipeline.cli run --date 2026-04-08 --channel 스마트스토어 --channel 쿠팡\ WING --dry-run
python3 -m src.openclaw_sales_pipeline.cli validate
```

## 런타임 설정
예시 설정은 [`/Users/joinerhs/Documents/New project/config/runtime.example.json`](/Users/joinerhs/Documents/New%20project/config/runtime.example.json)에 있다.

핵심 설정:
- `master_path`: OpenClaw 채널 마스터 JSON 경로
- `api_concurrency`: API 채널 병렬 수
- `browser_concurrency`: 브라우저 채널 병렬 수
- `manual_concurrency`: 수동 개입 채널 병렬 수
- `artifact_root`: 결과물 저장 루트
- `secrets_path`: 로컬 API 키 설정 파일
- `session_state_root`: Playwright storage state 저장 루트

## 실행 모드
- `plan`: 오늘 어떤 채널을 어떤 전략으로 돌릴지 미리 보여준다.
- `run --dry-run`: 실제 호출 없이 병렬 실행 계획과 출력 경로를 만든다.

## 비밀키 설정
예시 파일은 [`/Users/joinerhs/Documents/New project/config/secrets.example.json`](/Users/joinerhs/Documents/New%20project/config/secrets.example.json)에 있다.

실제 파일은 예를 들면 `config/secrets.local.json`으로 두고 다음처럼 관리한다.
```json
{
  "smartstore": {
    "client_id": "your-client-id",
    "client_secret": "your-client-secret"
  },
  "cafe24": {
    "mall_id": "your-mall-id",
    "client_id": "your-client-id",
    "client_secret": "your-client-secret",
    "access_token": "optional-bootstrap-token"
  },
  "coupang": {
    "access_key": "your-access-key",
    "secret_key": "your-secret-key",
    "vendor_id": "A00000000"
  }
}
```

## Playwright
브라우저 수집기는 Python Playwright를 기준으로 설계했다.

설치 예시:
```bash
python3 -m pip install playwright
python3 -m playwright install chromium
```

현재 구현은 다음을 지원한다.
- 세션 state 경로 생성
- 플레이북 기반 메타데이터 출력
- 실제 selector 액션을 넣을 수 있는 collector 뼈대
- `goto`, `screenshot`, `note`, `wait_for_timeout` 브라우저 액션 실행

## 구현 상태
- API collector:
  - provider별 요청 매니페스트 생성
  - 비밀키 존재 여부 검증
  - 키가 없으면 `missing_credentials`로 종료
- Browser collector:
  - Playwright가 있으면 세션 state 저장
  - 플레이북 액션을 실행하고 결과를 저장
- Validation:
  - 플레이북/비밀키/브라우저 액션 커버리지를 한 번에 점검

## 다음 확장 포인트
- Playwright 기반 브라우저 수집기 추가
- API 키 로더 추가
- 엑셀/CSV 파서 추가
- 이상치 검증 규칙 추가
- Telegram 인증코드 요청 브리지 추가
