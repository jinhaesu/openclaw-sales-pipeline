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
python3 -m src.openclaw_sales_pipeline.cli build-knowledge
python3 -m src.openclaw_sales_pipeline.cli analyze-file --file /path/to/download.xlsx
python3 -m src.openclaw_sales_pipeline.cli discover-browser --date 2026-04-08 --channel GS25
python3 -m src.openclaw_sales_pipeline.cli report-bundle --input-root run_outputs --date-from 2026-04-01 --date-to 2026-04-08 --output-dir artifacts/report_bundles/april_week2
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
- `discover-browser`: 플레이북 액션을 실행한 뒤 현재 페이지, 프레임, 링크, 텍스트 구조를 덤프해서 selector 기준을 빠르게 맞춘다.
- `report-bundle`: 다운로드 파일과 분석 JSON을 모아 통합 엑셀 리포트, 요약 문서, 메일 초안을 만든다.

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
  },
  "smtp": {
    "host": "smtp.example.com",
    "port": 587,
    "username": "report@example.com",
    "password": "replace-me",
    "from_addr": "report@example.com",
    "use_tls": true,
    "use_ssl": false
  }
}
```

현재 로컬에는 [`/Users/joinerhs/Documents/New project/config/secrets.local.json`](/Users/joinerhs/Documents/New%20project/config/secrets.local.json) 템플릿도 만들어뒀다.

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
- `click_text`, `click_role`, `fill_label`, `fill_name`, `fill_credential`, `click_alt`, `eval` 액션 지원
- `click_selector`, `fill_selector`, `type_selector`, `click_text_in_frame`, `assert_frame_url_contains`, `eval_dump` 액션 지원

## 구현 상태
- API collector:
- provider별 요청 매니페스트 생성
- 비밀키 존재 여부 검증
- 키가 없으면 `missing_credentials`로 종료
- 스마트스토어: OAuth 토큰 발급 + 데이터셋 호출 구조 구현
- 카페24: refresh token/access token 기반 호출 구조 구현
- 쿠팡: HMAC 서명 기반 호출 구조 구현
- Browser collector:
  - Playwright가 있으면 세션 state 저장
  - 플레이북 액션을 실행하고 결과를 저장
- Browser discovery:
  - 로그인 이후 페이지/프레임/링크/텍스트 구조를 파일로 저장
  - 여러 채널 selector 기준을 빠르게 맞출 때 사용
- GS25/CU/세븐일레븐: 텍스트 클릭 기반 브라우저 액션 1차 적용
- Workflow knowledge:
  - OpenClaw 채널 마스터 + 영상 지원 + 플레이북 커버리지를 하나의 JSON으로 생성
- File analysis:
  - 다운로드한 CSV/XLSX를 읽고 품목별 판매량/매출 요약 생성
- Reporting:
  - 다운로드 파일/분석 JSON을 모아 통합 레코드 생성
  - 일별 채널 매출, 월별 채널 매출, 품목별 매출, 품목별 판매량, 채널별 품목 매출 엑셀 시트 생성
  - 요약 Markdown과 `.eml` 메일 초안 생성
  - SMTP 설정이 있으면 실제 메일 발송 가능
- Validation:
  - 플레이북/비밀키/브라우저 액션 커버리지를 한 번에 점검

## 다음 확장 포인트
- Playwright 기반 브라우저 수집기 추가
- API 키 로더 추가
- 엑셀/CSV 파서 추가
- 이상치 검증 규칙 추가
- Telegram 인증코드 요청 브리지 추가

## 확인 프롬프트 최소화 운영 모드
- 가능하면 `validate`, `build-knowledge`, `discover-browser`, `run --dry-run` 같은 비대화형 명령을 먼저 사용한다.
- 브라우저 채널은 `discover-browser`로 메뉴 구조를 먼저 수집한 뒤 selector를 고정한다.
- OpenClaw 쪽에는 `sales_ops/openclaw_pipeline_bridge.md`가 연결돼 있어서, 반복 작업은 이 프로젝트 명령을 우선 참고하도록 해두었다.
- 사용자 확인이 꼭 필요한 경우는 인증코드, 계정 권한, 실제 destructive 작업 정도로만 제한한다.

## 리포트 번들 운영 예시
다운로드 파일을 각 채널 결과 디렉터리 아래에 둔 뒤 다음처럼 실행하면 된다.

```bash
python3 -m src.openclaw_sales_pipeline.cli report-bundle \
  --input-root run_outputs \
  --date-from 2026-04-01 \
  --date-to 2026-04-08 \
  --output-dir artifacts/report_bundles/april_week2 \
  --email-to finance@example.com \
  --email-to sales@example.com
```

생성 결과:
- 통합 엑셀 리포트
- 요약 Markdown
- 소스 manifest JSON
- 첨부가 포함된 `.eml` 메일 초안

SMTP 설정이 있으면 `--send-email`을 추가해 바로 발송할 수 있다.
