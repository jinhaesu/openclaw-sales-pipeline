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

## 디렉터리 구조
```text
src/openclaw_sales_pipeline/
  cli.py
  config.py
  models.py
  orchestrator.py
playbooks/
  smartstore.json
  cafe24.json
  coupang_wing.json
config/
  runtime.example.json
run_outputs/
```

## 빠른 시작
```bash
cd "/Users/joinerhs/Documents/New project"
python3 -m src.openclaw_sales_pipeline.cli plan --date 2026-04-08
python3 -m src.openclaw_sales_pipeline.cli run --date 2026-04-08 --dry-run
```

## 런타임 설정
예시 설정은 [`/Users/joinerhs/Documents/New project/config/runtime.example.json`](/Users/joinerhs/Documents/New%20project/config/runtime.example.json)에 있다.

핵심 설정:
- `master_path`: OpenClaw 채널 마스터 JSON 경로
- `api_concurrency`: API 채널 병렬 수
- `browser_concurrency`: 브라우저 채널 병렬 수
- `manual_concurrency`: 수동 개입 채널 병렬 수
- `artifact_root`: 결과물 저장 루트

## 실행 모드
- `plan`: 오늘 어떤 채널을 어떤 전략으로 돌릴지 미리 보여준다.
- `run --dry-run`: 실제 호출 없이 병렬 실행 계획과 출력 경로를 만든다.

## 다음 확장 포인트
- Playwright 기반 브라우저 수집기 추가
- API 키 로더 추가
- 엑셀/CSV 파서 추가
- 이상치 검증 규칙 추가
- Telegram 인증코드 요청 브리지 추가
