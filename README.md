# KRX 액티브 ETF PDF 비중 모니터

이 폴더는 기존 FunETF 방식과 별개로 새로 만든 KRX 마켓데이터 로그인 방식입니다.

## 중요한 파일

- `krx_etf_monitor.py`: 실제 DB 업데이트 프로그램
- `setup_krx_login.bat`: KRX 아이디/비밀번호를 Windows 자격 증명 저장소에 저장하는 버튼
- `run_krx_etf_monitor.bat`: 매일 실행할 버튼
- `config.json`: DB 위치, 전체/일부 ETF 설정, 변화 기준 설정
- `.env`: 텔레그램 토큰과 채팅 ID. GitHub에 올리면 안 됩니다.
- `data/krx_active_etf_holdings.sqlite`: SQLite DB 파일

## 처음 한 번만 할 일

1. `setup_krx_login.bat` 더블클릭
2. KRX 마켓데이터 아이디 입력
3. KRX 마켓데이터 비밀번호 입력

비밀번호는 이 폴더 파일에 저장하지 않고 Windows 자격 증명 저장소에 저장합니다.

## 매일 실행

`run_krx_etf_monitor.bat` 더블클릭

실행하면 KRX에서 액티브 ETF PDF 구성종목을 수집하고 DB에 저장한 뒤 텔레그램으로 결과를 보냅니다.

## 직접 PowerShell에서 실행

```powershell
cd "C:\Users\se2in\Desktop\destiny\ETF_KRX"
python .\krx_etf_monitor.py run --send-telegram --sleep 0.2
```

## 목록만 확인

```powershell
python .\krx_etf_monitor.py discover
```

## DB 위치

`config.json`의 이 줄입니다.

```json
"database_path": "data/krx_active_etf_holdings.sqlite"
```

## GitHub에 올리면 안 되는 파일

- `.env`
- `.env.txt`
- `data/`
- `.matplotlib-cache/`

## GitHub Pages 공개

다른 사람이 웹에서 보게 하려면 GitHub 저장소 Settings - Pages에서 다음처럼 설정합니다.

- Source: Deploy from a branch
- Branch: main
- Folder: /docs

매일 자동 실행은 `run_collect_publish.bat`가 `reports/latest_changes.html`을 `docs/index.html`로 복사한 뒤 GitHub에 push합니다.

처음 한 번만:

1. GitHub에서 빈 저장소 생성
2. `setup_github_publish.bat` 실행
3. GitHub 저장소 URL 입력
4. GitHub 저장소 Settings - Pages에서 main / docs 선택
5. `install_daily_publish_tasks.bat` 실행

그 다음부터는 매일 08:35, 09:05에 자동으로 수집/텔레그램/HTML/GitHub 업로드가 진행됩니다.
