@echo off
chcp 65001 > nul
title 신제품 인텔리전스 대시보드 구동기

:: 1. 현재 배치 파일이 위치한 폴더로 이동
cd /d "%~dp0"

echo ======================================================
echo   신제품 인텔리전스 대시보드를 시작합니다.
echo ======================================================
echo.

:: 2. CSV 파일 변경사항 최신 Parquet로 자동 변환 (필요 시)
echo [1/3] 원본 데이터(CSV) 변환 및 최적화 확인 중...
python convert.py
echo.

:: 3. 2초 대기 후 브라우저 자동 실행 (백그라운드)
echo [2/3] 웹 브라우저 대시보드 페이지 호출 중...
start "" timeout /t 2 /nobreak > nul ^& start http://127.0.0.1:8000

:: 4. FastAPI Uvicorn 서버 실행
echo [3/3] 대시보드 로컬 서버(포트: 8000)를 구동합니다...
echo.
echo ※ 대시보드를 종료하려면 이 창을 닫거나 Ctrl+C 를 누르세요.
echo ======================================================
python -m uvicorn main:app --port 8000 --reload

pause