from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
import duckdb
import os
import io
import csv
from collections import Counter
import re

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

db = duckdb.connect()
db.execute("SET memory_limit = '256MB';")
db.execute("SET threads = 1;")

# 🟢 식품 타겟 품목 (B2C 완제품 33개)
TARGET_FOOD_PTYPES = {
    "과자", "캔디류", "초콜릿가공품", "추잉껌", "빵류", "초콜릿", "밀크초콜릿", "준초콜릿", 
    "빙과", "당류가공품", "땅콩 또는 견과류가공품", 
    "만두", "어육소시지", "식육함유가공품", "즉석조리식품", "신선편의식품", "소스", 
    "마요네즈", "토마토케첩", "복합조미식품", 
    "고형차", "커피", "과.채주스", "혼합음료", "유산균음료", "액상차", "과.채음료", 
    "기타 영.유아식", "영아용 조제식", "성장기용 조제식", "임산.수유부용 식품", 
    "기타가공품", "두류가공품"
}

# 🔴 축산물 타겟 품목 (B2C 완제품 15개)
TARGET_MEAT_PTYPES = {
    "비유지방아이스크림", "샤베트", "아이스밀크", "아이스크림", "가공유", "농후발효유", 
    "발효유", "영아용 조제유", "우유", "유당분해우유", "유산균첨가우유", 
    "베이컨류", "분쇄가공육제품", "소시지", "프레스햄"
}

def get_file_path(data_type: str):
    return os.path.join(BASE_DIR, f"data_{data_type}.parquet")

def query_to_dict(cursor, query: str):
    res = cursor.execute(query)
    columns = [desc[0] for desc in res.description]
    return [dict(zip(columns, row)) for row in res.fetchall()]

def sanitize_input(val: str) -> str:
    return val.replace("'", "''").replace("\\", "")

def build_where_clause(company: str, ptype: str, pname: str, rawmtrl: str, dateFrom: str, dateTo: str):
    clauses = ["1=1"]
    if company and company != "ALL":
        keys = [sanitize_input(k.strip()) for k in company.split(",") if k.strip()]
        if keys: clauses.append(f"({' OR '.join([f"BSSH_NM = '{k}'" for k in keys])})")
    if ptype and ptype != "ALL":
        keys = [sanitize_input(k.strip()) for k in ptype.split(",") if k.strip()]
        if keys: clauses.append(f"({' OR '.join([f"PRDLST_DCNM = '{k}'" for k in keys])})")
    if pname:
        keys = [sanitize_input(k.strip()) for k in pname.split(",") if k.strip()]
        if keys: clauses.append(f"({' OR '.join([f"PRDLST_NM ILIKE '%{k}%'" for k in keys])})")
    if rawmtrl:
        keys = [sanitize_input(k.strip()) for k in rawmtrl.split(",") if k.strip()]
        if keys: clauses.append(f"({' OR '.join([f"RAWMTRL_NM ILIKE '%{k}%'" for k in keys])})")
    if dateFrom: clauses.append(f"PRMS_DT >= '{sanitize_input(dateFrom.replace("-", ""))}'")
    if dateTo: clauses.append(f"PRMS_DT <= '{sanitize_input(dateTo.replace("-", ""))}'")
    return " AND ".join(clauses)

@app.api_route("/", methods=["GET", "HEAD"], response_class=HTMLResponse)
def serve_dashboard():
    html_path = os.path.join(BASE_DIR, "index.html")
    with open(html_path, "r", encoding="utf-8") as f:
        return f.read()

@app.get("/api/meta")
def get_meta(dataType: str = "food"):
    file_path = get_file_path(dataType)
    if not os.path.exists(file_path): return {"min_date": "-", "max_date": "-", "companies": [], "ptypes": []}
    cursor = db.cursor()
    try:
        dt_query = f"SELECT MIN(PRMS_DT), MAX(PRMS_DT) FROM '{file_path}' WHERE LENGTH(PRMS_DT) = 8 AND PRMS_DT NOT LIKE '0000%'"
        row = cursor.execute(dt_query).fetchone()
        min_d = f"{row[0][:4]}-{row[0][4:6]}-{row[0][6:]}" if row and row[0] else "-"
        max_d = f"{row[1][:4]}-{row[1][4:6]}-{row[1][6:]}" if row and row[1] else "-"
        companies = [r[0] for r in cursor.execute(f"SELECT BSSH_NM, COUNT(*) as cnt FROM '{file_path}' WHERE BSSH_NM != '' GROUP BY BSSH_NM ORDER BY cnt DESC").fetchall()]
        ptypes = [r[0] for r in cursor.execute(f"SELECT PRDLST_DCNM, COUNT(*) as cnt FROM '{file_path}' WHERE PRDLST_DCNM != '' GROUP BY PRDLST_DCNM ORDER BY cnt DESC").fetchall()]
        return {"min_date": min_d, "max_date": max_d, "companies": companies, "ptypes": ptypes}
    finally:
        cursor.close()

@app.get("/api/dashboard")
def get_dashboard_data(
    dataType: str="food", company: str="", ptype: str="", pname: str="", rawmtrl: str="", 
    dateFrom: str="", dateTo: str="", targetStartDate: str="", targetEndDate: str=""
):
    file_path = get_file_path(dataType)
    if not os.path.exists(file_path): return JSONResponse(status_code=404, content={"error": "File not found"})

    where_sql = build_where_clause(company, ptype, pname, rawmtrl, dateFrom, dateTo)
    cursor = db.cursor()
    try:
        # 1. 메인 KPI
        kpi_result = query_to_dict(cursor, f"SELECT COUNT(*) as total_items, COUNT(DISTINCT BSSH_NM) as company_count, COUNT(DISTINCT PRDLST_DCNM) as ptype_count FROM '{file_path}' WHERE {where_sql}")
        kpi = kpi_result[0] if kpi_result else {"total_items": 0, "company_count": 0, "ptype_count": 0}

        # 2. 🚀 상단 요약 카드 집계 로직 (기간 지정 방식)
        target_stats = {"items": [], "total": 0}
        
        if targetStartDate and targetEndDate:
            str_start = sanitize_input(targetStartDate.replace("-", ""))
            str_end = sanitize_input(targetEndDate.replace("-", ""))
            
            target_set = TARGET_FOOD_PTYPES if dataType == "food" else TARGET_MEAT_PTYPES
            in_clause_items = ", ".join([f"'{p}'" for p in target_set])
            
            target_query = f"""
                SELECT PRDLST_DCNM as ptype, COUNT(*) as count, STRING_AGG(BSSH_NM, ', ') as mfr_list
                FROM '{file_path}'
                WHERE PRMS_DT >= '{str_start}' AND PRMS_DT <= '{str_end}' 
                  AND PRDLST_DCNM IN ({in_clause_items})
                GROUP BY PRDLST_DCNM
                ORDER BY count DESC
            """
            try:
                rows = query_to_dict(cursor, target_query)
                for r in rows:
                    if r['count'] > 0: 
                        target_stats["total"] += r['count']
                        mfr_set = list(set([m.strip() for m in r['mfr_list'].split(',') if m.strip()]))
                        r['top_mfrs'] = ", ".join(mfr_set[:3]) + (" 등" if len(mfr_set) > 3 else "")
                        target_stats["items"].append(r)
            except Exception as e:
                pass

        # 3. 제조사 순위 차트
        mfr_chart = query_to_dict(cursor, f"SELECT BSSH_NM as company, COUNT(*) as count FROM '{file_path}' WHERE {where_sql} AND BSSH_NM != '' GROUP BY BSSH_NM ORDER BY count DESC LIMIT 8")
        
        # 4. 🚀 신규: 품목명 핵심 키워드 파싱 로직 (띄어쓰기 기준)
        pname_rows = cursor.execute(f"SELECT PRDLST_NM FROM '{file_path}' WHERE {where_sql} AND PRDLST_NM != ''").fetchall()
        pname_words = []
        for row in pname_rows:
            if row[0]:
                words = str(row[0]).split()
                pname_words.extend([w.strip() for w in words if len(w.strip()) > 0])
        
        word_counter = Counter(pname_words)
        pname_chart = [{"keyword": k, "count": v} for k, v in word_counter.most_common(10)]

        # 5. 🚀 신규: 급부상 원재료 사용 빈도 파싱 로직 (쉼표 기준)
        raw_rows = cursor.execute(f"SELECT RAWMTRL_NM FROM '{file_path}' WHERE {where_sql} AND RAWMTRL_NM != ''").fetchall()
        raw_materials = []
        for row in raw_rows:
            if row[0]:
                materials = str(row[0]).split(',')
                raw_materials.extend([m.strip() for m in materials if len(m.strip()) > 0])
        
        raw_counter = Counter(raw_materials)
        raw_chart = [{"material": k, "count": v} for k, v in raw_counter.most_common(10)]

        return {
            "kpi": kpi, 
            "mfr_chart": mfr_chart, 
            "pname_chart": pname_chart,
            "raw_chart": raw_chart,
            "target_weekly_stats": target_stats
        }
    finally:
        cursor.close()

@app.get("/api/details")
def details(dataType: str="food", company: str="", ptype: str="", pname: str="", rawmtrl: str="", dateFrom: str="", dateTo: str="", page: int=1, sortCol: str="PRMS_DT", sortAsc: str="false"):
    file_path = get_file_path(dataType)
    if not os.path.exists(file_path): return JSONResponse(content={"TotalCount": 0, "Data": []})
    where_sql = build_where_clause(company, ptype, pname, rawmtrl, dateFrom, dateTo)
    cursor = db.cursor()
    try:
        total_count = cursor.execute(f"SELECT COUNT(*) FROM '{file_path}' WHERE {where_sql}").fetchone()[0]
        safe_sort_col = sortCol if sortCol in {"BSSH_NM", "PRDLST_REPORT_NO", "PRDLST_NM", "PRDLST_DCNM", "RAWMTRL_NM", "PRMS_DT", "CHNG_DT"} else "PRMS_DT"
        data = query_to_dict(cursor, f"SELECT COALESCE(BSSH_NM, '') as BSSH_NM, COALESCE(PRDLST_REPORT_NO, '') as PRDLST_REPORT_NO, COALESCE(PRDLST_NM, '') as PRDLST_NM, COALESCE(PRDLST_DCNM, '') as PRDLST_DCNM, COALESCE(RAWMTRL_NM, '') as RAWMTRL_NM, COALESCE(PRMS_DT, '') as PRMS_DT, COALESCE(CHNG_DT, '') as CHNG_DT FROM '{file_path}' WHERE {where_sql} ORDER BY {safe_sort_col} {'ASC' if sortAsc.lower() == 'true' else 'DESC'} LIMIT 20 OFFSET {(page - 1) * 20}")
        return JSONResponse(content={"TotalCount": total_count, "Data": data})
    finally:
        cursor.close()

@app.get("/api/download")
def download(dataType: str="food", company: str="", ptype: str="", pname: str="", rawmtrl: str="", dateFrom: str="", dateTo: str="", sortCol: str="PRMS_DT", sortAsc: str="false"):
    file_path = get_file_path(dataType)
    if not os.path.exists(file_path): return JSONResponse(status_code=404, content={"error": "File not found"})
    where_sql = build_where_clause(company, ptype, pname, rawmtrl, dateFrom, dateTo)
    safe_sort_col = sortCol if sortCol in {"BSSH_NM", "PRDLST_REPORT_NO", "PRDLST_NM", "PRDLST_DCNM", "RAWMTRL_NM", "PRMS_DT", "CHNG_DT"} else "PRMS_DT"
    export_query = f"SELECT BSSH_NM as '업소명', PRDLST_REPORT_NO as '품목제조번호', PRDLST_NM as '품목명', PRDLST_DCNM as '품목유형', RAWMTRL_NM as '원재료명', PRMS_DT as '등록일자', CHNG_DT as '변경일자' FROM '{file_path}' WHERE {where_sql} ORDER BY {safe_sort_col} {'ASC' if sortAsc.lower() == 'true' else 'DESC'}"
    def iter_csv():
        cursor = db.cursor()
        try:
            res = cursor.execute(export_query)
            buffer = io.StringIO()
            writer = csv.writer(buffer)
            buffer.write('\ufeff')
            writer.writerow([desc[0] for desc in res.description])
            yield buffer.getvalue()
            buffer.seek(0); buffer.truncate(0)
            while True:
                chunk = res.fetchmany(1000)
                if not chunk: break
                writer.writerows(chunk)
                yield buffer.getvalue()
                buffer.seek(0); buffer.truncate(0)
        finally:
            cursor.close()
    return StreamingResponse(iter_csv(), media_type="text/csv", headers={"Content-Disposition": 'attachment; filename="Report_Data_Export.csv"'})