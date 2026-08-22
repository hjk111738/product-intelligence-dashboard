from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
import duckdb
import os
import io
import csv

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 1. 글로벌 DB 설정: 메모리 상한선 적용
db = duckdb.connect()
db.execute("SET memory_limit = '256MB';")
db.execute("SET threads = 1;")

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
        if keys:
            sub = " OR ".join([f"BSSH_NM = '{k}'" for k in keys])
            clauses.append(f"({sub})")
            
    if ptype and ptype != "ALL":
        keys = [sanitize_input(k.strip()) for k in ptype.split(",") if k.strip()]
        if keys:
            sub = " OR ".join([f"PRDLST_DCNM = '{k}'" for k in keys])
            clauses.append(f"({sub})")
            
    if pname:
        keys = [sanitize_input(k.strip()) for k in pname.split(",") if k.strip()]
        if keys:
            sub = " OR ".join([f"PRDLST_NM ILIKE '%{k}%'" for k in keys])
            clauses.append(f"({sub})")
            
    if rawmtrl:
        keys = [sanitize_input(k.strip()) for k in rawmtrl.split(",") if k.strip()]
        if keys:
            sub = " OR ".join([f"RAWMTRL_NM ILIKE '%{k}%'" for k in keys])
            clauses.append(f"({sub})")
            
    if dateFrom:
        clean_df = sanitize_input(dateFrom.replace("-", ""))
        clauses.append(f"PRMS_DT >= '{clean_df}'")
    if dateTo:
        clean_dt = sanitize_input(dateTo.replace("-", ""))
        clauses.append(f"PRMS_DT <= '{clean_dt}'")
        
    return " AND ".join(clauses)

@app.api_route("/", methods=["GET", "HEAD"], response_class=HTMLResponse)
def serve_dashboard():
    html_path = os.path.join(BASE_DIR, "index.html")
    if not os.path.exists(html_path):
        return HTMLResponse("<h1>index.html not found</h1>", status_code=404)
    with open(html_path, "r", encoding="utf-8") as f:
        return f.read()

@app.get("/api/meta")
def get_meta(dataType: str = "food"):
    file_path = get_file_path(dataType)
    if not os.path.exists(file_path):
        return {"min_date": "-", "max_date": "-", "companies": [], "ptypes": []}
        
    cursor = db.cursor() # 안전한 동시 처리를 위한 커서 생성
    try:
        dt_query = f"""
            SELECT MIN(PRMS_DT), MAX(PRMS_DT) 
            FROM '{file_path}' 
            WHERE LENGTH(PRMS_DT) = 8 AND PRMS_DT NOT LIKE '0000%'
        """
        row = cursor.execute(dt_query).fetchone()
        min_d = f"{row[0][:4]}-{row[0][4:6]}-{row[0][6:]}" if row and row[0] else "-"
        max_d = f"{row[1][:4]}-{row[1][4:6]}-{row[1][6:]}" if row and row[1] else "-"

        mfr_query = f"SELECT BSSH_NM, COUNT(*) as cnt FROM '{file_path}' WHERE BSSH_NM IS NOT NULL AND BSSH_NM != '' GROUP BY BSSH_NM ORDER BY cnt DESC"
        companies = [r[0] for r in cursor.execute(mfr_query).fetchall()]

        ptype_query = f"SELECT PRDLST_DCNM, COUNT(*) as cnt FROM '{file_path}' WHERE PRDLST_DCNM IS NOT NULL AND PRDLST_DCNM != '' GROUP BY PRDLST_DCNM ORDER BY cnt DESC"
        ptypes = [r[0] for r in cursor.execute(ptype_query).fetchall()]

        return {"min_date": min_d, "max_date": max_d, "companies": companies, "ptypes": ptypes}
    finally:
        cursor.close() # 쿼리 완료 후 메모리 반환

@app.get("/api/dashboard")
def get_dashboard_data(
    dataType: str = "food", company: str = "", ptype: str = "", 
    pname: str = "", rawmtrl: str = "", dateFrom: str = "", dateTo: str = ""
):
    file_path = get_file_path(dataType)
    if not os.path.exists(file_path):
        return JSONResponse(status_code=404, content={"error": "File not found"})

    where_sql = build_where_clause(company, ptype, pname, rawmtrl, dateFrom, dateTo)
    cursor = db.cursor()
    
    try:
        # 1. KPI 지표 (빈 결과값 방어 로직 추가)
        kpi_query = f"SELECT COUNT(*) as total_items, COUNT(DISTINCT BSSH_NM) as company_count, COUNT(DISTINCT PRDLST_DCNM) as ptype_count FROM '{file_path}' WHERE {where_sql}"
        kpi_result = query_to_dict(cursor, kpi_query)
        kpi = kpi_result[0] if kpi_result else {"total_items": 0, "company_count": 0, "ptype_count": 0}

        # 2. 제조사별 순위 (Top 8)
        mfr_query = f"SELECT BSSH_NM as company, COUNT(*) as count FROM '{file_path}' WHERE {where_sql} AND BSSH_NM IS NOT NULL AND BSSH_NM != '' GROUP BY BSSH_NM ORDER BY count DESC LIMIT 8"
        mfr_chart = query_to_dict(cursor, mfr_query)

        # 3. 카테고리별 분포 (Top 6)
        ptype_chart_query = f"SELECT PRDLST_DCNM as ptype, COUNT(*) as count FROM '{file_path}' WHERE {where_sql} AND PRDLST_DCNM IS NOT NULL AND PRDLST_DCNM != '' GROUP BY PRDLST_DCNM ORDER BY count DESC LIMIT 6"
        ptype_chart = query_to_dict(cursor, ptype_chart_query)

        # 4. 시계열 추이
        top_ptypes = [p["ptype"] for p in ptype_chart[:5] if p.get("ptype")]
        period_query = f"SELECT DISTINCT SUBSTRING(PRMS_DT, 1, 4) || '-' || SUBSTRING(PRMS_DT, 5, 2) as ym FROM '{file_path}' WHERE {where_sql} AND PRMS_DT IS NOT NULL AND LENGTH(PRMS_DT) = 8 AND PRMS_DT NOT LIKE '0000%' ORDER BY ym ASC"
        periods = [r[0] for r in cursor.execute(period_query).fetchall() if r[0]]

        trend_matrix = {}
        if top_ptypes and periods:
            ptype_in = ", ".join([f"'{sanitize_input(p)}'" for p in top_ptypes])
            trend_query = f"SELECT PRDLST_DCNM as ptype, SUBSTRING(PRMS_DT, 1, 4) || '-' || SUBSTRING(PRMS_DT, 5, 2) as ym, COUNT(*) as count FROM '{file_path}' WHERE {where_sql} AND PRDLST_DCNM IN ({ptype_in}) AND PRMS_DT IS NOT NULL AND LENGTH(PRMS_DT) = 8 GROUP BY PRDLST_DCNM, ym ORDER BY ym ASC"
            rows = cursor.execute(trend_query).fetchall()
            for pt, ym, cnt in rows:
                if pt not in trend_matrix:
                    trend_matrix[pt] = {}
                trend_matrix[pt][ym] = cnt

        return {
            "kpi": kpi, "mfr_chart": mfr_chart, "ptype_chart": ptype_chart,
            "trend_periods": periods, "trend_matrix": trend_matrix, "top_ptypes": top_ptypes
        }
    finally:
        cursor.close()

@app.get("/api/details")
def details(
    dataType: str = "food", company: str = "", ptype: str = "", 
    pname: str = "", rawmtrl: str = "", dateFrom: str = "", dateTo: str = "", 
    page: int = 1, sortCol: str = "PRMS_DT", sortAsc: str = "false"
):
    file_path = get_file_path(dataType)
    if not os.path.exists(file_path):
        return JSONResponse(content={"TotalCount": 0, "Data": []})
        
    where_sql = build_where_clause(company, ptype, pname, rawmtrl, dateFrom, dateTo)
    cursor = db.cursor()
    
    try:
        count_query = f"SELECT COUNT(*) FROM '{file_path}' WHERE {where_sql}"
        total_count = cursor.execute(count_query).fetchone()[0]
        
        page_size = 20
        offset = (page - 1) * page_size
        
        allowed_sort_cols = {"BSSH_NM", "PRDLST_REPORT_NO", "PRDLST_NM", "PRDLST_DCNM", "RAWMTRL_NM", "PRMS_DT", "CHNG_DT"}
        safe_sort_col = sortCol if sortCol in allowed_sort_cols else "PRMS_DT"
        direction = "ASC" if sortAsc.lower() == "true" else "DESC"
        order_sql = f"ORDER BY {safe_sort_col} {direction}"
            
        data_query = f"""
            SELECT 
                COALESCE(BSSH_NM, '') as BSSH_NM,
                COALESCE(PRDLST_REPORT_NO, '') as PRDLST_REPORT_NO,
                COALESCE(PRDLST_NM, '') as PRDLST_NM,
                COALESCE(PRDLST_DCNM, '') as PRDLST_DCNM,
                COALESCE(RAWMTRL_NM, '') as RAWMTRL_NM,
                COALESCE(PRMS_DT, '') as PRMS_DT,
                COALESCE(CHNG_DT, '') as CHNG_DT
            FROM '{file_path}' 
            WHERE {where_sql}
            {order_sql}
            LIMIT {page_size} OFFSET {offset}
        """
        data = query_to_dict(cursor, data_query)
        return JSONResponse(content={"TotalCount": total_count, "Data": data})
    finally:
        cursor.close()

@app.get("/api/download")
def download(
    dataType: str = "food", company: str = "", ptype: str = "", 
    pname: str = "", rawmtrl: str = "", dateFrom: str = "", dateTo: str = "", 
    sortCol: str = "PRMS_DT", sortAsc: str = "false"
):
    file_path = get_file_path(dataType)
    if not os.path.exists(file_path):
        return JSONResponse(status_code=404, content={"error": "File not found"})
        
    where_sql = build_where_clause(company, ptype, pname, rawmtrl, dateFrom, dateTo)
    allowed_sort_cols = {"BSSH_NM", "PRDLST_REPORT_NO", "PRDLST_NM", "PRDLST_DCNM", "RAWMTRL_NM", "PRMS_DT", "CHNG_DT"}
    safe_sort_col = sortCol if sortCol in allowed_sort_cols else "PRMS_DT"
    direction = "ASC" if sortAsc.lower() == "true" else "DESC"
    
    export_query = f"""
        SELECT 
            BSSH_NM as "업소명", PRDLST_REPORT_NO as "품목제조번호",
            PRDLST_NM as "품목명", PRDLST_DCNM as "품목유형",
            RAWMTRL_NM as "원재료명", PRMS_DT as "등록일자", CHNG_DT as "변경일자"
        FROM '{file_path}'
        WHERE {where_sql}
        ORDER BY {safe_sort_col} {direction}
    """
    
    # 💥 Pro 핵심 변경점: 데이터를 RAM에 다 올리지 않고 1000줄씩 스트리밍 처리 (메모리 사용량 O(1))
    def iter_csv():
        cursor = db.cursor()
        try:
            res = cursor.execute(export_query)
            columns = [desc[0] for desc in res.description]
            
            buffer = io.StringIO()
            writer = csv.writer(buffer)
            
            # 1. 엑셀 한글 깨짐 방지 BOM 및 헤더 먼저 전송
            buffer.write('\ufeff')
            writer.writerow(columns)
            yield buffer.getvalue()
            buffer.seek(0)
            buffer.truncate(0)
            
            # 2. 1000줄씩 청크 단위로 읽어서 내보냄 (RAM 초과 절대 불가)
            while True:
                chunk = res.fetchmany(1000)
                if not chunk:
                    break
                writer.writerows(chunk)
                yield buffer.getvalue()
                buffer.seek(0)
                buffer.truncate(0)
        finally:
            cursor.close()
            
    return StreamingResponse(
        iter_csv(),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="Report_Data_Export.csv"'}
    )