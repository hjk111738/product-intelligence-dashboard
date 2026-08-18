from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, Response
import duckdb
import os
import urllib.parse
from typing import Optional

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def get_file_path(data_type: str):
    return os.path.join(BASE_DIR, f"data_{data_type}.parquet")

def query_to_dict(conn, query):
    res = conn.execute(query)
    columns = [desc[0] for desc in res.description]
    return [dict(zip(columns, row)) for row in res.fetchall()]

def build_where_clause(company: str, ptype: str, pname: str, dateFrom: str, dateTo: str):
    clauses = ["1=1"]
    
    if company and company != "ALL":
        keys = [k.strip() for k in company.split(",") if k.strip()]
        if keys:
            sub = " OR ".join([f"BSSH_NM = '{k}'" for k in keys])
            clauses.append(f"({sub})")
            
    if ptype and ptype != "ALL":
        keys = [k.strip() for k in ptype.split(",") if k.strip()]
        if keys:
            sub = " OR ".join([f"PRDLST_DCNM = '{k}'" for k in keys])
            clauses.append(f"({sub})")
            
    if pname:
        keys = [k.strip() for k in pname.split(",") if k.strip()]
        if keys:
            sub = " OR ".join([f"(PRDLST_NM ILIKE '%{k}%' OR RAWMTRL_NM ILIKE '%{k}%')" for k in keys])
            clauses.append(f"({sub})")
            
    if dateFrom:
        clean_df = dateFrom.replace("-", "")
        clauses.append(f"PRMS_DT >= '{clean_df}'")
    if dateTo:
        clean_dt = dateTo.replace("-", "")
        clauses.append(f"PRMS_DT <= '{clean_dt}'")
        
    return " AND ".join(clauses)

@app.api_route("/", methods=["GET", "HEAD"], response_class=HTMLResponse)
def serve_dashboard():
    html_path = os.path.join(BASE_DIR, "index.html")
    with open(html_path, "r", encoding="utf-8") as f:
        return f.read()

@app.get("/api/meta")
def get_meta(dataType: str = "food"):
    file_path = get_file_path(dataType)
    if not os.path.exists(file_path):
        return {"min_date": "-", "max_date": "-", "companies": [], "ptypes": []}
        
    conn = duckdb.connect()
    
    # 일자 범위
    dt_query = f"""
        SELECT MIN(PRMS_DT), MAX(PRMS_DT) 
        FROM '{file_path}' 
        WHERE LENGTH(PRMS_DT) = 8 AND PRMS_DT NOT LIKE '0000%'
    """
    row = conn.execute(dt_query).fetchone()
    min_d = f"{row[0][:4]}-{row[0][4:6]}-{row[0][6:]}" if row and row[0] else "-"
    max_d = f"{row[1][:4]}-{row[1][4:6]}-{row[1][6:]}" if row and row[1] else "-"

    # 전체 제조사 목록
    mfr_query = f"""
        SELECT BSSH_NM, COUNT(*) as cnt
        FROM '{file_path}'
        WHERE BSSH_NM IS NOT NULL AND BSSH_NM != ''
        GROUP BY BSSH_NM
        ORDER BY cnt DESC
    """
    companies = [r[0] for r in conn.execute(mfr_query).fetchall()]

    # 전체 품목유형 목록
    ptype_query = f"""
        SELECT PRDLST_DCNM, COUNT(*) as cnt
        FROM '{file_path}'
        WHERE PRDLST_DCNM IS NOT NULL AND PRDLST_DCNM != ''
        GROUP BY PRDLST_DCNM
        ORDER BY cnt DESC
    """
    ptypes = [r[0] for r in conn.execute(ptype_query).fetchall()]

    return {"min_date": min_d, "max_date": max_d, "companies": companies, "ptypes": ptypes}

@app.get("/api/dashboard")
def get_dashboard_data(
    dataType: str = "food", company: str = "", ptype: str = "", pname: str = "",
    dateFrom: str = "", dateTo: str = ""
):
    file_path = get_file_path(dataType)
    if not os.path.exists(file_path):
        return JSONResponse(status_code=404, content={"error": "File not found"})

    conn = duckdb.connect()
    where_sql = build_where_clause(company, ptype, pname, dateFrom, dateTo)

    # 1. KPI 지표
    kpi_query = f"""
        SELECT 
            COUNT(*) as total_items,
            COUNT(DISTINCT BSSH_NM) as company_count,
            COUNT(DISTINCT PRDLST_DCNM) as ptype_count
        FROM '{file_path}' 
        WHERE {where_sql}
    """
    kpi = query_to_dict(conn, kpi_query)[0]

    # 2. 제조사별 신제품 런칭 순위 (Top 8)
    mfr_query = f"""
        SELECT BSSH_NM as company, COUNT(*) as count
        FROM '{file_path}' 
        WHERE {where_sql} AND BSSH_NM IS NOT NULL AND BSSH_NM != ''
        GROUP BY BSSH_NM 
        ORDER BY count DESC 
        LIMIT 8
    """
    mfr_chart = query_to_dict(conn, mfr_query)

    # 3. 카테고리(품목유형)별 분포 (Top 6)
    ptype_chart_query = f"""
        SELECT PRDLST_DCNM as ptype, COUNT(*) as count
        FROM '{file_path}' 
        WHERE {where_sql} AND PRDLST_DCNM IS NOT NULL AND PRDLST_DCNM != ''
        GROUP BY PRDLST_DCNM 
        ORDER BY count DESC 
        LIMIT 6
    """
    ptype_chart = query_to_dict(conn, ptype_chart_query)

    # 4. 품목유형별 월별 시계열 추이 (Top 5 품목유형 대상 다중 꺾은선)
    top_ptypes = [p["ptype"] for p in ptype_chart[:5]]
    
    # 전체 기간 리스트 (X축)
    period_query = f"""
        SELECT DISTINCT SUBSTRING(PRMS_DT, 1, 4) || '-' || SUBSTRING(PRMS_DT, 5, 2) as ym
        FROM '{file_path}' 
        WHERE {where_sql} AND LENGTH(PRMS_DT) >= 6
        ORDER BY ym ASC
    """
    periods = [r[0] for r in conn.execute(period_query).fetchall()]

    trend_matrix = {}
    if top_ptypes and periods:
        ptype_in = ", ".join([f"'{p}'" for p in top_ptypes])
        trend_query = f"""
            SELECT 
                PRDLST_DCNM as ptype,
                SUBSTRING(PRMS_DT, 1, 4) || '-' || SUBSTRING(PRMS_DT, 5, 2) as ym,
                COUNT(*) as count
            FROM '{file_path}' 
            WHERE {where_sql} AND PRDLST_DCNM IN ({ptype_in}) AND LENGTH(PRMS_DT) >= 6
            GROUP BY PRDLST_DCNM, ym
            ORDER BY ym ASC
        """
        rows = conn.execute(trend_query).fetchall()
        for pt, ym, cnt in rows:
            if pt not in trend_matrix:
                trend_matrix[pt] = {}
            trend_matrix[pt][ym] = cnt

    return {
        "kpi": kpi,
        "mfr_chart": mfr_chart,
        "ptype_chart": ptype_chart,
        "trend_periods": periods,
        "trend_matrix": trend_matrix,
        "top_ptypes": top_ptypes
    }

@app.get("/api/details")
def details(
    dataType: str = "food", company: str = "", ptype: str = "", pname: str = "",
    dateFrom: str = "", dateTo: str = "", page: int = 1, sortCol: str = "PRMS_DT", sortAsc: str = "false"
):
    file_path = get_file_path(dataType)
    if not os.path.exists(file_path):
        return JSONResponse(content={"TotalCount": 0, "Data": []})
        
    conn = duckdb.connect()
    where_sql = build_where_clause(company, ptype, pname, dateFrom, dateTo)
    
    count_query = f"SELECT COUNT(*) FROM '{file_path}' WHERE {where_sql}"
    total_count = conn.execute(count_query).fetchone()[0]
    
    page_size = 20
    offset = (page - 1) * page_size
    
    order_sql = ""
    if sortCol:
        direction = "ASC" if sortAsc.lower() == "true" else "DESC"
        order_sql = f"ORDER BY {sortCol} {direction}"
        
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
    data = query_to_dict(conn, data_query)
    
    return JSONResponse(content={"TotalCount": total_count, "Data": data})

@app.get("/api/download")
def download(
    dataType: str = "food", company: str = "", ptype: str = "", pname: str = "",
    dateFrom: str = "", dateTo: str = "", sortCol: str = "PRMS_DT", sortAsc: str = "false"
):
    file_path = get_file_path(dataType)
    if not os.path.exists(file_path):
        return Response(content="", media_type="text/csv")
        
    conn = duckdb.connect()
    where_sql = build_where_clause(company, ptype, pname, dateFrom, dateTo)
    
    direction = "ASC" if sortAsc.lower() == "true" else "DESC"
    order_sql = f"ORDER BY {sortCol} {direction}" if sortCol else ""
        
    export_query = f"""
        SELECT 
            BSSH_NM as "업소명",
            PRDLST_REPORT_NO as "품목제조번호",
            PRDLST_NM as "품목명",
            PRDLST_DCNM as "품목유형",
            RAWMTRL_NM as "원재료명",
            PRMS_DT as "등록일자",
            CHNG_DT as "변경일자"
        FROM '{file_path}'
        WHERE {where_sql}
        {order_sql}
    """
    
    csv_temp_path = os.path.join(BASE_DIR, "temp_export.csv")
    conn.execute(f"COPY ({export_query}) TO '{csv_temp_path}' (HEADER, DELIMITER ',')")
    
    with open(csv_temp_path, "r", encoding="utf-8") as f:
        csv_data = "\ufeff" + f.read()
        
    if os.path.exists(csv_temp_path):
        os.remove(csv_temp_path)
        
    return Response(
        content=csv_data,
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="Report_Data_Export.csv"'}
    )