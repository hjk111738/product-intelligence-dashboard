from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
import duckdb
import os
import io
import csv
import re
from datetime import datetime

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

def get_file_path(data_type: str):
    return os.path.join(BASE_DIR, f"data_{data_type}.parquet")

def get_file_mtime(file_path: str) -> str:
    if os.path.exists(file_path):
        try:
            mtime_timestamp = os.path.getmtime(file_path)
            return datetime.fromtimestamp(mtime_timestamp).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
    return "-"

def query_to_dict(cursor, query: str):
    res = cursor.execute(query)
    columns = [desc[0] for desc in res.description]
    return [dict(zip(columns, row)) for row in res.fetchall()]

def sanitize_input(val: str) -> str:
    return val.replace("'", "''").replace("\\", "")

def parse_smart_keyword_clause(col_name: str, raw_input: str) -> str:
    """
    콤마(,) -> OR 조건
    공백( ) 또는 플러스(+) -> AND 조건
    마이너스(-키워드) -> 제외(NOT ILIKE) 조건
    """
    if not raw_input or not raw_input.strip():
        return ""
    
    or_groups = []
    exclude_terms = []
    
    for chunk in raw_input.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        
        tokens = [t.strip() for t in re.split(r'[\s+]+', chunk) if t.strip()]
        and_terms = []
        
        for token in tokens:
            if token.startswith("-") and len(token) > 1:
                clean_ex = sanitize_input(token[1:].strip())
                if clean_ex:
                    exclude_terms.append(f"COALESCE({col_name}, '') NOT ILIKE '%{clean_ex}%'")
            else:
                clean_in = sanitize_input(token.lstrip("+").strip())
                if clean_in:
                    and_terms.append(f"{col_name} ILIKE '%{clean_in}%'")
        
        if and_terms:
            if len(and_terms) == 1:
                or_groups.append(and_terms[0])
            else:
                or_groups.append(f"({' AND '.join(and_terms)})")
                
    clauses = []
    if or_groups:
        clauses.append(f"({' OR '.join(or_groups)})")
    if exclude_terms:
        clauses.append(" AND ".join(exclude_terms))
        
    return " AND ".join(clauses)

def build_where_clause(
    company: str, ptype: str, pname: str, rawmtrl: str, 
    dateFrom: str = "", dateTo: str = "", include_ptype: bool = True
):
    clauses = ["1=1"]
    if company and company != "ALL":
        keys = [sanitize_input(k.strip()) for k in company.split(",") if k.strip()]
        if keys:
            or_sql = " OR ".join([f"BSSH_NM = '{k}'" for k in keys])
            clauses.append(f"({or_sql})")
            
    if include_ptype and ptype and ptype != "ALL":
        keys = [sanitize_input(k.strip()) for k in ptype.split(",") if k.strip()]
        if keys:
            or_sql = " OR ".join([f"PRDLST_DCNM = '{k}'" for k in keys])
            clauses.append(f"({or_sql})")
            
    if pname:
        pname_sql = parse_smart_keyword_clause("PRDLST_NM", pname)
        if pname_sql:
            clauses.append(pname_sql)
            
    if rawmtrl:
        raw_sql = parse_smart_keyword_clause("RAWMTRL_NM", rawmtrl)
        if raw_sql:
            clauses.append(raw_sql)
            
    if dateFrom:
        clean_from = sanitize_input(re.sub(r'[^0-9]', '', dateFrom))
        if clean_from:
            clauses.append(f"PRMS_DT >= '{clean_from}'")
            
    if dateTo:
        clean_to = sanitize_input(re.sub(r'[^0-9]', '', dateTo))
        if clean_to:
            clauses.append(f"PRMS_DT <= '{clean_to}'")
            
    return " AND ".join(clauses)

def get_dataset_date_range(cursor, data_type: str):
    file_path = get_file_path(data_type)
    if not os.path.exists(file_path):
        return {"min_slash": "-", "max_slash": "-", "min_dash": "-", "max_dash": "-", "label": "데이터 없음"}
    try:
        dt_query = f"SELECT MIN(PRMS_DT), MAX(PRMS_DT) FROM '{file_path}' WHERE LENGTH(PRMS_DT) = 8 AND PRMS_DT NOT LIKE '0000%'"
        row = cursor.execute(dt_query).fetchone()
        if row and row[0] and row[1]:
            min_s = f"{row[0][:4]}/{row[0][4:6]}/{row[0][6:]}"
            max_s = f"{row[1][:4]}/{row[1][4:6]}/{row[1][6:]}"
            min_d = f"{row[0][:4]}-{row[0][4:6]}-{row[0][6:]}"
            max_d = f"{row[1][:4]}-{row[1][4:6]}-{row[1][6:]}"
            return {
                "min_slash": min_s,
                "max_slash": max_s,
                "min_dash": min_d,
                "max_dash": max_d,
                "label": f"{min_s}~{max_s}"
            }
    except Exception:
        pass
    return {"min_slash": "-", "max_slash": "-", "min_dash": "-", "max_dash": "-", "label": "-"}

@app.api_route("/", methods=["GET", "HEAD"], response_class=HTMLResponse)
def serve_dashboard():
    html_path = os.path.join(BASE_DIR, "index.html")
    with open(html_path, "r", encoding="utf-8") as f:
        return f.read()

@app.get("/api/meta")
def get_meta(dataType: str = "food"):
    cursor = db.cursor()
    try:
        food_range = get_dataset_date_range(cursor, "food")
        meat_range = get_dataset_date_range(cursor, "meat")
        current_range = food_range if dataType == "food" else meat_range

        food_path = get_file_path("food")
        meat_path = get_file_path("meat")

        food_updated_at = get_file_mtime(food_path)
        meat_updated_at = get_file_mtime(meat_path)
        current_updated_at = food_updated_at if dataType == "food" else meat_updated_at

        file_path = get_file_path(dataType)
        if not os.path.exists(file_path):
            return {
                "min_date": "-", "max_date": "-", 
                "food_period": food_range["label"],
                "meat_period": meat_range["label"],
                "food_updated_at": food_updated_at,
                "meat_updated_at": meat_updated_at,
                "last_updated": current_updated_at,
                "companies": [], "ptypes": []
            }

        companies = [r[0] for r in cursor.execute(f"SELECT BSSH_NM, COUNT(*) as cnt FROM '{file_path}' WHERE BSSH_NM != '' GROUP BY BSSH_NM ORDER BY cnt DESC").fetchall()]
        ptypes = [r[0] for r in cursor.execute(f"SELECT PRDLST_DCNM, COUNT(*) as cnt FROM '{file_path}' WHERE PRDLST_DCNM != '' GROUP BY PRDLST_DCNM ORDER BY cnt DESC").fetchall()]
        
        return {
            "min_date": current_range["min_dash"],
            "max_date": current_range["max_dash"],
            "food_period": food_range["label"],
            "meat_period": meat_range["label"],
            "food_updated_at": food_updated_at,
            "meat_updated_at": meat_updated_at,
            "last_updated": current_updated_at,
            "companies": companies,
            "ptypes": ptypes
        }
    finally:
        cursor.close()

@app.get("/api/dashboard")
def get_dashboard_data(
    dataType: str = "food", company: str = "", ptype: str = "", pname: str = "", rawmtrl: str = "", 
    dateFrom: str = "", dateTo: str = "", targetStartDate: str = "", targetEndDate: str = ""
):
    file_path = get_file_path(dataType)
    if not os.path.exists(file_path):
        return JSONResponse(status_code=404, content={"error": "File not found"})

    where_sql = build_where_clause(company, ptype, pname, rawmtrl, dateFrom, dateTo, include_ptype=True)
    cursor = db.cursor()
    try:
        kpi_result = query_to_dict(
            cursor, 
            f"SELECT COUNT(*) as total_items, COUNT(DISTINCT BSSH_NM) as company_count, COUNT(DISTINCT PRDLST_DCNM) as ptype_count FROM '{file_path}' WHERE {where_sql}"
        )
        kpi = kpi_result[0] if kpi_result else {"total_items": 0, "company_count": 0, "ptype_count": 0}

        target_stats = {"items": [], "by_company": [], "total": 0}
        
        if targetStartDate and targetEndDate:
            target_where = build_where_clause(
                company, ptype, pname, rawmtrl, 
                dateFrom=targetStartDate, dateTo=targetEndDate, include_ptype=True
            )
            
            target_query = f"""
                SELECT PRDLST_DCNM as ptype, BSSH_NM as company, COUNT(*) as count
                FROM '{file_path}'
                WHERE {target_where}
                GROUP BY PRDLST_DCNM, BSSH_NM
                ORDER BY count DESC
            """
            try:
                rows = query_to_dict(cursor, target_query)
                ptype_map = {}
                
                for r in rows:
                    pt = r["ptype"] or "미분류"
                    comp = r["company"] or "미상"
                    cnt = int(r["count"] or 0)
                    if cnt <= 0:
                        continue
                        
                    target_stats["total"] += cnt
                    target_stats["by_company"].append({"ptype": pt, "company": comp, "count": cnt})
                    
                    if pt not in ptype_map:
                        ptype_map[pt] = {"ptype": pt, "count": 0, "top_companies": []}
                    ptype_map[pt]["count"] += cnt
                    if len(ptype_map[pt]["top_companies"]) < 3:
                        ptype_map[pt]["top_companies"].append({"name": comp, "count": cnt})
                
                target_stats["items"] = sorted(ptype_map.values(), key=lambda x: x["count"], reverse=True)
            except Exception:
                pass

        return {
            "kpi": kpi, 
            "target_weekly_stats": target_stats
        }
    finally:
        cursor.close()

@app.get("/api/details")
def details(
    dataType: str = "food", company: str = "", ptype: str = "", pname: str = "", rawmtrl: str = "", 
    dateFrom: str = "", dateTo: str = "", page: int = 1, sortCol: str = "PRMS_DT", sortAsc: str = "false"
):
    file_path = get_file_path(dataType)
    if not os.path.exists(file_path):
        return JSONResponse(content={"TotalCount": 0, "Data": []})
    where_sql = build_where_clause(company, ptype, pname, rawmtrl, dateFrom, dateTo, include_ptype=True)
    cursor = db.cursor()
    try:
        total_count = cursor.execute(f"SELECT COUNT(*) FROM '{file_path}' WHERE {where_sql}").fetchone()[0]
        safe_sort_col = sortCol if sortCol in {"BSSH_NM", "PRDLST_REPORT_NO", "PRDLST_NM", "PRDLST_DCNM", "RAWMTRL_NM", "PRMS_DT", "CHNG_DT"} else "PRMS_DT"
        order_dir = "ASC" if sortAsc.lower() == "true" else "DESC"
        offset = max(0, (page - 1) * 20)
        
        data = query_to_dict(
            cursor, 
            f"""
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
            ORDER BY {safe_sort_col} {order_dir} 
            LIMIT 20 OFFSET {offset}
            """
        )
        return JSONResponse(content={"TotalCount": total_count, "Data": data})
    finally:
        cursor.close()

@app.get("/api/download")
def download(
    dataType: str = "food", company: str = "", ptype: str = "", pname: str = "", rawmtrl: str = "", 
    dateFrom: str = "", dateTo: str = "", sortCol: str = "PRMS_DT", sortAsc: str = "false"
):
    file_path = get_file_path(dataType)
    if not os.path.exists(file_path):
        return JSONResponse(status_code=404, content={"error": "File not found"})
    where_sql = build_where_clause(company, ptype, pname, rawmtrl, dateFrom, dateTo, include_ptype=True)
    safe_sort_col = sortCol if sortCol in {"BSSH_NM", "PRDLST_REPORT_NO", "PRDLST_NM", "PRDLST_DCNM", "RAWMTRL_NM", "PRMS_DT", "CHNG_DT"} else "PRMS_DT"
    order_dir = "ASC" if sortAsc.lower() == "true" else "DESC"
    
    export_query = f"""
        SELECT 
            BSSH_NM as '업소명', 
            PRDLST_REPORT_NO as '품목제조번호', 
            PRDLST_NM as '품목명', 
            PRDLST_DCNM as '품목유형', 
            RAWMTRL_NM as '원재료명', 
            PRMS_DT as '등록일자', 
            CHNG_DT as '변경일자' 
        FROM '{file_path}' 
        WHERE {where_sql} 
        ORDER BY {safe_sort_col} {order_dir}
    """
    def iter_csv():
        cursor = db.cursor()
        try:
            res = cursor.execute(export_query)
            buffer = io.StringIO()
            writer = csv.writer(buffer)
            buffer.write('\ufeff')
            writer.writerow([desc[0] for desc in res.description])
            yield buffer.getvalue()
            buffer.seek(0)
            buffer.truncate(0)
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
