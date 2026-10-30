import os
import time
import urllib.parse
import requests
from datetime import datetime, timedelta
import polars as pl

API_KEY = os.environ.get("FOOD_API_KEY", "b7fa39c1e193461cbd94")

# 🟢 식품 핵심 타겟 품목 (33종)
TARGET_FOOD_PTYPES = [
    "과자", "캔디류", "초콜릿가공품", "추잉껌", "빵류", "초콜릿", "밀크초콜릿", "준초콜릿", 
    "빙과", "당류가공품", "땅콩 또는 견과류가공품", 
    "만두", "어육소시지", "식육함유가공품", "즉석조리식품", "신선편의식품", "소스", 
    "마요네즈", "토마토케첩", "복합조미식품", 
    "고형차", "커피", "과.채주스", "혼합음료", "유산균음료", "액상차", "과.채음료", 
    "기타 영.유아식", "영아용 조제식", "성장기용 조제식", "임산.수유부용 식품", 
    "기타가공품", "두류가공품"
]

# 🔴 축산물 핵심 타겟 품목 (15종)
TARGET_MEAT_PTYPES = [
    "비유지방아이스크림", "샤베트", "아이스밀크", "아이스크림", "가공유", "농후발효유", 
    "발효유", "영아용 조제유", "우유", "유당분해우유", "유산균첨가우유", 
    "베이컨류", "분쇄가공육제품", "소시지", "프레스햄"
]

def sort_materials(struct_val):
    ord_val = struct_val.get("RAWMTRL_ORDNO")
    mat_val = struct_val.get("RAWMTRL_NM")
    if not ord_val or not mat_val:
        return mat_val
    try:
        ord_list = [x.strip() for x in str(ord_val).split(',') if x.strip()]
        mat_list = [x.strip() for x in str(mat_val).split(',') if x.strip()]
        if len(ord_list) != len(mat_list) or len(ord_list) == 0:
            return mat_val
        paired = []
        for o, m in zip(ord_list, mat_list):
            try:
                paired.append((int(o), m))
            except ValueError:
                paired.append((9999, m))
        paired.sort(key=lambda x: x[0])
        return ", ".join([p[1] for p in paired])
    except Exception:
        return mat_val

def get_latest_change_date(parquet_file: str) -> str:
    """
    식품안전나라 API 제한(최대 7일 이내 증분만 호출 가능)을 준수하면서
    기존 파일의 최신 날짜 또는 기본 6일 전 날짜를 계산
    """
    today = datetime.now()
    max_history_limit = today - timedelta(days=6) # API 최대 제한(7일) 내 안전 마진

    if os.path.exists(parquet_file):
        try:
            df = pl.read_parquet(parquet_file)
            if "CHNG_DT" in df.columns:
                valid_dates = df.filter(pl.col("CHNG_DT").str.len_chars() == 8)["CHNG_DT"]
                if valid_dates.len() > 0:
                    max_dt_str = valid_dates.max()
                    file_dt = datetime.strptime(max_dt_str, "%Y%m%d")
                    
                    # 파일 날짜가 너무 오래되었더라도 API 제한(최대 6일 전)으로 클램핑
                    target_dt = max(file_dt, max_history_limit)
                    start_str = target_dt.strftime("%Y%m%d")
                    print(f"📌 [{parquet_file}] 기존 최신 변경일자: {max_dt_str} -> API 제한 반영 수집 시작일: {start_str}")
                    return start_str
        except Exception as e:
            print(f"⚠️ 기존 파일 날짜 확인 중 오류 ({e}), 기본값 사용")

    fallback_str = max_history_limit.strftime("%Y%m%d")
    print(f"📌 [{parquet_file}] 기본 수집 시작일 적용: {fallback_str}")
    return fallback_str

def fetch_api_all_changes(service_id: str, start_dt: str):
    """
    축산물(C006) 등 품목 필터 미지원 서비스용 일괄 페이징 수집
    """
    collected = []
    start_idx = 1
    page_size = 1000

    print(f"📡 [{service_id}] API 일괄 수집 시작 (CHNG_DT >= {start_dt})")

    while True:
        end_idx = start_idx + page_size - 1
        url = f"http://openapi.foodsafetykorea.go.kr/api/{API_KEY}/{service_id}/json/{start_idx}/{end_idx}/CHNG_DT={start_dt}"
        
        try:
            resp = requests.get(url, timeout=45)
            res = resp.json()

            if "RESULT" in res and res["RESULT"].get("CODE") != "INFO-000":
                print(f"   ⚠️ API 응답 메시지: {res['RESULT'].get('MSG')}")
                break

            data_block = res.get(service_id, {})
            result = data_block.get("RESULT", {})

            if result.get("CODE") != "INFO-000":
                break

            chunk = data_block.get("row", [])
            if not chunk:
                break

            collected.extend(chunk)
            total_count = int(data_block.get("total_count", 0))

            if len(collected) >= total_count:
                break

            start_idx += page_size
            time.sleep(0.2)
        except Exception as e:
            print(f"   ❌ API 호출 오류: {e}")
            break

    print(f"✅ [{service_id}] 일괄 수집 완료: 총 {len(collected):,}건")
    return collected

def fetch_api_by_ptype_and_date(service_id: str, ptype: str, start_dt: str):
    """
    일반식품(C002)용 품목유형 + 날짜 핀포인트 수집
    """
    collected = []
    start_idx = 1
    page_size = 1000
    encoded_ptype = urllib.parse.quote(ptype)

    while True:
        end_idx = start_idx + page_size - 1
        url = (
            f"http://openapi.foodsafetykorea.go.kr/api/{API_KEY}/{service_id}/json/{start_idx}/{end_idx}"
            f"/CHNG_DT={start_dt}&PRDLST_DCNM={encoded_ptype}"
        )
        
        try:
            resp = requests.get(url, timeout=30)
            res = resp.json()

            if "RESULT" in res and res["RESULT"].get("CODE") != "INFO-000":
                break

            data_block = res.get(service_id, {})
            result = data_block.get("RESULT", {})

            if result.get("CODE") != "INFO-000":
                break

            chunk = data_block.get("row", [])
            if not chunk:
                break

            collected.extend(chunk)
            total_count = int(data_block.get("total_count", 0))

            if len(collected) >= total_count:
                break

            start_idx += page_size
            time.sleep(0.15)
        except Exception as e:
            break

    return collected

def sync_dataset(service_id: str, parquet_file: str, data_type: str):
    target_ptypes = set(TARGET_FOOD_PTYPES if data_type == 'food' else TARGET_MEAT_PTYPES)
    start_dt = get_latest_change_date(parquet_file)
    
    raw_rows = []
    if service_id == "C002":
        print(f"\n=======================================================")
        print(f"🚀 [C002 - food] 품목별 핀포인트 수집 시작 (CHNG_DT >= {start_dt})")
        print(f"=======================================================")
        for idx, pt in enumerate(TARGET_FOOD_PTYPES, 1):
            rows = fetch_api_by_ptype_and_date(service_id, pt, start_dt)
            if rows:
                print(f" [{idx:02d}/{len(TARGET_FOOD_PTYPES)}] {pt}: +{len(rows)}건 수집")
                raw_rows.extend(rows)
            time.sleep(0.1)
    else:
        print(f"\n=======================================================")
        print(f"🚀 [C006 - meat] 축산물 전체 변경분 수집 시작 (CHNG_DT >= {start_dt})")
        print(f"=======================================================")
        raw_rows = fetch_api_all_changes(service_id, start_dt)

    new_rows = [r for r in raw_rows if r.get("PRDLST_DCNM") in target_ptypes]
    print(f"📊 [{data_type}] 수집된 총 데이터 중 타겟 완제품 {len(new_rows):,}건 선별됨")

    new_df = None
    if new_rows:
        new_df = pl.DataFrame(new_rows)
        new_df = new_df.select([pl.col(c).cast(pl.Utf8) for c in new_df.columns])
        if "RAWMTRL_ORDNO" in new_df.columns and "RAWMTRL_NM" in new_df.columns:
            print(f"🔄 신규 수집 데이터 원재료명 배합순 정렬 중...")
            new_df = new_df.with_columns(
                pl.struct(["RAWMTRL_ORDNO", "RAWMTRL_NM"]).map_elements(sort_materials, return_dtype=pl.Utf8).alias("RAWMTRL_NM")
            )

    existing_df = None
    if os.path.exists(parquet_file):
        existing_df = pl.read_parquet(parquet_file)

    if new_df is None and existing_df is None:
        print(f"⚠️ [{parquet_file}] 갱신할 데이터가 없습니다.")
        return

    if existing_df is not None and new_df is not None:
        combined_df = pl.concat([existing_df, new_df], how="diagonal")
    elif existing_df is not None:
        combined_df = existing_df
    else:
        combined_df = new_df

    if "PRDLST_DCNM" in combined_df.columns:
        combined_df = combined_df.filter(pl.col("PRDLST_DCNM").is_in(list(target_ptypes)))

    if "CHNG_DT" in combined_df.columns and "PRDLST_REPORT_NO" in combined_df.columns:
        combined_df = combined_df.sort("CHNG_DT", descending=True).unique(subset=["PRDLST_REPORT_NO"], keep="first")

    if "PRMS_DT" in combined_df.columns:
        combined_df = combined_df.with_columns(
            pl.col("PRMS_DT").str.replace_all("[^0-9]", "").str.slice(0, 8)
        ).filter(pl.col("PRMS_DT").str.len_chars() == 8)

    combined_df.write_parquet(parquet_file, compression="zstd")
    p_size = round(os.path.getsize(parquet_file) / (1024 * 1024), 2)
    print(f"✅ [{parquet_file}] 갱신 및 압축 완료! (총 {combined_df.height:,}행 / {p_size} MB)\n")

if __name__ == "__main__":
    sync_dataset("C002", "data_food.parquet", "food")
    sync_dataset("C006", "data_meat.parquet", "meat")
