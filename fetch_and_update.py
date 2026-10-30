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

def get_latest_change_date(parquet_file: str, fallback_days: int = 14) -> str:
    if os.path.exists(parquet_file):
        try:
            df = pl.read_parquet(parquet_file)
            if "CHNG_DT" in df.columns:
                valid_dates = df.filter(pl.col("CHNG_DT").str.len_chars() == 8)["CHNG_DT"]
                if valid_dates.len() > 0:
                    max_dt_str = valid_dates.max()
                    # 안전 버퍼 3일 전부터 조회
                    base_dt = datetime.strptime(max_dt_str, "%Y%m%d") - timedelta(days=3)
                    start_str = base_dt.strftime("%Y%m%d")
                    print(f"📌 [{parquet_file}] 기존 최신 변경일자: {max_dt_str} -> 수집 시작일: {start_str}")
                    return start_str
        except Exception as e:
            print(f"⚠️ 기존 파일 날짜 확인 중 오류 ({e}), 기본값 사용")

    fallback_str = (datetime.now() - timedelta(days=fallback_days)).strftime("%Y%m%d")
    print(f"📌 [{parquet_file}] 기본 수집 시작일 적용: {fallback_str}")
    return fallback_str

def fetch_api_all_changes(service_id: str, start_dt: str):
    """
    CHNG_DT=시작일 이후의 변경 데이터를 페이징(1000건씩)으로 누락 없이 일괄 수집
    """
    collected = []
    start_idx = 1
    page_size = 1000

    print(f"📡 [{service_id}] API 수집 시작 (기준일 이후: {start_dt})")

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
            print(f"   -> {len(collected):,} / 총 {total_count:,}건 수집 중...")

            if len(collected) >= total_count:
                break

            start_idx += page_size
            time.sleep(0.2)
        except Exception as e:
            print(f"   ❌ API 호출 오류: {e}")
            break

    print(f"✅ [{service_id}] 수집 완료: 총 {len(collected):,}건")
    return collected

def sync_dataset(service_id: str, parquet_file: str, data_type: str):
    target_ptypes = set(TARGET_FOOD_PTYPES if data_type == 'food' else TARGET_MEAT_PTYPES)
    start_dt = get_latest_change_date(parquet_file, fallback_days=14)
    
    # 1. API에서 해당일 이후 변경분 일괄 수집
    raw_rows = fetch_api_all_changes(service_id, start_dt)

    # 2. 타겟 품목유형(B2C)에 해당하는 것만 필터링
    new_rows = [r for r in raw_rows if r.get("PRDLST_DCNM") in target_ptypes]
    print(f"📊 [{data_type}] 전체 변경분 {len(raw_rows):,}건 중 타겟 완제품 {len(new_rows):,}건 선별됨")

    new_df = None
    if new_rows:
        new_df = pl.DataFrame(new_rows)
        new_df = new_df.select([pl.col(c).cast(pl.Utf8) for c in new_df.columns])
        if "RAWMTRL_ORDNO" in new_df.columns and "RAWMTRL_NM" in new_df.columns:
            print(f"🔄 신규 수집 데이터 원재료명 배합순 정렬 중...")
            new_df = new_df.with_columns(
                pl.struct(["RAWMTRL_ORDNO", "RAWMTRL_NM"]).map_elements(sort_materials, return_dtype=pl.Utf8).alias("RAWMTRL_NM")
            )

    # 3. 기존 Parquet 로드 및 병합
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

    # 4. 품목유형 재확인 필터링
    if "PRDLST_DCNM" in combined_df.columns:
        combined_df = combined_df.filter(pl.col("PRDLST_DCNM").is_in(list(target_ptypes)))

    # 5. 최신 변경일자(CHNG_DT) 기준 중복 제거 (최신 스펙 덮어쓰기)
    if "CHNG_DT" in combined_df.columns and "PRDLST_REPORT_NO" in combined_df.columns:
        combined_df = combined_df.sort("CHNG_DT", descending=True).unique(subset=["PRDLST_REPORT_NO"], keep="first")

    # 6. 등록일자 정규화
    if "PRMS_DT" in combined_df.columns:
        combined_df = combined_df.with_columns(
            pl.col("PRMS_DT").str.replace_all("[^0-9]", "").str.slice(0, 8)
        ).filter(pl.col("PRMS_DT").str.len_chars() == 8)

    # 7. Parquet 저장
    combined_df.write_parquet(parquet_file, compression="zstd")
    p_size = round(os.path.getsize(parquet_file) / (1024 * 1024), 2)
    print(f"✅ [{parquet_file}] 갱신 및 압축 완료! (총 {combined_df.height:,}행 / {p_size} MB)\n")

if __name__ == "__main__":
    sync_dataset("C002", "data_food.parquet", "food")
    sync_dataset("I0300", "data_meat.parquet", "meat")
