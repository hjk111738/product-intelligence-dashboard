import os
import time
import requests
from datetime import datetime, timedelta
import polars as pl

API_KEY = os.environ.get("FOOD_API_KEY", "b7fa39c1e193461cbd94")

TARGET_FOOD_PTYPES = {
    "과자", "캔디류", "초콜릿가공품", "추잉껌", "빵류", "초콜릿", "밀크초콜릿", "준초콜릿", 
    "빙과", "당류가공품", "땅콩 또는 견과류가공품", 
    "만두", "어육소시지", "식육함유가공품", "즉석조리식품", "신선편의식품", "소스", 
    "마요네즈", "토마토케첩", "복합조미식품", 
    "고형차", "커피", "과.채주스", "혼합음료", "유산균음료", "액상차", "과.채음료", 
    "기타 영.유아식", "영아용 조제식", "성장기용 조제식", "임산.수유부용 식품", 
    "기타가공품", "두류가공품"
}

TARGET_MEAT_PTYPES = {
    "비유지방아이스크림", "샤베트", "아이스밀크", "아이스크림", "가공유", "농후발효유", 
    "발효유", "영아용 조제유", "우유", "유당분해우유", "유산균첨가우유", 
    "베이컨류", "분쇄가공육제품", "소시지", "프레스햄"
}

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

def fetch_api_data(service_id: str, days_back: int = 14):
    end_dt = datetime.now().strftime("%Y%m%d")
    start_dt = (datetime.now() - timedelta(days=days_back)).strftime("%Y%m%d")
    print(f"📡 [{service_id}] API 수집 시작 ({start_dt} ~ {end_dt})")
    
    rows = []
    start_idx = 1
    page_size = 1000
    
    while True:
        end_idx = start_idx + page_size - 1
        url = f"http://openapi.foodsafetykorea.go.kr/api/{API_KEY}/{service_id}/json/{start_idx}/{end_idx}"
        try:
            res = requests.get(url, timeout=30).json()
            data_block = res.get(service_id, {})
            result = data_block.get("RESULT", {})
            
            if result.get("CODE") != "INFO-000":
                break
                
            chunk = data_block.get("row", [])
            if not chunk:
                break
                
            rows.extend(chunk)
            total_count = int(data_block.get("total_count", 0))
            print(f"   -> {len(rows):,} / {total_count:,} 건 수집 중...")
            
            if len(rows) >= total_count:
                break
                
            start_idx += page_size
            time.sleep(0.2)
        except Exception as e:
            print(f"   ❌ 요청 실패: {e}")
            break
            
    print(f"✅ [{service_id}] 수집 완료: {len(rows):,}건")
    return rows

def process_and_merge(service_id: str, parquet_file: str, data_type: str):
    new_rows = fetch_api_data(service_id, days_back=14)
    new_df = pl.DataFrame(new_rows) if new_rows else None
    if new_df is not None:
        new_df = new_df.select([pl.col(c).cast(pl.Utf8) for c in new_df.columns])
    
    existing_df = None
    if os.path.exists(parquet_file):
        existing_df = pl.read_parquet(parquet_file)
        
    if new_df is None and existing_df is None:
        return
        
    if existing_df is not None and new_df is not None:
        combined_df = pl.concat([existing_df, new_df], how="diagonal")
    elif existing_df is not None:
        combined_df = existing_df
    else:
        combined_df = new_df

    target_set = TARGET_FOOD_PTYPES if data_type == 'food' else TARGET_MEAT_PTYPES
    if "PRDLST_DCNM" in combined_df.columns:
        combined_df = combined_df.filter(pl.col("PRDLST_DCNM").is_in(list(target_set)))

    if "RAWMTRL_ORDNO" in combined_df.columns and "RAWMTRL_NM" in combined_df.columns:
        combined_df = combined_df.with_columns(
            pl.struct(["RAWMTRL_ORDNO", "RAWMTRL_NM"]).map_elements(sort_materials, return_dtype=pl.Utf8).alias("RAWMTRL_NM")
        )

    if "CHNG_DT" in combined_df.columns and "PRDLST_REPORT_NO" in combined_df.columns:
        combined_df = combined_df.sort("CHNG_DT", descending=True).unique(subset=["PRDLST_REPORT_NO"], keep="first")
    
    if "PRMS_DT" in combined_df.columns:
        combined_df = combined_df.with_columns(
            pl.col("PRMS_DT").str.replace_all("[^0-9]", "").str.slice(0, 8)
        ).filter(pl.col("PRMS_DT").str.len_chars() == 8)

    combined_df.write_parquet(parquet_file, compression="zstd")
    print(f"🎉 [{parquet_file}] 완료 ({combined_df.height:,}행)")

if __name__ == "__main__":
    process_and_merge("C002", "data_food.parquet", "food")
    process_and_merge("I0300", "data_meat.parquet", "meat")
