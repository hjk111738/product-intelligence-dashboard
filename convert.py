import polars as pl
import os
import glob
import time
import json
from collections import Counter
import re

# 🟢 식품 핵심 타겟 품목 (B2C 완제품 33개)
TARGET_FOOD_PTYPES = {
    "과자", "캔디류", "초콜릿가공품", "추잉껌", "빵류", "초콜릿", "밀크초콜릿", "준초콜릿", 
    "빙과", "당류가공품", "땅콩 또는 견과류가공품", 
    "만두", "어육소시지", "식육함유가공품", "즉석조리식품", "신선편의식품", "소스", 
    "마요네즈", "토마토케첩", "복합조미식품", 
    "고형차", "커피", "과.채주스", "혼합음료", "유산균음료", "액상차", "과.채음료", 
    "기타 영.유아식", "영아용 조제식", "성장기용 조제식", "임산.수유부용 식품", 
    "기타가공품", "두류가공품"
}

# 🔴 축산물 핵심 타겟 품목 (B2C 완제품 15개)
TARGET_MEAT_PTYPES = {
    "비유지방아이스크림", "샤베트", "아이스밀크", "아이스크림", "가공유", "농후발효유", 
    "발효유", "영아용 조제유", "우유", "유당분해우유", "유산균첨가우유", 
    "베이컨류", "분쇄가공육제품", "소시지", "프레스햄"
}

def clean_text(text):
    """특수문자 제거 후 순수 텍스트만 반환 (정규표현식)"""
    if not text: return ""
    return re.sub(r'[^\w\s]', ' ', str(text))

def generate_text_summary(df, data_type):
    print("🔄 텍스트 마이닝 정밀 분석 중 (키워드/원재료 추출)...")
    
    # 1. 품목명(PRDLST_NM) 키워드 정밀 분석 (특수문자 제거 후 띄어쓰기)
    pname_list = df.select("PRDLST_NM").drop_nulls().to_series().to_list()
    pname_words = []
    for p in pname_list:
        cleaned_p = clean_text(p)
        pname_words.extend([w.strip() for w in cleaned_p.split() if len(w.strip()) > 1])
    
    top_pnames = [{"keyword": k, "count": v} for k, v in Counter(pname_words).most_common(50)]

    # 2. 원재료명(RAWMTRL_NM) 정밀 분석 (쉼표 분리 후 여백 제거)
    raw_list = df.select("RAWMTRL_NM").drop_nulls().to_series().to_list()
    raw_materials = []
    for r in raw_list:
        raw_materials.extend([m.strip() for m in str(r).split(',') if len(m.strip()) > 0])
        
    top_raws = [{"material": k, "count": v} for k, v in Counter(raw_materials).most_common(50)]

    summary_data = {
        "pname_chart": top_pnames,
        "raw_chart": top_raws
    }
    
    summary_file = f"summary_{data_type}.json"
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(summary_data, f, ensure_ascii=False, indent=2)
        
    print(f"✅ 텍스트 마이닝 요약 완료: {summary_file}")

def process_folder(folder_name, output_parquet_name, data_type):
    if not os.path.exists(folder_name):
        print(f"[{folder_name}] 폴더가 존재하지 않습니다.")
        return

    csv_files = glob.glob(os.path.join(folder_name, "*.csv"))
    if not csv_files:
        print(f"[{folder_name}] 폴더 내에 CSV 파일이 없습니다.")
        return

    print(f"\n==================================================")
    print(f"📂 [{folder_name}] 전처리 및 병합 시작 (총 {len(csv_files)}개 파일)")
    print(f"==================================================")
    
    start_time = time.time()
    dfs = []
    
    for idx, f in enumerate(csv_files, 1):
        try:
            try:
                df = pl.read_csv(f, infer_schema_length=0, ignore_errors=True)
            except Exception:
                df = pl.read_csv(f, encoding='cp949', infer_schema_length=0, ignore_errors=True)
                
            dfs.append(df)
            print(f" [{idx}/{len(csv_files)}] {os.path.basename(f)} 읽기 완료 ({df.height:,}행)")
        except Exception as e:
            print(f" [{idx}/{len(csv_files)}] ❌ {os.path.basename(f)} 읽기 실패: {e}")

    if not dfs: return

    combined_df = pl.concat(dfs, how="diagonal")

    # 타겟 완제품군만 남기고 불필요 원료/첨가물 필터링
    print(f"🔄 핵심 모니터링 타겟 품목군 필터링 중...")
    target_set = TARGET_FOOD_PTYPES if data_type == 'food' else TARGET_MEAT_PTYPES
    target_list = list(target_set)
    
    if "PRDLST_DCNM" in combined_df.columns:
        original_count = combined_df.height
        combined_df = combined_df.filter(pl.col("PRDLST_DCNM").is_in(target_list))
        print(f"   -> 최적화 결과: {original_count:,}행 중 핵심 완제품 {combined_df.height:,}행 유지")

    def sort_materials(struct_val):
        ord_val = struct_val.get("RAWMTRL_ORDNO")
        mat_val = struct_val.get("RAWMTRL_NM")
        if not ord_val or not mat_val: return mat_val
        try:
            ord_list = [x.strip() for x in str(ord_val).split(',') if x.strip()]
            mat_list = [x.strip() for x in str(mat_val).split(',') if x.strip()]
            if len(ord_list) != len(mat_list) or len(ord_list) == 0: return mat_val
            paired = []
            for o, m in zip(ord_list, mat_list):
                try: paired.append((int(o), m))
                except ValueError: paired.append((9999, m))
            paired.sort(key=lambda x: x[0])
            return ", ".join([p[1] for p in paired])
        except Exception:
            return mat_val

    if "RAWMTRL_ORDNO" in combined_df.columns and "RAWMTRL_NM" in combined_df.columns:
        print(f"🔄 원재료명 배합비(순서) 기준 재배열 중...")
        combined_df = combined_df.with_columns(
            pl.struct(["RAWMTRL_ORDNO", "RAWMTRL_NM"]).map_elements(sort_materials, return_dtype=pl.Utf8).alias("RAWMTRL_NM")
        )

    if "CHNG_DT" in combined_df.columns and "PRDLST_REPORT_NO" in combined_df.columns:
        combined_df = combined_df.sort("CHNG_DT", descending=True).unique(subset=["PRDLST_REPORT_NO"], keep="first")
    
    if "PRMS_DT" in combined_df.columns:
        combined_df = combined_df.with_columns(
            pl.col("PRMS_DT").str.replace_all("[^0-9]", "").str.slice(0, 8)
        ).filter(pl.col("PRMS_DT").str.len_chars() == 8)

    generate_text_summary(combined_df, data_type)

    combined_df.write_parquet(output_parquet_name, compression="zstd")
    
    p_size = round(os.path.getsize(output_parquet_name) / (1024 * 1024), 2)
    elapsed = round(time.time() - start_time, 2)
    print(f"✅ [{output_parquet_name}] 생성 완료! (총 {combined_df.height:,}행 / {p_size} MB / 소요시간: {elapsed}초)")

if __name__ == "__main__":
    try:
        process_folder("raw_food", "data_food.parquet", "food")
        process_folder("raw_meat", "data_meat.parquet", "meat")
        print("\n🎉 모든 데이터 최적화 및 텍스트 마이닝 작업이 성공적으로 완료되었습니다!")
    except Exception as e:
        print(f"\n❌ 작업 중 오류가 발생했습니다: {e}")
    finally:
        print("\n" + "="*50)
        input("📌 작업이 끝났습니다. 창을 닫으려면 Enter 키를 누르세요...")