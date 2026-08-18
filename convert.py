import polars as pl
import os
import glob
import time

def process_folder(folder_name, output_parquet_name):
    if not os.path.exists(folder_name):
        print(f"[{folder_name}] 폴더가 존재하지 않습니다.")
        return

    csv_files = glob.glob(os.path.join(folder_name, "*.csv"))
    if not csv_files:
        print(f"[{folder_name}] 폴더 내에 CSV 파일이 없습니다.")
        return

    print(f"\n==================================================")
    print(f"📂 [{folder_name}] 폴더 변환 시작 (총 {len(csv_files)}개 파일)")
    print(f"==================================================")
    
    start_time = time.time()
    dfs = []
    
    for idx, f in enumerate(csv_files, 1):
        try:
            # utf-8 로드 시도, 실패 시 cp949 로드
            try:
                df = pl.read_csv(f, infer_schema_length=0, ignore_errors=True)
            except Exception:
                df = pl.read_csv(f, encoding='cp949', infer_schema_length=0, ignore_errors=True)
                
            dfs.append(df)
            print(f" [{idx}/{len(csv_files)}] {os.path.basename(f)} 읽기 완료 ({df.height:,}행)")
        except Exception as e:
            print(f" [{idx}/{len(csv_files)}] ❌ {os.path.basename(f)} 읽기 실패: {e}")

    if not dfs:
        print(f"[{folder_name}] 유효한 데이터가 없습니다.")
        return

    print(f"🔄 데이터 병합 및 최적화(중복 제거) 중...")
    combined_df = pl.concat(dfs, how="diagonal")

    # 변경일자(CHNG_DT) 기준 정렬 후 품목제조번호(PRDLST_REPORT_NO) 기준 중복 제거 (최신 데이터 유지)
    if "CHNG_DT" in combined_df.columns and "PRDLST_REPORT_NO" in combined_df.columns:
        combined_df = combined_df.sort("CHNG_DT", descending=True).unique(subset=["PRDLST_REPORT_NO"], keep="first")
    
    # Parquet 압축 저장
    combined_df.write_parquet(output_parquet_name, compression="zstd")
    
    p_size = round(os.path.getsize(output_parquet_name) / (1024 * 1024), 2)
    elapsed = round(time.time() - start_time, 2)
    print(f"✅ [{output_parquet_name}] 생성 완료! (총 {combined_df.height:,}행 / {p_size} MB / 소요시간: {elapsed}초)")

if __name__ == "__main__":
    process_folder("raw_food", "data_food.parquet")
    process_folder("raw_meat", "data_meat.parquet")
    print("\n🎉 모든 변환 작업이 완료되었습니다!")