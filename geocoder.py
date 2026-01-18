"""
VWorld 지오코딩 API - 주소 → 좌표 변환

사용법:
    python geocoder.py --apt              # 공동주택 CSV 처리
    python geocoder.py --academy          # 학원교습소 CSV 처리
    python geocoder.py --apt --workers 4  # 병렬 처리 (4 스레드)
"""

import argparse
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

# ============================================================
# 설정
# ============================================================

SERVICE_KEY = os.getenv("VWORLD_API_KEY")
BASE_URL = "https://api.vworld.kr/req/address"
DEFAULT_DELAY = 0.05  # API 호출 간격 (초)


# ============================================================
# 지오코딩 API
# ============================================================


def address_to_coordinate(
    address: str,
    addr_type: str = "ROAD",
    max_retries: int = 2,
    timeout: int = 10,
) -> Optional[Tuple[float, float]]:
    """
    주소 → 좌표 변환 (Forward Geocoding)

    Args:
        address: 주소 문자열
        addr_type: 'ROAD' (도로명) 또는 'PARCEL' (지번)
        max_retries: 재시도 횟수
        timeout: 타임아웃 (초)

    Returns:
        (경도, 위도) 또는 None
    """
    if not address or not address.strip():
        return None

    params = {
        "service": "address",
        "request": "getcoord",
        "crs": "epsg:4326",
        "address": address.strip(),
        "format": "json",
        "type": addr_type.lower(),
        "key": SERVICE_KEY,
    }

    for attempt in range(max_retries):
        try:
            response = requests.get(BASE_URL, params=params, timeout=timeout)
            response.raise_for_status()
            data = response.json()

            if data.get("response", {}).get("status") == "OK":
                point = data["response"]["result"].get("point", {})
                if point:
                    return float(point["x"]), float(point["y"])
            return None

        except (requests.exceptions.RequestException, KeyError, ValueError):
            if attempt < max_retries - 1:
                time.sleep(1)
            continue

    return None


# ============================================================
# Fallback 로직
# ============================================================

# 시군구 매핑 (경기도)
SIGUNGU_MAP = {
    "성남시": ["분당구", "수정구", "중원구"],
    "고양시": ["덕양구", "일산동구", "일산서구"],
    "수원시": ["장안구", "팔달구", "권선구", "영통구"],
    "안양시": ["동안구", "만안구"],
    "용인시": ["처인구", "기흥구", "수지구"],
    "부천시": ["원미구", "소사구", "오정구"],
    "안산시": ["단원구", "상록구"],
}


def parse_sigungu_address(address: str) -> str:
    """
    시군구가 붙어있는 주소 파싱
    예: "경기도 수원영통구 매탄동" → "경기도 수원시 영통구 매탄동"
    """
    if not address or not address.startswith("경기도"):
        return address

    for si_name, gu_list in SIGUNGU_MAP.items():
        si_short = si_name.replace("시", "")
        for gu_name in sorted(gu_list, key=len, reverse=True):
            pattern = f"경기도 {si_short}{gu_name}"
            if pattern in address:
                return address.replace(pattern, f"경기도 {si_name} {gu_name}", 1)

    return address


def apply_road_fallback(address: str) -> list[tuple[str, str]]:
    """
    도로명주소 Fallback 변형 생성

    Returns:
        [(변형 주소, 규칙명), ...]
    """
    variants = [(address, "원본")]

    # 퇴계원면 → 퇴계원읍
    if "퇴계원면" in address:
        variants.append((address.replace("퇴계원면", "퇴계원읍"), "퇴계원면→읍"))

    # 읍/면 제거
    addr_no_eup = re.sub(r"\s+\S+읍\s+", " ", address)
    if addr_no_eup != address:
        variants.append((addr_no_eup.strip(), "읍 제거"))

    addr_no_myeon = re.sub(r"\s+\S+면\s+", " ", address)
    if addr_no_myeon != address:
        variants.append((addr_no_myeon.strip(), "면 제거"))

    return variants


def geocode_apt(
    road_address: str,
    parcel_address: str,
) -> tuple[Optional[float], Optional[float], str]:
    """
    공동주택 지오코딩 (도로명 → 지번 → 지번+시군구분리)

    Returns:
        (경도, 위도, 성공규칙) 또는 (None, None, "실패")
    """
    # 1. 도로명주소 (원본만)
    if road_address:
        result = address_to_coordinate(road_address, addr_type="ROAD")
        if result:
            return result[0], result[1], "도로명"
        time.sleep(DEFAULT_DELAY)

    # 2. 지번주소 (원본)
    if parcel_address:
        result = address_to_coordinate(parcel_address, addr_type="PARCEL")
        if result:
            return result[0], result[1], "지번"
        time.sleep(DEFAULT_DELAY)

        # 3. 지번주소 (시군구 분리)
        parsed = parse_sigungu_address(parcel_address)
        if parsed != parcel_address:
            result = address_to_coordinate(parsed, addr_type="PARCEL")
            if result:
                return result[0], result[1], "지번(시군구분리)"
            time.sleep(DEFAULT_DELAY)

    return None, None, "실패"


def geocode_academy(
    road_address: str,
) -> tuple[Optional[float], Optional[float], str]:
    """
    학원교습소 지오코딩 (도로명 fallback: 원본 → 퇴계원면→읍 → 읍/면제거)

    Returns:
        (경도, 위도, 성공규칙) 또는 (None, None, "실패")
    """
    if not road_address:
        return None, None, "실패"

    for addr, rule in apply_road_fallback(road_address):
        result = address_to_coordinate(addr, addr_type="ROAD")
        if result:
            return result[0], result[1], rule
        time.sleep(DEFAULT_DELAY)

    return None, None, "실패"


# ============================================================
# CSV 처리
# ============================================================


def process_apt_row(row: tuple, idx: int, total: int) -> dict:
    """공동주택 행 처리"""
    kapt_code = getattr(row, "kaptCode", "")
    kapt_name = getattr(row, "kaptName", "")
    doro_juso = str(getattr(row, "doroJuso", "")).strip()
    kapt_addr = str(getattr(row, "kaptAddr", "")).strip()

    if pd.isna(doro_juso) or doro_juso == "nan":
        doro_juso = ""
    if pd.isna(kapt_addr) or kapt_addr == "nan":
        kapt_addr = ""

    x, y, rule = geocode_apt(doro_juso, kapt_addr)

    # 실패만 출력 (tqdm과 호환)
    if x is None:
        tqdm.write(f"✗ {kapt_name}: {doro_juso or kapt_addr}")

    return {
        "kaptCode": kapt_code,
        "kaptName": kapt_name,
        "doroJuso": doro_juso,
        "kaptAddr": kapt_addr,
        "경도": x,
        "위도": y,
        "비고": rule,
    }


def process_academy_row(row: tuple, idx: int, total: int) -> dict:
    """학원교습소 행 처리"""
    academy_id = getattr(row, "학원지정번호", "")
    academy_name = getattr(row, "학원명", "")
    road_address = str(getattr(row, "도로명주소", "")).strip()

    if pd.isna(road_address) or road_address == "nan":
        road_address = ""

    x, y, rule = geocode_academy(road_address)

    # 실패만 출력 (tqdm과 호환)
    if x is None:
        tqdm.write(f"✗ {academy_name}: {road_address}")

    return {
        "학원지정번호": academy_id,
        "학원명": academy_name,
        "도로명주소": road_address,
        "경도": x,
        "위도": y,
        "비고": rule,
    }


def process_csv(
    input_path: Path,
    output_path: Path,
    row_processor: callable,
    workers: int = 1,
) -> pd.DataFrame:
    """
    CSV 처리 (병렬 지원)

    Args:
        input_path: 입력 CSV 경로
        output_path: 출력 CSV 경로
        row_processor: 행 처리 함수
        workers: 병렬 스레드 수 (1이면 순차 처리)
    """
    print(f"CSV 읽는 중: {input_path}")
    df = pd.read_csv(input_path, encoding="utf-8-sig")
    total = len(df)
    print(f"총 {total}개 행 처리 (workers={workers})\n")

    results = []
    fail_count = 0

    try:
        if workers <= 1:
            # 순차 처리
            pbar = tqdm(total=total, desc="처리중", unit="건")
            for idx, row in enumerate(df.itertuples(index=False), 1):
                result = row_processor(row, idx, total)
                results.append(result)
                if result.get("경도") is None:
                    fail_count += 1
                pbar.set_postfix({"실패": fail_count})
                pbar.update(1)
            pbar.close()
        else:
            # 병렬 처리
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {}
                for idx, row in enumerate(df.itertuples(index=False), 1):
                    future = executor.submit(row_processor, row, idx, total)
                    futures[future] = idx

                pbar = tqdm(total=total, desc="처리중", unit="건")
                for future in as_completed(futures):
                    result = future.result()
                    results.append(result)
                    if result.get("경도") is None:
                        fail_count += 1
                    pbar.set_postfix({"실패": fail_count})
                    pbar.update(1)
                pbar.close()

        # 결과 저장
        result_df = pd.DataFrame(results)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        result_df.to_csv(output_path, index=False, encoding="utf-8-sig")

        # 통계
        success = result_df["경도"].notna().sum()
        fail = len(result_df) - success

        print(f"\n{'='*60}")
        print(f"완료!")
        print(f"  총: {total}개 | 성공: {success}개 | 실패: {fail}개")
        print(f"  저장: {output_path}")
        print(f"{'='*60}")

        return result_df

    except KeyboardInterrupt:
        print(f"\n\n⚠️ 중단됨. {len(results)}개 저장 중...")
        if results:
            result_df = pd.DataFrame(results)
            interrupted_path = (
                output_path.parent / f"{output_path.stem}_중단{output_path.suffix}"
            )
            result_df.to_csv(interrupted_path, index=False, encoding="utf-8-sig")
            print(f"  저장: {interrupted_path}")
            return result_df
        return pd.DataFrame()


# ============================================================
# 메인
# ============================================================


def main():
    parser = argparse.ArgumentParser(description="VWorld 지오코딩 API")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--apt", action="store_true", help="공동주택 CSV 처리")
    group.add_argument("--academy", action="store_true", help="학원교습소 CSV 처리")
    parser.add_argument(
        "--workers", type=int, default=1, help="병렬 스레드 수 (기본: 1)"
    )
    args = parser.parse_args()

    base_dir = Path(__file__).parent
    data_dir = base_dir / "data"
    output_dir = base_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.apt:
        input_file = data_dir / "국토교통부_공동주택_기본정보.csv"
        output_file = output_dir / "국토교통부_공동주택_기본정보_좌표.csv"

        if not input_file.exists():
            print(f"파일 없음: {input_file}")
            return

        process_csv(input_file, output_file, process_apt_row, args.workers)

    elif args.academy:
        input_file = data_dir / "학원교습소정보.csv"
        output_file = output_dir / "학원교습소정보_좌표.csv"

        if not input_file.exists():
            print(f"파일 없음: {input_file}")
            return

        process_csv(input_file, output_file, process_academy_row, args.workers)


if __name__ == "__main__":
    main()
