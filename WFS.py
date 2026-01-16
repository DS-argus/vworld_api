"""
VWorld WFS API를 사용하여 행정구역 데이터를 Parquet으로 저장

사용법:
    python WFS.py                     # 전체 레이어 다운로드
    python WFS.py --layer 시군구      # 특정 레이어만 다운로드
    python WFS.py --layer 시군구 읍면동  # 여러 레이어 다운로드
    python WFS.py --list              # 사용 가능한 레이어 목록 출력
"""

import argparse
import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# 설정
# ============================================================

BASE_URL = "https://api.vworld.kr/req/wfs"
API_KEY = os.getenv("VWORLD_API_KEY")

# 레이어 정의 (표시명: (API 레이어명, ID 필드))
LAYERS = {
    "시군구": ("lt_c_adsigg_info", "sig_cd"),
    "읍면동": ("lt_c_ademd_info", "emd_cd"),
    "리": ("lt_c_adri_info", "li_cd"),
}

# EPSG:4326 (경위도, 단위 degree)
# 요구 형태: (ymin, xmin, ymax, xmax) = (min_lat, min_lon, max_lat, max_lon)
# bbox = (36.893900, 126.379000, 38.281700, 127.848100)

# EPSG:5186 (투영좌표, 단위 meter)
# 요구 형태: (xmin, ymin, xmax, ymax)
# bbox = (144804.345398, 477277.335460, 274943.996015, 631272.674315)

# 서울/경기도 경계 박스 (EPSG:5186 기준)
DEFAULT_BBOX = (144693, 477383, 275745, 633107)  # (minx, miny, maxx, maxy)

# STARTINDEX 최대값 (API 제한)
MAX_STARTINDEX = 1000


# ============================================================
# API 호출
# ============================================================

PAGE_SIZE = 1000  # API 최대값


def fetch_wfs_page(
    layer_name: str,
    start_index: int = 0,
    bbox: Optional[Tuple[float, float, float, float]] = None,
    timeout: int = 300,
) -> Optional[Dict]:
    """
    WFS API를 호출하여 단일 페이지의 GeoJSON 데이터 반환

    Args:
        layer_name: API 레이어명 (예: lt_c_adsigg_info)
        start_index: 시작 인덱스 (페이지네이션용)
        bbox: 경계 박스 (minx, miny, maxx, maxy) - EPSG:5186 기준
        timeout: 요청 타임아웃 (초)

    Returns:
        GeoJSON 형식의 딕셔너리, 실패 시 None
    """
    params = {
        "SERVICE": "WFS",
        "VERSION": "2.0.0",
        "REQUEST": "GetFeature",
        "TYPENAME": layer_name,
        "SRSNAME": "EPSG:5186",
        "OUTPUT": "application/json",
        "COUNT": PAGE_SIZE,
        "STARTINDEX": start_index,
    }

    if bbox:
        params["BBOX"] = f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]},EPSG:5186"

    try:
        url = f"{BASE_URL}?key={API_KEY}"
        response = requests.get(url, params=params, timeout=timeout)
        response.raise_for_status()
        return response.json()

    except json.JSONDecodeError:
        print(f"\n    ✗ JSON 파싱 실패")
        print(f"    응답 상태 코드: {response.status_code}")
        print(f"    응답 내용 (처음 500자):\n{response.text[:500]}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"    ✗ 요청 실패: {e}")
        return None


def split_bbox(
    bbox: Tuple[float, float, float, float],
    divisions: int = 2,
) -> List[Tuple[float, float, float, float]]:
    """
    BBOX를 격자로 분할

    Args:
        bbox: (minx, miny, maxx, maxy)
        divisions: 가로/세로 분할 수 (2면 4등분, 3이면 9등분)

    Returns:
        분할된 BBOX 리스트
    """
    minx, miny, maxx, maxy = bbox
    width = (maxx - minx) / divisions
    height = (maxy - miny) / divisions

    boxes = []
    for i in range(divisions):
        for j in range(divisions):
            box = (
                minx + i * width,
                miny + j * height,
                minx + (i + 1) * width,
                miny + (j + 1) * height,
            )
            boxes.append(box)

    return boxes


def fetch_wfs_bbox(
    layer_name: str,
    bbox: Tuple[float, float, float, float],
) -> List[Dict]:
    """
    단일 BBOX에서 페이지네이션하여 피처 수집 (최대 2000개)

    Args:
        layer_name: API 레이어명
        bbox: 경계 박스

    Returns:
        피처 리스트
    """
    features = []
    start_index = 0

    while start_index <= MAX_STARTINDEX:
        data = fetch_wfs_page(layer_name, start_index=start_index, bbox=bbox)

        if not data or "features" not in data:
            break

        page_features = data["features"]
        count = len(page_features)

        if count == 0:
            break

        features.extend(page_features)

        # 1000개 미만이면 마지막 페이지
        if count < PAGE_SIZE:
            break

        start_index += PAGE_SIZE

    return features


def fetch_wfs_all(
    layer_name: str,
    bbox: Optional[Tuple[float, float, float, float]] = None,
    id_field: str = "li_cd",
) -> List[Dict]:
    """
    WFS API를 BBOX 분할 + 페이지네이션으로 모든 피처 수집

    STARTINDEX가 1000까지만 허용되므로, BBOX를 분할하여 수집합니다.

    Args:
        layer_name: API 레이어명
        bbox: 경계 박스
        id_field: 중복 제거용 ID 필드명

    Returns:
        모든 피처 리스트 (중복 제거됨)
    """
    if not bbox:
        bbox = DEFAULT_BBOX

    # 1차 시도: 분할 없이 시도
    print(f"    [1차 시도] 전체 영역 요청...")
    all_features = []
    seen_ids = set()

    start_index = 0
    page_num = 1
    need_split = False

    while start_index <= MAX_STARTINDEX:
        print(f"    - 페이지 {page_num} (startindex={start_index})...", end=" ")

        data = fetch_wfs_page(layer_name, start_index=start_index, bbox=bbox)

        if not data or "features" not in data:
            # STARTINDEX 초과 에러 체크는 이미 처리됨
            print("✗ 실패")
            break

        features = data["features"]
        count = len(features)
        print(f"✓ {count}개")

        if count == 0:
            break

        for f in features:
            fid = f.get("properties", {}).get(id_field)
            if fid and fid not in seen_ids:
                seen_ids.add(fid)
                all_features.append(f)

        # 1000개 미만이면 마지막 페이지
        if count < PAGE_SIZE:
            break

        # STARTINDEX 한계에 도달하면 분할 필요
        if start_index + PAGE_SIZE > MAX_STARTINDEX:
            need_split = True
            print(f"    ⚠️  STARTINDEX 한계 도달 (1000), BBOX 분할 진행...")
            break

        start_index += PAGE_SIZE
        page_num += 1

    # 2차 시도: BBOX 분할
    if need_split:
        divisions = 3  # 9등분
        split_boxes = split_bbox(bbox, divisions)
        print(
            f"    [2차 시도] BBOX를 {divisions}x{divisions}={len(split_boxes)}개로 분할"
        )

        for idx, sub_bbox in enumerate(split_boxes, 1):
            print(f"    - 영역 {idx}/{len(split_boxes)} 처리 중...")

            sub_features = fetch_wfs_bbox(layer_name, sub_bbox)

            # 중복 제거하면서 추가
            new_count = 0
            for f in sub_features:
                fid = f.get("properties", {}).get(id_field)
                if fid and fid not in seen_ids:
                    seen_ids.add(fid)
                    all_features.append(f)
                    new_count += 1

            print(f"      ✓ {len(sub_features)}개 수집, {new_count}개 신규 추가")

    return all_features


# ============================================================
# 데이터 변환
# ============================================================


def features_to_dataframe(features: List[Dict]) -> pd.DataFrame:
    """
    GeoJSON features 리스트를 DataFrame으로 변환 (geometry는 JSON 문자열로 저장)

    Args:
        features: GeoJSON features 리스트

    Returns:
        DataFrame (geometry 컬럼은 JSON 문자열)
    """
    if not features:
        return pd.DataFrame()

    rows = []
    for feature in features:
        row = feature.get("properties", {}).copy()
        # geometry를 JSON 문자열로 저장
        if "geometry" in feature and feature["geometry"]:
            row["geometry"] = json.dumps(feature["geometry"], ensure_ascii=False)
        else:
            row["geometry"] = None
        rows.append(row)

    return pd.DataFrame(rows)


# ============================================================
# 메인 로직
# ============================================================


def download_layer(
    display_name: str,
    api_layer_name: str,
    id_field: str,
    output_dir: Path,
    bbox: Optional[Tuple[float, float, float, float]] = None,
) -> bool:
    """
    단일 레이어 다운로드 및 저장 (페이지네이션 + BBOX 분할로 전체 수집)

    Args:
        display_name: 표시명 (파일명으로 사용)
        api_layer_name: API 레이어명
        id_field: 중복 제거용 ID 필드명
        output_dir: 출력 디렉토리
        bbox: 경계 박스

    Returns:
        성공 여부
    """
    print(f"\n[{display_name}] 데이터 수집 중...")
    print(f"  - API 레이어: {api_layer_name}")
    print(f"  - ID 필드: {id_field}")

    # 모든 피처 수집 (BBOX 분할 + 페이지네이션)
    all_features = fetch_wfs_all(api_layer_name, bbox=bbox, id_field=id_field)

    if not all_features:
        print(f"    ✗ 데이터 없음")
        return False

    # DataFrame 변환
    df = features_to_dataframe(all_features)
    if df.empty:
        print(f"    ✗ DataFrame 변환 실패")
        return False

    print(f"  ✓ 총 {len(df)}개 피처 수집 완료")

    # Parquet 저장
    output_file = output_dir / f"{display_name}.parquet"
    df.to_parquet(output_file, index=False)
    print(f"  ✓ 저장 완료: {output_file}")

    return True


def main():
    """메인 실행 함수"""
    # 인자 파싱
    parser = argparse.ArgumentParser(
        description="VWorld WFS API로 행정구역 데이터 다운로드"
    )
    parser.add_argument(
        "--layer",
        nargs="*",
        help="다운로드할 레이어 (예: 시군구 읍면동). 지정하지 않으면 전체 다운로드",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="사용 가능한 레이어 목록 출력",
    )
    args = parser.parse_args()

    # 레이어 목록 출력
    if args.list:
        print("사용 가능한 레이어:")
        for name, (api_name, id_field) in LAYERS.items():
            print(f"  - {name}: {api_name} (ID: {id_field})")
        return

    print("=" * 60)
    print("VWorld WFS API - 행정구역 데이터 다운로드")
    print("=" * 60)

    # 출력 디렉토리 생성
    output_dir = Path(__file__).parent / "output" / "WFS"
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"출력 디렉토리: {output_dir}")

    # 다운로드할 레이어 결정
    if args.layer:
        # 지정된 레이어만
        target_layers = {}
        for name in args.layer:
            if name in LAYERS:
                target_layers[name] = LAYERS[name]
            else:
                print(f"⚠️  알 수 없는 레이어: {name} (--list로 목록 확인)")
    else:
        # 전체 레이어
        target_layers = LAYERS

    if not target_layers:
        print("다운로드할 레이어가 없습니다.")
        return

    print(f"다운로드 대상: {', '.join(target_layers.keys())}")

    # 각 레이어 다운로드
    success_count = 0
    fail_count = 0

    for display_name, (api_layer_name, id_field) in target_layers.items():
        if download_layer(
            display_name, api_layer_name, id_field, output_dir, bbox=DEFAULT_BBOX
        ):
            success_count += 1
        else:
            fail_count += 1

    # 결과 출력
    print(f"\n" + "=" * 60)
    print(f"작업 완료!")
    print(f"  성공: {success_count}개")
    print(f"  실패: {fail_count}개")
    print(f"  저장 경로: {output_dir}")
    print("=" * 60)


if __name__ == "__main__":
    main()
