import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

SERVICE_KEY = os.getenv("VWORLD_API_KEY")

# API 기본 URL
BASE_URL = "https://api.vworld.kr/req/address"


def coordinate_to_address(
    x: float,
    y: float,
    max_retries: int = 3,
    timeout: int = 30,
) -> str:
    """
    좌표를 주소로 변환하는 함수 (Reverse Geocoding)

    Args:
        x: 경도 (Longitude)
        y: 위도 (Latitude)
        max_retries: 최대 재시도 횟수 (기본값: 3)
        timeout: 요청 타임아웃 초 (기본값: 30)

    Returns:
        변환된 주소 문자열 (도로명주소) (실패 시 빈 문자열 반환)
    """
    params = {
        "service": "address",
        "request": "getAddress",
        "version": "2.0",
        "crs": "epsg:4326",
        "point": f"{x},{y}",
        "format": "json",
        "type": "both",  # both: 도로명+지번, road: 도로명만, parcel: 지번만
        "key": SERVICE_KEY,
    }

    for attempt in range(max_retries):
        try:
            response = requests.get(BASE_URL, params=params, timeout=timeout)
            response.raise_for_status()

            data = response.json()

            if data.get("response", {}).get("status") == "OK":
                result = data["response"]["result"][0]
                # 도로명주소를 우선 반환, 없으면 지번주소 반환
                return result.get("text", result.get("zipcode", ""))
            else:
                return ""

        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2
                print(
                    f"(타임아웃, {wait_time}초 후 재시도 {attempt + 2}/{max_retries}) ",
                    end="",
                )
                time.sleep(wait_time)
            else:
                print("(타임아웃 최대 재시도 초과) ", end="")
                return ""

        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2
                print(
                    f"(네트워크 오류, {wait_time}초 후 재시도 {attempt + 2}/{max_retries}) ",
                    end="",
                )
                time.sleep(wait_time)
            else:
                print(f"(네트워크 오류 최대 재시도 초과: {e}) ", end="")
                return ""

    return ""


def address_to_coordinate(
    address: str,
    addr_type: str = "ROAD",
    crs: str = "EPSG:4326",
    max_retries: int = 3,
    timeout: int = 30,
) -> Optional[Tuple[float, float]]:
    """
    주소를 좌표로 변환하는 함수 (Forward Geocoding)

    Args:
        address: 변환할 주소 문자열
        addr_type: 주소 타입 ('ROAD': 도로명주소, 'PARCEL': 지번주소), 기본값: 'ROAD'
        crs: 좌표계 (기본값: 'EPSG:4326' WGS84)
        max_retries: 최대 재시도 횟수 (기본값: 3)
        timeout: 요청 타임아웃 초 (기본값: 30)

    Returns:
        (경도, 위도) 튜플. 실패 시 None 반환

    Note:
        VWorld API 응답 결과는 실시간 사용만 허용되며, 별도 저장장치/DB에 저장 불가
    """
    # 파라미터 정규화
    t = addr_type.strip().upper()
    crs_norm = crs.strip().upper()

    if t not in ("ROAD", "PARCEL"):
        print(f"type 파라미터 오류: {addr_type} (ROAD 또는 PARCEL 허용)")
        return None

    # 참고 코드 형식에 맞춰 파라미터 설정
    params = {
        "service": "address",
        "request": "getcoord",
        "crs": crs_norm.lower(),
        "address": address,
        "format": "json",
        "type": t.lower(),  # road, parcel
        "key": SERVICE_KEY,
    }

    # 재시도 로직
    for attempt in range(max_retries):
        try:
            response = requests.get(BASE_URL, params=params, timeout=timeout)
            response.raise_for_status()

            data = response.json()

            # 응답 구조: data["response"]["status"], data["response"]["result"]["point"]
            response_obj = data.get("response", {})
            status = response_obj.get("status", "")

            if status == "OK":
                result = response_obj.get("result", {})
                point = result.get("point", {})

                if point:
                    # x=경도(lon), y=위도(lat)
                    x = float(point.get("x", 0))
                    y = float(point.get("y", 0))
                    return x, y
            else:
                # API 응답은 왔지만 결과가 없는 경우 (주소를 찾을 수 없음)
                return None

        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2  # 2초, 4초, 6초...
                print(
                    f"(타임아웃, {wait_time}초 후 재시도 {attempt + 2}/{max_retries}) ",
                    end="",
                )
                time.sleep(wait_time)
            else:
                print("(타임아웃 최대 재시도 초과) ", end="")
                return None

        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2
                print(
                    f"(네트워크 오류, {wait_time}초 후 재시도 {attempt + 2}/{max_retries}) ",
                    end="",
                )
                time.sleep(wait_time)
            else:
                print(f"(네트워크 오류 최대 재시도 초과: {e}) ", end="")
                return None

    return None


def parse_sigungu_address(address: str) -> str:
    """
    법정동주소에서 시군구가 붙어있는 경우를 파싱하여 수정하는 함수
    예: "경기도 수원영통구 매탄동 897" -> "경기도 수원시 영통구 매탄동 897"
        "경기도 성남분당구 이매동 321" -> "경기도 성남시 분당구 이매동 321"

    Args:
        address: 원본 주소 문자열

    Returns:
        파싱된 주소 문자열
    """
    if not address or pd.isna(address):
        return ""

    address = str(address).strip()

    # 시군구 매핑 딕셔너리
    sigungu_map = {
        "성남시": ["분당구", "수정구", "중원구"],
        "고양시": ["덕양구", "일산동구", "일산서구"],
        "수원시": ["장안구", "팔달구", "권선구", "영통구"],
        "안양시": ["동안구", "만안구"],
        "용인시": ["처인구", "기흥구", "수지구"],
        "부천시": ["원미구", "소사구", "오정구"],
        "안산시": ["단원구", "상록구"],
    }

    # "경기도"로 시작하는지 확인
    if not address.startswith("경기도"):
        return address

    # 각 시와 구 조합을 확인 (긴 구 이름부터 매칭하도록 정렬)
    for si_name, gu_list in sigungu_map.items():
        # 구 이름을 길이 순으로 정렬 (긴 것부터)
        sorted_gu_list = sorted(gu_list, key=len, reverse=True)

        for gu_name in sorted_gu_list:
            # "경기도 {시}{구}" 패턴 찾기 (예: "경기도 수원영통구")
            si_name_without_si = si_name.replace("시", "")
            pattern = f"경기도 {si_name_without_si}{gu_name}"

            if pattern in address:
                # "경기도 {시}시 {구}"로 교체
                replacement = f"경기도 {si_name} {gu_name}"
                address = address.replace(pattern, replacement, 1)
                # 한 번만 교체하고 종료
                return address

    return address


def add_coordinates_to_apt_csv(
    input_csv_path: str | Path,
    output_csv_path: str | Path,
    delay: float = 0.1,
) -> pd.DataFrame:
    """
    공동주택 CSV 파일의 도로명주소(doroJuso)를 좌표로 변환하여 새로운 CSV 생성

    Args:
        input_csv_path: 입력 CSV 파일 경로 (국토교통부_공동주택_기본정보.csv)
        output_csv_path: 출력 CSV 파일 경로 (기본 경로, 진행도가 파일명에 추가됨)
        limit: 처리할 행 수 제한 (None이면 전체, 테스트용으로 20 등 사용)
        delay: API 호출 간 딜레이(초)

    Returns:
        좌표가 추가된 DataFrame (kaptCode, kaptName, doroJuso, kaptAddr, 경도, 위도, 비고)

    Note:
        모든 결과는 메모리에 저장되며, 정상 완료 시 또는 에러/중단 시에만 파일로 저장됩니다.
    """
    input_path = Path(input_csv_path)
    base_output_path = Path(output_csv_path)

    print(f"CSV 파일 읽는 중: {input_path}")
    df = pd.read_csv(input_path, encoding="utf-8-sig")
    total_count = len(df)
    print(f"총 {total_count}개 행 처리 시작...\n")

    # 결과를 저장할 리스트
    results = []

    try:
        for idx, row in enumerate(df.itertuples(index=False), 1):
            kapt_code = getattr(row, "kaptCode", "")
            kapt_name = getattr(row, "kaptName", "")
            doro_juso = getattr(row, "doroJuso", "")
            kapt_addr = getattr(row, "kaptAddr", "")

            coordinates = None
            x, y = None, None
            note = "실패(4)"  # 기본값: 실패

            # 주소 정리
            cleaned_doro = str(doro_juso).strip() if not pd.isna(doro_juso) else ""
            cleaned_parcel = str(kapt_addr).strip() if not pd.isna(kapt_addr) else ""
            prefix = f"[{idx}/{total_count}] {kapt_name} ({kapt_code})"

            # 1차 시도: 도로명주소 (doroJuso)
            if cleaned_doro:
                print(f"{prefix} - 도로명주소: {cleaned_doro} ... ", end="")
                coordinates = address_to_coordinate(cleaned_doro, addr_type="ROAD")
                if coordinates:
                    x, y = coordinates
                    note = "도로명(1)"
                    print(f"✓ 좌표: ({x}, {y})")

            # 2차 시도: 지번주소 (kaptAddr)
            if not coordinates and cleaned_parcel:
                print(f"{prefix} - 지번주소: {cleaned_parcel} ... ", end="")
                coordinates = address_to_coordinate(cleaned_parcel, addr_type="PARCEL")
                if coordinates:
                    x, y = coordinates
                    note = "지번(2)"
                    print(f"✓ 좌표: ({x}, {y})")

            # 3차 시도: 시군구 파싱 후 지번주소 재시도
            if not coordinates and cleaned_parcel:
                parsed_parcel = parse_sigungu_address(cleaned_parcel)
                if parsed_parcel != cleaned_parcel:
                    print(f"✗ 실패 -> 파싱된 지번주소: {parsed_parcel} ... ", end="")
                    coordinates = address_to_coordinate(
                        parsed_parcel, addr_type="PARCEL"
                    )
                    if coordinates:
                        x, y = coordinates
                        note = "시군구분리(3)"
                        print(f"✓ 좌표: ({x}, {y})")
                    else:
                        print("✗ 최종 실패")
                else:
                    print("✗ 최종 실패")

            # 주소가 모두 없는 경우
            if not cleaned_doro and not cleaned_parcel:
                print(f"{prefix}: 주소 없음")

            # 결과 저장 (메모리에만 저장)
            results.append(
                {
                    "kaptCode": kapt_code,
                    "kaptName": kapt_name,
                    "doroJuso": doro_juso,
                    "kaptAddr": kapt_addr,
                    "경도": x,
                    "위도": y,
                    "비고": note,
                }
            )

            # API 호출 제한 방지
            time.sleep(delay)

        # 전체 완료 시 최종 저장
        result_df = pd.DataFrame(results)
        final_output_path = (
            base_output_path.parent
            / f"{base_output_path.stem}_({len(results)}_{total_count}){base_output_path.suffix}"
        )
        final_output_path.parent.mkdir(parents=True, exist_ok=True)
        result_df.to_csv(final_output_path, index=False, encoding="utf-8-sig")

        # 통계 출력
        success_count = result_df["x"].notna().sum()
        fail_count = len(result_df) - success_count

        print(f"\n" + "=" * 60)
        print(f"작업 완료!")
        print(f"  총 처리: {len(result_df)}개")
        print(f"  성공: {success_count}개")
        print(f"  실패: {fail_count}개")
        print(f"  저장 경로: {final_output_path}")
        print("=" * 60)

        return result_df

    except KeyboardInterrupt:
        # 사용자가 중단한 경우 (Ctrl+C)
        print(f"\n\n⚠️  작업이 중단되었습니다. 현재까지 처리된 데이터를 저장합니다...")

        if len(results) > 0:
            result_df = pd.DataFrame(results)
            # 진행도가 포함된 파일명 생성
            interrupted_output_path = (
                base_output_path.parent
                / f"{base_output_path.stem}_({len(results)}_{total_count})_interrupted{base_output_path.suffix}"
            )
            interrupted_output_path.parent.mkdir(parents=True, exist_ok=True)
            result_df.to_csv(interrupted_output_path, index=False, encoding="utf-8-sig")

            success_count = result_df["경도"].notna().sum()
            fail_count = len(result_df) - success_count

            print(f"\n" + "=" * 60)
            print(f"중단된 작업 저장 완료!")
            print(f"  처리된 항목: {len(result_df)}개 / {total_count}개")
            print(f"  성공: {success_count}개")
            print(f"  실패: {fail_count}개")
            print(f"  저장 경로: {interrupted_output_path}")
            print("=" * 60)

            return result_df
        else:
            print("저장할 데이터가 없습니다.")
            return pd.DataFrame()

    except Exception as e:
        # 기타 예외 발생 시
        print(f"\n\n❌ 오류 발생: {e}")
        print("현재까지 처리된 데이터를 저장합니다...")

        if len(results) > 0:
            result_df = pd.DataFrame(results)
            # 진행도가 포함된 파일명 생성
            error_output_path = (
                base_output_path.parent
                / f"{base_output_path.stem}_({len(results)}_{total_count})_error{base_output_path.suffix}"
            )
            error_output_path.parent.mkdir(parents=True, exist_ok=True)
            result_df.to_csv(error_output_path, index=False, encoding="utf-8-sig")

            print(f"  저장 경로: {error_output_path}")
            return result_df
        else:
            return pd.DataFrame()


# 사용 예시
if __name__ == "__main__":
    print("공동주택 CSV에 좌표 추가 (전체)")
    print("=" * 60)

    input_path = Path(__file__).parent / "data"
    output_path = Path(__file__).parent / "output"
    output_path.mkdir(parents=True, exist_ok=True)

    if input_path.exists():
        result_df = add_coordinates_to_apt_csv(
            input_csv_path=input_path / "국토교통부_공동주택_기본정보.csv",
            output_csv_path=output_path / "국토교통부_공동주택_기본정보_좌표.csv",
            delay=0.1,
        )
    else:
        print(
            f"입력 파일을 찾을 수 없습니다: {input_path / '국토교통부_공동주택_기본정보.csv'}"
        )
