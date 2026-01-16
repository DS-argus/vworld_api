import os
import time
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

SERVICE_KEY = os.getenv("VWORLD_API_KEY")

# API 기본 URL
BASE_URL = "https://api.vworld.kr/req/address"


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


def add_coordinates_to_academy_csv(
    input_csv_path: str | Path,
    output_csv_path: str | Path,
    delay: float = 0.1,
) -> pd.DataFrame:
    """
    학원교습소 CSV 파일의 도로명주소를 좌표로 변환하여 새로운 CSV 생성

    Args:
        input_csv_path: 입력 CSV 파일 경로 (학원교습소정보.csv)
        output_csv_path: 출력 CSV 파일 경로 (기본 경로, 진행도가 파일명에 추가됨)
        delay: API 호출 간 딜레이(초)

    Returns:
        좌표가 추가된 DataFrame

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
            academy_id = getattr(row, "학원지정번호", "")
            academy_name = getattr(row, "학원명", "")
            road_address = getattr(row, "도로명주소", "")

            coordinates = None
            x, y = None, None
            note = "실패"  # 기본값: 실패

            # 주소 정리
            cleaned_road = (
                str(road_address).strip() if not pd.isna(road_address) else ""
            )
            prefix = f"[{idx}/{total_count}] {academy_name} ({academy_id})"

            # 도로명주소로 시도
            if cleaned_road:
                print(f"{prefix} - 도로명주소: {cleaned_road} ... ", end="")
                coordinates = address_to_coordinate(cleaned_road, addr_type="ROAD")
                if coordinates:
                    x, y = coordinates
                    note = "성공"
                    print(f"✓ 좌표: ({x}, {y})")
                else:
                    print("✗ 실패")
            else:
                # 주소가 없는 경우
                print(f"{prefix}: 주소 없음")

            # 결과 저장 (메모리에만 저장)
            results.append(
                {
                    "학원지정번호": academy_id,
                    "학원명": academy_name,
                    "도로명주소": road_address,
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
        success_count = result_df["경도"].notna().sum()
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
    print("학원교습소 CSV에 좌표 추가")
    print("=" * 60)

    input_path = Path(__file__).parent / "data"
    output_path = Path(__file__).parent / "output"
    output_path.mkdir(parents=True, exist_ok=True)

    input_file = input_path / "학원교습소정보.csv"

    if input_file.exists():
        result_df = add_coordinates_to_academy_csv(
            input_csv_path=input_file,
            output_csv_path=output_path / "학원교습소정보_좌표.csv",
            delay=0.1,
        )
    else:
        print(f"입력 파일을 찾을 수 없습니다: {input_file}")
