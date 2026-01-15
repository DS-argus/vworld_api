from typing import Any, Dict, Optional, Tuple, List
from pathlib import Path
import pandas as pd
import time
import logging
from datetime import datetime

import requests

# VWorld API 서비스 키
SERVICE_KEY = "5A7905BA-3D09-3F70-B7DC-A01DE4C7BE2F"

# API 기본 URL
BASE_URL = "https://api.vworld.kr/req/address"


def coordinate_to_address(
    x: float, y: float, output_format: str = "json"
) -> Optional[str]:
    """
    좌표를 주소로 변환하는 함수 (Reverse Geocoding)

    Args:
        x: 경도 (Longitude)
        y: 위도 (Latitude)
        output_format: 출력 형식 ('json' 또는 'xml'), 기본값: 'json'

    Returns:
        변환된 주소 문자열 (도로명주소). 실패 시 None 반환
    """
    params = {
        "service": "address",
        "request": "getAddress",
        "version": "2.0",
        "crs": "epsg:4326",
        "point": f"{x},{y}",
        "format": output_format,
        "type": "both",  # both: 도로명+지번, road: 도로명만, parcel: 지번만
        "key": SERVICE_KEY,
    }

    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()

        data = response.json()

        if data.get("response", {}).get("status") == "OK":
            result = data["response"]["result"][0]
            # 도로명주소를 우선 반환, 없으면 지번주소 반환
            return result.get("text", result.get("zipcode", None))
        else:
            error_message = (
                data.get("response", {}).get("error", {}).get("text", "알 수 없는 오류")
            )
            print(f"주소 변환 실패: {error_message}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"API 요청 오류: {e}")
        return None
    except (KeyError, IndexError) as e:
        print(f"응답 파싱 오류: {e}")
        return None


def address_to_coordinate(
    address: str,
    addr_type: str = "ROAD",
    crs: str = "EPSG:4326",
    output_format: str = "json",
) -> Optional[Tuple[float, float]]:
    """
    주소를 좌표로 변환하는 함수 (Forward Geocoding)

    Args:
        address: 변환할 주소 문자열
        addr_type: 주소 타입 ('ROAD': 도로명주소, 'PARCEL': 지번주소), 기본값: 'ROAD'
        crs: 좌표계 (기본값: 'EPSG:4326' WGS84)
        output_format: 출력 형식 ('json' 또는 'xml'), 기본값: 'json'

    Returns:
        (경도, 위도) 튜플. 실패 시 None 반환

    Note:
        VWorld API 응답 결과는 실시간 사용만 허용되며, 별도 저장장치/DB에 저장 불가
    """
    # 파라미터 정규화
    fmt = output_format.strip().lower()
    t = addr_type.strip().upper()
    crs_norm = crs.strip().upper()

    # 유효성 검사
    if fmt not in ("json", "xml"):
        print(f"출력 형식 오류: {output_format} (json 또는 xml 허용)")
        return None

    if t not in ("ROAD", "PARCEL"):
        print(f"type 파라미터 오류: {addr_type} (ROAD 또는 PARCEL 허용)")
        return None

    # 참고 코드 형식에 맞춰 파라미터 설정
    params = {
        "service": "address",
        "request": "getcoord",  # 소문자로 변경
        "crs": crs_norm.lower(),  # epsg:4326 형식
        "address": address,
        "format": fmt,
        "type": t.lower(),  # 소문자로 변환 (road, parcel)
        "key": SERVICE_KEY,
    }

    try:
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()

        if fmt == "json":
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
                    return (x, y)
                else:
                    print("좌표 변환 실패: point 정보가 없습니다.")
                    return None
            else:
                # 에러 구조 확인
                # breakpoint()
                error_info = response_obj.get("error", {})
                error_text = error_info.get("text", status or "알 수 없는 오류")
                error_code = error_info.get("code", "")
                print(f"좌표 변환 실패 [{error_code}]: {error_text}")
                return None

        else:
            # XML 응답 파싱
            import xml.etree.ElementTree as ET

            text = response.text
            try:
                root = ET.fromstring(text)
                # status 추출
                status_elem = root.find(".//status")
                status = status_elem.text if status_elem is not None else None

                if status == "OK":
                    x_elem = root.find(".//result/point/x")
                    y_elem = root.find(".//result/point/y")
                    if x_elem is None or y_elem is None:
                        print("XML 응답에서 좌표를 찾지 못했습니다.")
                        return None
                    x = float(x_elem.text)
                    y = float(y_elem.text)
                    return (x, y)
                else:
                    # 에러 정보 추출 시도
                    code_elem = root.find(".//error/code")
                    text_elem = root.find(".//error/text")
                    error_code = code_elem.text if code_elem is not None else ""
                    error_text = (
                        text_elem.text
                        if text_elem is not None
                        else status or "UNKNOWN_ERROR"
                    )
                    print(f"좌표 변환 실패 [{error_code}]: {error_text}")
                    return None
            except ET.ParseError as pe:
                print(f"XML 파싱 오류: {pe}")
                return None

    except requests.exceptions.RequestException as e:
        print(f"API 요청 오류: {e}")
        return None
    except (KeyError, ValueError, TypeError) as e:
        print(f"응답 파싱 오류: {e}")
        return None


def get_address_details(x: float, y: float) -> Optional[Dict[str, Any]]:
    """
    좌표의 상세 주소 정보를 반환하는 함수

    Args:
        x: 경도 (Longitude)
        y: 위도 (Latitude)

    Returns:
        주소 상세 정보 딕셔너리. 실패 시 None 반환
    """
    params = {
        "service": "address",
        "request": "getAddress",
        "version": "2.0",
        "crs": "epsg:4326",
        "point": f"{x},{y}",
        "format": "json",
        "type": "both",
        "key": SERVICE_KEY,
    }

    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()

        data = response.json()

        if data.get("response", {}).get("status") == "OK":
            return data["response"]["result"][0]
        else:
            error_message = (
                data.get("response", {}).get("error", {}).get("text", "알 수 없는 오류")
            )
            print(f"주소 정보 조회 실패: {error_message}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"API 요청 오류: {e}")
        return None
    except (KeyError, IndexError) as e:
        print(f"응답 파싱 오류: {e}")
        return None


def clean_address(address: str) -> str:
    """
    주소의 앞뒤 공백을 제거하는 함수 (단순화)
    
    Args:
        address: 원본 주소 문자열
    
    Returns:
        앞뒤 공백이 제거된 주소 문자열
    """
    if not address or pd.isna(address):
        return ""
    
    return str(address).strip()


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
    limit: Optional[int] = None,
    delay: float = 0.1,
    subset_ids: Optional[List[str]] = None,
    save_interval: int = 100,  # 사용하지 않음 (호환성을 위해 유지)
) -> pd.DataFrame:
    """
    공동주택 CSV 파일의 도로명주소(doroJuso)를 좌표로 변환하여 새로운 CSV 생성
    
    Args:
        input_csv_path: 입력 CSV 파일 경로 (apt_detail_info_seoul_gyeonggi.csv)
        output_csv_path: 출력 CSV 파일 경로 (기본 경로, 진행도가 파일명에 추가됨)
        limit: 처리할 행 수 제한 (None이면 전체, 테스트용으로 20 등 사용)
        delay: API 호출 간 딜레이(초)
        save_interval: 사용하지 않음 (호환성을 위해 유지)
    
    Returns:
        좌표가 추가된 DataFrame (kaptCode, kaptName, doroJuso, kaptAddr, 경도, 위도, 비고)
    
    Note:
        모든 결과는 메모리에 저장되며, 정상 완료 시 또는 에러/중단 시에만 파일로 저장됩니다.
    """
    input_path = Path(input_csv_path)
    base_output_path = Path(output_csv_path)
    
    print(f"CSV 파일 읽는 중: {input_path}")
    df = pd.read_csv(input_path, encoding="utf-8-sig")
    df = df[df["kaptCode"].isin(subset_ids)]
    # limit이 지정된 경우 일부만 처리
    if limit:
        df = df.head(limit)
        print(f"테스트 모드: 처음 {limit}개 행만 처리합니다.")
    
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
            
            # 1차 시도: 도로명주소 (doroJuso)
            if not pd.isna(doro_juso) and str(doro_juso).strip():
                cleaned_doro = clean_address(str(doro_juso))
                print(f"[{idx}/{total_count}] {kapt_name} ({kapt_code}) - 도로명주소: {cleaned_doro} ... ", end="")
                coordinates = address_to_coordinate(cleaned_doro, addr_type="ROAD")
                
                if coordinates:
                    x, y = coordinates
                    note = "도로명(1)"
                    print(f"✓ 좌표: ({x}, {y})")
                else:
                    print("✗ 실패", end="")
            
            # 2차 시도: 지번주소 (kaptAddr) - 도로명주소가 없거나 실패한 경우
            if coordinates is None:
                if not pd.isna(kapt_addr) and str(kapt_addr).strip():
                    cleaned_parcel = clean_address(str(kapt_addr))
                    if not pd.isna(doro_juso) and str(doro_juso).strip():
                        print(f" -> 지번주소: {cleaned_parcel} ... ", end="")
                    else:
                        print(f"[{idx}/{total_count}] {kapt_name} ({kapt_code}) - 지번주소: {cleaned_parcel} ... ", end="")
                    coordinates = address_to_coordinate(cleaned_parcel, addr_type="PARCEL")
                    
                    if coordinates:
                        x, y = coordinates
                        note = "지번(2)"
                        print(f"✓ 좌표: ({x}, {y})")
                    else:
                        print("✗ 실패", end="")
                        
                        # 3차 시도: 지번주소 파싱 후 재시도
                        parsed_parcel = parse_sigungu_address(cleaned_parcel)
                        if parsed_parcel != cleaned_parcel:
                            print(f" -> 파싱된 지번주소: {parsed_parcel} ... ", end="")
                            coordinates = address_to_coordinate(parsed_parcel, addr_type="PARCEL")
                            
                            if coordinates:
                                x, y = coordinates
                                note = "시군구분리(3)"
                                print(f"✓ 좌표: ({x}, {y})")
                            else:
                                print("✗ 실패")
                        else:
                            print()
                else:
                    if pd.isna(doro_juso) or not str(doro_juso).strip():
                        print(f"[{idx}/{total_count}] {kapt_name} ({kapt_code}): 주소 없음")
                    else:
                        print()
            
            # 결과 저장 (메모리에만 저장)
            results.append({
                "kaptCode": kapt_code,
                "kaptName": kapt_name,
                "doroJuso": doro_juso,
                "kaptAddr": kapt_addr,
                "경도": x,
                "위도": y,
                "비고": note,
            })
            
            # API 호출 제한 방지
            time.sleep(delay)
        
        # 전체 완료 시 최종 저장
        result_df = pd.DataFrame(results)
        final_output_path = base_output_path.parent / f"{base_output_path.stem}_({len(results)}_{total_count}){base_output_path.suffix}"
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
            interrupted_output_path = base_output_path.parent / f"{base_output_path.stem}_({len(results)}_{total_count})_interrupted{base_output_path.suffix}"
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
            error_output_path = base_output_path.parent / f"{base_output_path.stem}_({len(results)}_{total_count})_error{base_output_path.suffix}"
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
    
    
    input_csv = Path(__file__).parent / "output" / "apt_detail_info_seoul_gyeonggi.csv"
    output_csv = Path(__file__).parent / "output" / "apt_coordinates.csv"
    
    subset_ids = [
        "A10027909",
        "A10023488",
        "A10027469",
        "A10023985",
        "A10023983",
        "A10023604",
        "A10023938",
        "A10023847",
        "A10023041",
        "A10023757",
        "A10023374",
        "A10023371",
        "A10023273",
        "A10023276",
        "A10023246",
        "A10022806",
        "A10022347",
        "A10023328",
        "A10022665",
        "A10020748",
        "A10022750",
        "A10023600",
        "A10021006",
        "A10024452",
        "A48782308",
        "A10022875",
        "A10022724",
        "A10023274",
        "A10022783",
        "A10022983",
        "A10023918",
        "A10020813",
        "A10023310",
        "A10023413",
    ]

    if input_csv.exists():
        result_df = add_coordinates_to_apt_csv(
            input_csv_path=input_csv,
            output_csv_path=output_csv,
            limit=None,  # 전체 처리
            delay=0.1,
            subset_ids=subset_ids,
        )
    else:
        print(f"입력 파일을 찾을 수 없습니다: {input_csv}")
