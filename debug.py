import re
import time

from API1 import address_to_coordinate, parse_sigungu_address

addresses = [
    "서울특별시 강남구 율현동 217 강남한신휴플러스 8단지",
    "서울특별시 중구 신당동 367-15 약수역 더시티",
    "경기도 화성시 남양읍 남양리  화성남양뉴타운lh4단지",
    "경기도 파주시 당하동  파주운정별하람마을 4단지",
    "경기도 광주시 역동 169-11 경기광주역경기행복주택",
    "서울특별시 구로구 고척동 100-7 고척아이파크MD",
    "경기도 고양덕양구 지축동  위스테이지축",
    "경기도 시흥시 장곡동  시흥장현 서희스타힐스",
    "경기도 의왕시 고천동  e편한세상고천파크루체",
    "경기도 화성시 신동  동탄2 LH40단지",
    "경기도 평택시 고덕면 좌교리 2592 평택고덕LH12단지아파트",
    "경기도 파주시 목동동 385 물향기마을 LH1단지",
    "경기도 화성시 신동  화성동탄2LH행복주택38단지",
    "경기도 용인처인구 양지읍 양지리 614 용인 세영리첼 아파트",
    "경기도 여주시 교동 194 여주역 LH 3단지",
    "경기도 안성시 당왕동 121 금호어울림 더프라임아파트",
    "경기도 파주시 다율동 - 운정자이퍼스트시티 아파트",
    "경기도 양주시 덕계동  양주회천14단지주거행복지원센터",
    "경기도 양주시 회정동  양주회천 10단지",
    "경기도 양주시 장흥면 일영리 143-11 장흥역 경남아너스빌 북한산뷰5블럭",
    "경기도 포천시 어룡동 21-3 포천리버포레세영리첼",
    "경기도 포천시 어룡동 21-4 포천 모아엘가 리더스파크",
    "경기도 포천시 소흘읍 송우리 225-1 태봉공원푸르지오파크몬트",
    "경기도 포천시 소흘읍 송우리 257-5 포천송우서희스타힐스",
    "경기도 포천시 소흘읍 송우리 392-12 전원우정",
    "경기도 포천시 군내면 구읍리 655 포천금호어울림센트럴",
    "경기도 여주시 교동 673 여주역센트레빌트리니체아파트",
    "경기도 여주시 교동  여주역 우남퍼스트빌",
    "경기도 양평군 양평읍 양근리 538-1 더샵양평리버포레",
    "경기도 양평군 양평읍 양근리 24-41 양평역 한라비발디아파트",
    "경기도 양평군 양평읍 양근리 128-2 센트럴파크 써밋 아파트",
    "경기도 양평군 양평읍 공흥리 418- 양평 공흥3 휴먼빌 아틀리에 ",
    "경기도 양평군 양평읍 창대리 527-2 리버파크어반입주자대표회의",
    "경기도 양평군 양평읍 창대리 650-12 포레나양평",
]


def parse_sigungu_address_extended(address: str) -> str:
    """
    시군구가 붙어있는 경우를 파싱하여 수정 (확장 버전)
    기존 parse_sigungu_address에 추가 케이스 포함
    """
    if not address:
        return ""

    address = str(address).strip()

    # 기존 함수 먼저 적용
    address = parse_sigungu_address(address)

    # 추가 시군구 매핑 (기존에 없는 것들)
    additional_sigungu_map = {
        "고양시": ["덕양구", "일산동구", "일산서구"],
        "용인시": ["처인구", "기흥구", "수지구"],
    }

    # "경기도"로 시작하는지 확인
    if not address.startswith("경기도"):
        return address

    for si_name, gu_list in additional_sigungu_map.items():
        sorted_gu_list = sorted(gu_list, key=len, reverse=True)

        for gu_name in sorted_gu_list:
            si_name_without_si = si_name.replace("시", "")
            pattern = f"경기도 {si_name_without_si}{gu_name}"

            if pattern in address:
                replacement = f"경기도 {si_name} {gu_name}"
                address = address.replace(pattern, replacement, 1)
                return address

    return address


def extract_jibun_only(address: str) -> tuple[str, str]:
    """
    주소에서 법정동+지번만 추출하는 함수

    Returns:
        (정제된 주소, 상태 메시지)
        상태: "지번추출", "지번없음", "불완전지번"
    """
    if not address:
        return "", "빈주소"

    address = str(address).strip()

    # 1. 시군구 분리 처리
    address = parse_sigungu_address_extended(address)

    # 2. 지번이 없는 패턴 감지 (동/리 뒤에 공백 두 개 이상이 오는 경우)
    no_jibun_pattern = r"(동|리)\s{2,}"
    if re.search(no_jibun_pattern, address):
        return address, "지번없음"

    # 3. 지번이 "-"만 있는 경우
    dash_only_pattern = r"(동|리)\s+-\s+"
    if re.search(dash_only_pattern, address):
        return address, "지번없음"

    # 4. 불완전 지번 감지 (예: "418-" 숫자-로 끝나는 경우)
    incomplete_jibun_pattern = r"(동|리)\s+(\d+-)\s+"
    if re.search(incomplete_jibun_pattern, address):
        return address, "불완전지번"

    # 5. 정상적인 지번 패턴 추출
    # 패턴: (시도 시군구 읍면동/리) + 지번(숫자 또는 숫자-숫자) + (아파트명 등)
    # 동/리 뒤의 숫자(-숫자)?까지만 추출하고 나머지는 버림

    # 읍/면 + 리 패턴 (예: 고덕면 좌교리 2592)
    pattern_with_ri = r"^(.*?(?:읍|면)\s+\S+리)\s+(\d+(?:-\d+)?)\s*.*$"
    match = re.match(pattern_with_ri, address)
    if match:
        base = match.group(1).strip()
        jibun = match.group(2)
        return f"{base} {jibun}", "지번추출"

    # 동 패턴 (예: 율현동 217)
    pattern_with_dong = r"^(.*?동)\s+(\d+(?:-\d+)?)\s*.*$"
    match = re.match(pattern_with_dong, address)
    if match:
        base = match.group(1).strip()
        jibun = match.group(2)
        return f"{base} {jibun}", "지번추출"

    # 매칭 실패 시 원본 반환
    return address, "패턴불일치"


def process_addresses():
    """주소들을 처리하고 결과 출력"""
    print("=" * 80)
    print("실패 지번주소 추가 처리 테스트")
    print("=" * 80)

    success_count = 0
    fail_count = 0
    skip_count = 0

    results = []

    for i, addr in enumerate(addresses, 1):
        print(f"\n[{i}/{len(addresses)}]")
        print(f"  원본: {addr}")

        # 1. 지번주소 추출
        cleaned, status = extract_jibun_only(addr)
        print(f"  정제: {cleaned} [{status}]")

        # 지번이 없거나 불완전한 경우 스킵
        if status in ("지번없음", "불완전지번", "빈주소"):
            print(f"  결과: ⊘ 스킵 (유효한 지번 없음)")
            skip_count += 1
            results.append(
                {
                    "원본": addr,
                    "정제": cleaned,
                    "상태": status,
                    "경도": None,
                    "위도": None,
                }
            )
            continue

        # 2. 좌표 변환 시도
        result = address_to_coordinate(cleaned, addr_type="PARCEL")

        if result:
            x, y = result
            print(f"  결과: ✓ 좌표 ({x}, {y})")
            success_count += 1
            results.append(
                {"원본": addr, "정제": cleaned, "상태": "성공", "경도": x, "위도": y}
            )
        else:
            print(f"  결과: ✗ 실패")
            fail_count += 1
            results.append(
                {
                    "원본": addr,
                    "정제": cleaned,
                    "상태": "API실패",
                    "경도": None,
                    "위도": None,
                }
            )

        # API 호출 제한 방지
        time.sleep(0.1)

    # 결과 요약
    print("\n" + "=" * 80)
    print("처리 결과 요약")
    print("=" * 80)
    print(f"  총 주소: {len(addresses)}개")
    print(f"  성공: {success_count}개")
    print(f"  실패: {fail_count}개")
    print(f"  스킵(지번없음): {skip_count}개")
    print("=" * 80)

    # 상세 결과 테이블
    print("\n[상세 결과]")
    print("-" * 80)
    for r in results:
        status_icon = (
            "✓"
            if r["상태"] == "성공"
            else ("⊘" if "없음" in r["상태"] or "불완전" in r["상태"] else "✗")
        )
        coord_str = f"({r['경도']}, {r['위도']})" if r["경도"] else "-"
        print(f"{status_icon} [{r['상태']:^8}] {r['정제'][:40]:<40} → {coord_str}")


if __name__ == "__main__":
    process_addresses()
