"""
WFS Parquet 데이터를 Folium으로 시각화

사용법:
    python visualize_wfs.py                        # 전체 파일 시각화
    python visualize_wfs.py --files 시군구         # 특정 파일만
    python visualize_wfs.py --files 시군구 읍면동   # 여러 파일
    python visualize_wfs.py --list                 # 사용 가능한 파일 목록
    python visualize_wfs.py --output map.html      # 출력 파일 지정
"""

import argparse
import json
import webbrowser
from pathlib import Path
from typing import List, Optional

import folium
import geopandas as gpd
import pandas as pd

# ============================================================
# 설정
# ============================================================

# 기본 경로
WFS_DIR = Path(__file__).parent / "output" / "WFS"
OUTPUT_DIR = Path(__file__).parent / "output"

# 레이어별 스타일 (색상, 투명도)
LAYER_STYLES = {
    "시군구": {
        "color": "#000000",  # 검은색 경계
        "fill_color": "#e74c3c",
        "fill_opacity": 0.2,
        "weight": 3,  # 경계선 두께 증가
    },
    "읍면동": {
        "color": "#000000",  # 검은색 경계
        "fill_color": "#3498db",
        "fill_opacity": 0.3,
        "weight": 2.5,  # 경계선 두께 증가
    },
    "리": {
        "color": "#000000",  # 검은색 경계
        "fill_color": "#2ecc71",
        "fill_opacity": 0.4,
        "weight": 2,  # 경계선 두께 증가
    },
    "고등학교학교군": {
        "color": "#000000",  # 검은색 경계
        "fill_color": "#2ecc71",
        "fill_opacity": 0.4,
        "weight": 2.5,  # 경계선 두께 증가
    },
}

# 기본 스타일
DEFAULT_STYLE = {
    "color": "#000000",  # 검은색 경계
    "fill_color": "#9b59b6",
    "fill_opacity": 0.3,
    "weight": 2.5,  # 경계선 두께 증가
}

# 목표 좌표계 (Folium은 WGS84 사용)
TARGET_CRS = "EPSG:4326"


# ============================================================
# 데이터 로드
# ============================================================


def get_available_files() -> List[str]:
    """사용 가능한 parquet 파일 목록 반환"""
    if not WFS_DIR.exists():
        return []
    return [f.stem for f in WFS_DIR.glob("*.parquet")]


def load_parquet(file_name: str) -> Optional[gpd.GeoDataFrame]:
    """
    GeoParquet 파일 로드 및 CRS 변환

    Args:
        file_name: 파일명 (확장자 제외)

    Returns:
        GeoDataFrame (EPSG:4326) 또는 None
    """
    file_path = WFS_DIR / f"{file_name}.parquet"

    if not file_path.exists():
        print(f"  ✗ 파일 없음: {file_path}")
        return None

    try:
        # GeoParquet 파일 로드
        gdf = gpd.read_parquet(file_path)
        print(f"  ✓ {file_name}: {len(gdf)}개 피처 로드 (CRS: {gdf.crs})")
        
        # CRS 변환 (Folium은 WGS84 필요)
        if gdf.crs and gdf.crs.to_string() != TARGET_CRS:
            print(f"    → CRS 변환: {gdf.crs} → {TARGET_CRS}")
            gdf = gdf.to_crs(TARGET_CRS)
        
        return gdf
    except Exception as e:
        print(f"  ✗ 로드 실패 ({file_name}): {e}")
        return None


# ============================================================
# Folium 시각화
# ============================================================


def get_base_layer_name(file_name: str) -> str:
    """파일명에서 기본 레이어 이름 추출 (시군구_41 → 시군구)"""
    for layer in LAYER_STYLES.keys():
        if file_name.startswith(layer):
            return layer
    return file_name


def create_popup_content(properties: dict, layer_name: str) -> str:
    """팝업 HTML 생성"""
    html = f"<b>{layer_name}</b><br><hr style='margin: 5px 0;'>"

    for key, value in properties.items():
        if key != "geometry" and value is not None:
            html += f"<b>{key}:</b> {value}<br>"

    return html


def add_layer_to_map(
    m: folium.Map,
    gdf: gpd.GeoDataFrame,
    layer_name: str,
) -> int:
    """
    GeoDataFrame의 geometry를 Folium 맵에 추가

    Args:
        m: Folium Map 객체
        gdf: GeoDataFrame (EPSG:4326)
        layer_name: 레이어 이름

    Returns:
        추가된 피처 수
    """
    if not isinstance(gdf, gpd.GeoDataFrame) or gdf.geometry is None:
        print(f"    ⚠️  유효한 GeoDataFrame이 아님")
        return 0

    # 스타일 선택 (시군구_41 → 시군구 스타일 사용)
    base_name = get_base_layer_name(layer_name)
    style = LAYER_STYLES.get(base_name, DEFAULT_STYLE)

    # FeatureGroup 생성 (레이어 컨트롤용)
    feature_group = folium.FeatureGroup(name=layer_name)

    # GeoDataFrame을 GeoJSON으로 변환
    try:
        geojson_data = json.loads(gdf.to_json())
        
        # 각 피처에 스타일과 팝업 추가
        for feature in geojson_data.get("features", []):
            properties = feature.get("properties", {})
            
            # 팝업 내용 생성
            popup_html = create_popup_content(properties, layer_name)
            
            # 툴팁 텍스트 결정
            tooltip_text = (
                properties.get("sig_kor_nm")
                or properties.get("emd_kor_nm")
                or properties.get("li_kor_nm")
                or properties.get("hakgudo_nm")
                or layer_name
            )
            
            # Folium에 추가
            folium.GeoJson(
                feature,
                style_function=lambda x, s=style: {
                    "color": s["color"],
                    "fillColor": s["fill_color"],
                    "fillOpacity": s["fill_opacity"],
                    "weight": s["weight"],
                },
                popup=folium.Popup(popup_html, max_width=300),
                tooltip=tooltip_text,
            ).add_to(feature_group)
        
        count = len(geojson_data.get("features", []))
        
    except Exception as e:
        print(f"    ⚠️  레이어 추가 실패: {e}")
        return 0

    feature_group.add_to(m)
    return count


def visualize(
    file_names: List[str],
    output_file: Optional[str] = None,
    open_browser: bool = True,
) -> Optional[Path]:
    """
    Parquet 파일들을 Folium으로 시각화

    Args:
        file_names: 시각화할 파일명 리스트
        output_file: 출력 HTML 파일명
        open_browser: 브라우저 자동 열기 여부

    Returns:
        생성된 HTML 파일 경로
    """
    print("=" * 60)
    print("WFS 데이터 시각화")
    print("=" * 60)

    # 파일 로드
    print("\n[1] 데이터 로드 및 CRS 변환")
    geodataframes = {}
    for name in file_names:
        gdf = load_parquet(name)
        if gdf is not None:
            geodataframes[name] = gdf

    if not geodataframes:
        print("\n✗ 로드된 데이터 없음")
        return None

    # 지도 생성 (한국 중심)
    print("\n[2] 지도 생성")
    m = folium.Map(
        location=[37.5665, 126.9780],  # 서울 중심
        zoom_start=10,
        tiles="OpenStreetMap",
    )

    # 각 레이어 추가
    print("\n[3] 레이어 추가")
    total_features = 0
    for name, gdf in geodataframes.items():
        print(f"  {name} 처리 중...")
        count = add_layer_to_map(m, gdf, name)
        total_features += count
        print(f"    → {count}개 피처 추가됨")

    # 레이어 컨트롤 추가
    folium.LayerControl().add_to(m)

    # 파일 저장
    if output_file:
        output_path = OUTPUT_DIR / output_file
    else:
        layer_names = "_".join(file_names[:3])
        if len(file_names) > 3:
            layer_names += f"_외{len(file_names)-3}개"
        output_path = OUTPUT_DIR / f"WFS_시각화_{layer_names}.html"

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    m.save(str(output_path))

    print(f"\n[4] 저장 완료")
    print(f"  파일: {output_path}")
    print(f"  총 피처: {total_features}개")

    # 브라우저 열기
    if open_browser:
        print("\n브라우저에서 열기...")
        webbrowser.open(f"file://{output_path.absolute()}")

    print("\n" + "=" * 60)
    return output_path


# ============================================================
# 메인
# ============================================================


def main():
    parser = argparse.ArgumentParser(
        description="WFS Parquet 데이터를 Folium으로 시각화"
    )
    parser.add_argument(
        "--files",
        nargs="*",
        help="시각화할 파일명 (확장자 제외). 지정하지 않으면 전체 시각화",
    )
    parser.add_argument(
        "--output",
        type=str,
        help="출력 HTML 파일명",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="사용 가능한 파일 목록 출력",
    )
    parser.add_argument(
        "--no-browser",
        action="store_true",
        help="브라우저 자동 열기 비활성화",
    )
    args = parser.parse_args()

    # 파일 목록 출력
    if args.list:
        available = get_available_files()
        if available:
            print("사용 가능한 파일:")
            for name in available:
                base_name = get_base_layer_name(name)
                style = LAYER_STYLES.get(base_name, DEFAULT_STYLE)
                print(f"  - {name} (색상: {style['color']})")
        else:
            print("사용 가능한 파일 없음")
            print(f"경로: {WFS_DIR}")
        return

    # 시각화할 파일 결정
    if args.files:
        file_names = args.files
    else:
        file_names = get_available_files()
        if not file_names:
            print(f"✗ {WFS_DIR}에 parquet 파일이 없습니다.")
            return

    # 시각화 실행
    visualize(
        file_names=file_names,
        output_file=args.output,
        open_browser=not args.no_browser,
    )


if __name__ == "__main__":
    main()
