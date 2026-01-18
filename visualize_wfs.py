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
import pandas as pd
from pyproj import Transformer

# ============================================================
# 설정
# ============================================================

# 기본 경로
WFS_DIR = Path(__file__).parent / "output" / "WFS"
OUTPUT_DIR = Path(__file__).parent / "output"

# 레이어별 스타일 (색상, 투명도)
LAYER_STYLES = {
    "시군구": {
        "color": "#e74c3c",
        "fill_color": "#e74c3c",
        "fill_opacity": 0.2,
        "weight": 2,
    },
    "읍면동": {
        "color": "#3498db",
        "fill_color": "#3498db",
        "fill_opacity": 0.3,
        "weight": 1.5,
    },
    "리": {
        "color": "#2ecc71",
        "fill_color": "#2ecc71",
        "fill_opacity": 0.4,
        "weight": 1,
    },
}

# 기본 스타일
DEFAULT_STYLE = {
    "color": "#9b59b6",
    "fill_color": "#9b59b6",
    "fill_opacity": 0.3,
    "weight": 1,
}

# 원본 좌표계 (WFS.py에서 EPSG:5186 사용)
SOURCE_CRS = "EPSG:5186"
TARGET_CRS = "EPSG:4326"


# ============================================================
# 좌표 변환
# ============================================================


def get_transformer():
    """좌표 변환기 생성"""
    return Transformer.from_crs(SOURCE_CRS, TARGET_CRS, always_xy=True)


def transform_coordinates(geometry: dict, transformer: Transformer) -> dict:
    """
    GeoJSON geometry 좌표를 EPSG:5186에서 EPSG:4326으로 변환

    Args:
        geometry: GeoJSON geometry 딕셔너리
        transformer: pyproj Transformer 객체

    Returns:
        변환된 GeoJSON geometry
    """
    geom_type = geometry.get("type")
    coords = geometry.get("coordinates")

    if not coords:
        return geometry

    def transform_point(point):
        """단일 좌표 변환"""
        lon, lat = transformer.transform(point[0], point[1])
        return [lon, lat]

    def transform_ring(ring):
        """링(폴리곤의 외곽선) 변환"""
        return [transform_point(p) for p in ring]

    def transform_polygon(polygon):
        """폴리곤 변환 (외곽선 + 홀)"""
        return [transform_ring(ring) for ring in polygon]

    if geom_type == "Point":
        new_coords = transform_point(coords)
    elif geom_type == "LineString":
        new_coords = transform_ring(coords)
    elif geom_type == "Polygon":
        new_coords = transform_polygon(coords)
    elif geom_type == "MultiPoint":
        new_coords = [transform_point(p) for p in coords]
    elif geom_type == "MultiLineString":
        new_coords = [transform_ring(line) for line in coords]
    elif geom_type == "MultiPolygon":
        new_coords = [transform_polygon(poly) for poly in coords]
    else:
        return geometry

    return {"type": geom_type, "coordinates": new_coords}


# ============================================================
# 데이터 로드
# ============================================================


def get_available_files() -> List[str]:
    """사용 가능한 parquet 파일 목록 반환"""
    if not WFS_DIR.exists():
        return []
    return [f.stem for f in WFS_DIR.glob("*.parquet")]


def load_parquet(file_name: str) -> Optional[pd.DataFrame]:
    """
    Parquet 파일 로드

    Args:
        file_name: 파일명 (확장자 제외)

    Returns:
        DataFrame 또는 None
    """
    file_path = WFS_DIR / f"{file_name}.parquet"

    if not file_path.exists():
        print(f"  ✗ 파일 없음: {file_path}")
        return None

    try:
        df = pd.read_parquet(file_path)
        print(f"  ✓ {file_name}: {len(df)}개 피처 로드")
        return df
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
    df: pd.DataFrame,
    layer_name: str,
    transformer: Transformer,
) -> int:
    """
    DataFrame의 geometry를 Folium 맵에 추가

    Args:
        m: Folium Map 객체
        df: DataFrame (geometry 컬럼 포함)
        layer_name: 레이어 이름
        transformer: 좌표 변환기

    Returns:
        추가된 피처 수
    """
    if "geometry" not in df.columns:
        print(f"    ⚠️  geometry 컬럼 없음")
        return 0

    # 스타일 선택 (시군구_41 → 시군구 스타일 사용)
    base_name = get_base_layer_name(layer_name)
    style = LAYER_STYLES.get(base_name, DEFAULT_STYLE)

    # FeatureGroup 생성 (레이어 컨트롤용)
    feature_group = folium.FeatureGroup(name=layer_name)

    count = 0
    for _, row in df.iterrows():
        geom_str = row.get("geometry")
        if not geom_str or pd.isna(geom_str):
            continue

        try:
            # JSON 문자열을 딕셔너리로 변환
            geometry = json.loads(geom_str)

            # 좌표 변환 (EPSG:5186 → EPSG:4326)
            geometry = transform_coordinates(geometry, transformer)

            # 속성 추출 (geometry 제외)
            properties = {k: v for k, v in row.items() if k != "geometry"}

            # GeoJSON Feature 생성
            feature = {
                "type": "Feature",
                "geometry": geometry,
                "properties": properties,
            }

            # 팝업 내용 생성
            popup_html = create_popup_content(properties, layer_name)

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
                tooltip=properties.get("sig_kor_nm")
                or properties.get("emd_kor_nm")
                or properties.get("li_kor_nm")
                or layer_name,
            ).add_to(feature_group)

            count += 1

        except json.JSONDecodeError:
            continue
        except Exception as e:
            print(f"    ⚠️  피처 추가 실패: {e}")
            continue

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
    print("\n[1] 데이터 로드")
    dataframes = {}
    for name in file_names:
        df = load_parquet(name)
        if df is not None:
            dataframes[name] = df

    if not dataframes:
        print("\n✗ 로드된 데이터 없음")
        return None

    # 좌표 변환기 생성
    print("\n[2] 좌표 변환기 초기화")
    print(f"  {SOURCE_CRS} → {TARGET_CRS}")
    transformer = get_transformer()

    # 지도 생성 (한국 중심)
    print("\n[3] 지도 생성")
    m = folium.Map(
        location=[37.5665, 126.9780],  # 서울 중심
        zoom_start=10,
        tiles="OpenStreetMap",
    )

    # 각 레이어 추가
    print("\n[4] 레이어 추가")
    total_features = 0
    for name, df in dataframes.items():
        print(f"  {name} 처리 중...")
        count = add_layer_to_map(m, df, name, transformer)
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

    print(f"\n[5] 저장 완료")
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
