"""
VWorld WFS API를 사용하여 서울/경기도의 시군구/읍면동/리 데이터를 GPKG로 저장
"""
import requests
from typing import Optional, Dict, List, Tuple
import json
import geopandas as gpd
from shapely.geometry import shape
import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()

class VWorldWFSClient:
    """VWorld WFS API 클라이언트"""

    BASE_URL = "https://api.vworld.kr/req/wfs"
    
    def __init__(self):
        self.api_key = os.getenv("VWORLD_API_KEY")
    
    def get_feature_geojson(
        self,
        typename: str,
        bbox: Optional[Tuple[float, float, float, float]] = None,
        max_features: int = 1000
    ) -> Optional[Dict]:
        """
        법정구역도 좌표 정보를 GeoJSON 형식으로 요청
        
        Args:
            typename: 레이어명 (lt_c_adsigg, lt_c_ademd, lt_c_adri 등)
            bbox: 경계 박스 (minx, miny, maxx, maxy) - EPSG:5186 기준
            max_features: 최대 피처 수
            
        Returns:
            GeoJSON 형식의 딕셔너리
        """
        params = {
            'REQUEST': 'GetFeature',
            'TYPENAME': typename,
            'SRSNAME': 'EPSG:5186',  # KATEC2000 / UTM-K 좌표계 고정
            'OUTPUT': 'application/json',
        }
        
        # BBOX 필터 추가
        if bbox:
            params['BBOX'] = f'{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]},EPSG:5186'
        
        try:
            REQUEST_URL = f"{self.BASE_URL}?key={self.api_key}"
            response = requests.get(REQUEST_URL, params=params, timeout=300)
            response.raise_for_status()
            
            try:
                return response.json()
            except json.JSONDecodeError:
                print(f"JSON 파싱 실패. 응답 내용:")
                print(response.text[:500])
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"GetFeature 요청 실패: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"응답 내용: {e.response.text[:500]}")
            return None


def geojson_to_geodataframe(geojson_data: Dict) -> gpd.GeoDataFrame:
    """
    GeoJSON 데이터를 GeoDataFrame으로 변환
    
    Args:
        geojson_data: GeoJSON 형식의 딕셔너리
        
    Returns:
        GeoDataFrame
    """
    if not geojson_data or 'features' not in geojson_data:
        return gpd.GeoDataFrame()
    
    features = geojson_data['features']
    if not features:
        return gpd.GeoDataFrame()
    
    # 각 피처를 처리
    geometries = []
    properties = []
    
    for feature in features:
        if 'geometry' in feature and feature['geometry']:
            try:
                geom = shape(feature['geometry'])
                geometries.append(geom)
                properties.append(feature.get('properties', {}))
            except Exception as e:
                print(f"지오메트리 변환 실패: {e}")
                continue
    
    if not geometries:
        return gpd.GeoDataFrame()
    
    # GeoDataFrame 생성 (EPSG:5186으로 직접 생성)
    gdf = gpd.GeoDataFrame(properties, geometry=geometries, crs='EPSG:5186')
    return gdf


def fetch_and_merge_data(
    client: VWorldWFSClient,
    layer_name: str,
    layer_display_name: str,
    bbox: Optional[Tuple[float, float, float, float]] = None
) -> gpd.GeoDataFrame:
    """
    데이터를 호출하여 반환
    
    Args:
        client: VWorldWFSClient 인스턴스
        layer_name: 레이어명
        layer_display_name: 레이어 표시명
        bbox: 경계 박스 (minx, miny, maxx, maxy) - EPSG:5186 기준
        
    Returns:
        GeoDataFrame
    """
    print(f"\n[{layer_display_name}] 데이터 수집 중...")
    
    # 데이터 요청
    print(f"  - 데이터 요청 중...")
    data = client.get_feature_geojson(
        typename=layer_name,
        bbox=bbox,
        max_features=1000
    )
    
    if data:
        gdf = geojson_to_geodataframe(data)
        if not gdf.empty:
            print(f"    ✓ {len(gdf)}개 피처 수집 완료")
            return gdf
        else:
            print(f"    ✗ 데이터 없음")
            return gpd.GeoDataFrame()
    else:
        print(f"    ✗ 요청 실패")
        return gpd.GeoDataFrame()


def main():
    """메인 실행 함수"""
    print("=" * 60)
    print("VWorld API - 서울/경기도 행정구역 데이터 수집 및 GPKG 저장")
    print("=" * 60)
    
    # WFS 클라이언트 생성
    client = VWorldWFSClient()
    
    # 서울/경기도 경계 박스 (EPSG:5186 기준)
    bbox = (144693, 477383, 275745, 633107)  # (minx, miny, maxx, maxy)
    
    # 레이어 정의
    layers = {
        '시군구': 'lt_c_adsigg_info',
        '읍면동': 'lt_c_ademd_info',
        '리': 'lt_c_adri_info'
    }

    layers = {
        "초등학교학교군": "lt_c_desch",
        "중학교학교군": "lt_c_dmsch",
        "고등학교학교군": "lt_c_dhsch",
        "교육행정구역": "lt_c_eadist"
    }
    
    # 각 레이어 데이터 수집
    all_layers = {}
    
    for layer_display_name, layer_name in layers.items():
        gdf = fetch_and_merge_data(
            client=client,
            layer_name=layer_name,
            layer_display_name=layer_display_name,
            bbox=bbox
        )
        
        if not gdf.empty:
            all_layers[layer_display_name] = gdf
    
    # GPKG 파일로 저장 (각 레이어를 별도 파일로)
    if all_layers:
        print(f"\n[GPKG 저장]")
        
        # 각 레이어를 별도 파일로 저장
        for layer_name, gdf in all_layers.items():
            output_file = f'G:/내 드라이브/01_Company/data/법정구역정보(읍면동)_디지털트윈/{layer_name}.gpkg'
            print(f"  - {layer_name} 레이어 저장 중... ({len(gdf)}개 피처)")
            print(f"    파일명: {output_file}")
            gdf.to_file(output_file, driver='GPKG')
        
        print(f"\n✓ 저장 완료: {len(all_layers)}개 파일 생성됨")
        
        # 레이어별 통계 출력
        print(f"\n[저장된 파일 통계]")
        for layer_name, gdf in all_layers.items():
            print(f"  - {layer_name}.gpkg: {len(gdf)}개 피처")
    else:
        print("\n✗ 저장할 데이터가 없습니다.")


if __name__ == "__main__":
    main()
