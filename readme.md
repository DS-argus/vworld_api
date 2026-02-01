## 브이월드 주요 API 코드

### 1. 지오코딩 API (`geocoder.py`)

```bash
python geocoder.py --apt              # 공동주택 CSV
python geocoder.py --academy          # 학원교습소 CSV
python geocoder.py --apt --workers 4  # 병렬 처리
```

#### Fallback 로직

**공동주택 (`--apt`)**
| 순서 | 주소 유형 | 시도 내용 |
|------|----------|----------|
| 1 | 도로명 | 원본 시도 |
| 2 | 지번 | 원본 시도 |
| 3 | 지번 | 시군구 분리 (`수원영통구` → `수원시 영통구`) |

**학원교습소 (`--academy`)**
| 순서 | 시도 내용 |
|------|----------|
| 1 | 원본 시도 |
| 2 | `퇴계원면` → `퇴계원읍` |
| 3 | 읍/면 단어 제거 |

#### 입출력
- 입력: `data/국토교통부_공동주택_기본정보.csv`, `data/학원교습소정보.csv`
- 출력: `output/..._좌표.csv`
- 실패 주소만 콘솔 출력, tqdm 진행바 표시

---

### 2. WFS API (`WFS.py`)

#### 레이어 목록 및 필터 조건

| 레이어 | typename | 필터 |
|--------|----------|------|
| 시군구 | lt_c_adsigg_info | `sig_cd LIKE '41*'` |
| 읍면동 | lt_c_ademd_info | `emd_cd LIKE '41*'` |
| 리 | lt_c_adri_info | `li_cd LIKE '41*'` |
| 초등학교학교군 | lt_c_desch | `edu_up_cd = '7530000'` |
| 중학교학교군 | lt_c_dmsch | `edu_up_cd = '7530000'` |
| 고등학교학교군 | lt_c_dhsch | `edu_up_cd = '7530000'` |
| 교육행정구역 | lt_c_eadist | `edu_up_cd = '7530000'` |
| 교통노드 | lt_p_moctnode | `node_type = '106'` + BBOX |
| 하천망 | lt_c_wkmstrm | BBOX |

> 필터 코드: `41` = 경기도, `7530000` = 경기도교육청

#### 경기도 BBOX 범위
```
(126.5, 36.89, 127.90, 38.5)  # (minx, miny, maxx, maxy) EPSG:4326
```

#### API 제한 및 대응
- **STARTINDEX 2000개 제한** → `bbox_split` 옵션으로 BBOX 분할 조회 (4등분, 9등분)
- 분할 조회 시 feature `id`로 중복 제거

#### API 호출 파라미터
| 파라미터 | 값 |
|----------|-----|
| SERVICE | WFS |
| VERSION | 2.0.0 |
| REQUEST | GetFeature |
| SRSNAME | EPSG:5186 |
| OUTPUT | application/json |
| COUNT | 1000 (페이지당) |
| FILTER | FES 2.0 XML |

#### 데이터 저장 형식
- **GeoParquet 형식**으로 저장 (geopandas 사용)
- geometry는 shapely geometry 객체로 저장 (공간 데이터 최적화)
- CRS: EPSG:5186 (한국 좌표계)
- 출력: `output/WFS/{레이어명}.parquet`

#### LIKE 필터 사용법
- 와일드카드를 직접 입력해야 함
- 예시:
  - `"41*"`: 41로 시작하는 모든 값
  - `"*서울*"`: 서울을 포함하는 모든 값
  - `"41??"`: 41로 시작하고 뒤에 2글자가 오는 값

#### 사용법
```bash
python WFS.py                    # 전체 레이어 다운로드
python WFS.py --layer 시군구     # 특정 레이어만 다운로드
python WFS.py --layer 시군구 읍면동  # 여러 레이어 다운로드
python WFS.py --list             # 레이어 목록 출력
```

---

### 3. WFS 시각화 (`visualize_wfs.py`)

Folium을 사용하여 WFS Parquet 데이터를 지도로 시각화합니다.

#### 주요 기능
- **GeoParquet 직접 지원**: geopandas로 GeoParquet 파일을 직접 읽음
- **CRS 자동 변환**: EPSG:5186 → EPSG:4326 (Folium 호환)
- **레이어별 스타일**: 시군구, 읍면동, 리 등 레이어별 색상 및 두께 설정
- **경계선 강조**: 검은색 두꺼운 경계선으로 명확한 시각화
- **레이어 컨트롤**: 지도에서 레이어 on/off 가능

#### 사용법
```bash
python visualize_wfs.py                        # 전체 파일 시각화
python visualize_wfs.py --files 시군구         # 특정 파일만
python visualize_wfs.py --files 시군구 읍면동   # 여러 파일
python visualize_wfs.py --list                 # 사용 가능한 파일 목록
python visualize_wfs.py --output map.html      # 출력 파일 지정
python visualize_wfs.py --no-browser           # 브라우저 자동 열기 비활성화
```

#### 레이어별 스타일
| 레이어 | 경계선 색상 | 채우기 색상 | 경계선 두께 |
|--------|------------|------------|------------|
| 시군구 | 검은색 | 빨간색 | 3px |
| 읍면동 | 검은색 | 파란색 | 2.5px |
| 리 | 검은색 | 초록색 | 2px |
| 고등학교학교군 | 검은색 | 초록색 | 2.5px |
| 기본 | 검은색 | 보라색 | 2.5px |

#### 출력
- HTML 파일: `output/WFS_시각화_{레이어명}.html`
- 브라우저에서 자동으로 열림 (선택 가능)