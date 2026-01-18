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

#### geometry 컬럼 형태
- GeoJSON 문자열로 저장 (Parquet)
- 예: `{"type": "Polygon", "coordinates": [[[x, y], ...]]}`