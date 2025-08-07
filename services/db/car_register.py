import os
from pathlib import Path
import sqlite3
import pandas as pd
from . import query

# 프로젝트 경로 지정
PROJECT_PATH = Path(__file__).parent.parent.parent

# 경로 지정
DB_PATH = PROJECT_PATH / "db"
DATA_PATH = PROJECT_PATH / "data"

# 테이블 생성
def create_tables():
    """
    SQLite3 데이터베이스 생성 및 테이블 생성
    """
    conn = sqlite3.connect(DB_PATH / "car_register.db")
    cursor = conn.cursor()

    cursor.execute(query.CREATE_CAR_REGISTER)

    conn.commit()
    conn.close()

# 데이터 삽입
def insert_data(df: pd.DataFrame):
    """
    데이터프레임을 데이터베이스에 삽입하는 함수
    작동 순서
    1. DB 연결
    2. DB register_index 최대값 조회 없으면 0부터 시작
    3. 최대값에서부터 데이터프레임 행 수만큼 반복
    4. 각 반복마다 데이터베이스에 행 삽입
    5. 데이터베이스 커밋
    6. 데이터베이스 연결 해제
    Args:
        df: 데이터프레임
    Returns:
        None
    """
    conn = sqlite3.connect(DB_PATH / "car_register.db")
    cursor = conn.cursor()
    
    max_index = cursor.execute(query.SELECT_MAX_INDEX).fetchone()[0]
    if max_index is None:
        max_index = 0
    
    for idx in range(max_index, max_index + len(df)):
        cursor.execute(query.INSERT_CAR_REGISTER, (idx, df.loc[idx, 'sido'], df.loc[idx, 'sigungu'], df.loc[idx, 'car_type'], df.loc[idx, 'car_purpose'], df.loc[idx, 'year'], df.loc[idx, 'month'], df.loc[idx, 'value']))
    
    conn.commit()
    conn.close()

# ------------------------------------------------------------
# ERD 수정으로 인해 하위 코드 변경
# ------------------------------------------------------------
# 초기 테이블 생성
# def create_tables():
#     """
#     SQLite3 데이터베이스 생성 및 테이블 생성
#     """
#     conn = sqlite3.connect(DB_PATH / "car_register.db")
#     cursor = conn.cursor()

#     cursor.execute(query.CREATE_CAR_REGISTER_META)

#     # 자동차 등록 현황 테이블 생성
#     cursor.execute(query.CREATE_CAR_REGISTER_DATA)

#     conn.commit()
#     conn.close()

# # csv 데이터 삽입
# def insert_data():
#     """
#     csv 데이터를 데이터베이스에 삽입하는 함수
#     """
#     # DB 유효성 검증 및 생성
#     if not os.path.exists(DB_PATH / "car_register.db"):
#         print("DB 파일이 존재하지 않습니다. 테이블 생성 중...")
#         create_tables()

#     # 데이터 유효성 검증
#     if not os.path.exists(DATA_PATH / "meta_data.csv"):
#         print("meta_data.csv 파일이 존재하지 않습니다.")
#         print(f"경로: {DATA_PATH / 'meta_data.csv'}")
#         return 0
#     else:
#         meta_data = pd.read_csv(DATA_PATH / "meta_data.csv")

#     if not os.path.exists(DATA_PATH / "monthly_data.csv"):
#         print("monthly_data.csv 파일이 존재하지 않습니다.")
#         print(f"경로: {DATA_PATH / 'monthly_data.csv'}")
#         return 0
#     else:
#         monthly_data = pd.read_csv(DATA_PATH / "monthly_data.csv")

#     # DB 연결
#     print("DB 연결 중...")
#     conn = sqlite3.connect(DB_PATH / "car_register.db")
#     cursor = conn.cursor()

#     # 데이터 삽입
#     print("데이터 삽입 중...")
#     for index, row in meta_data.iterrows():
#         cursor.execute(query.INSERT_CAR_REGISTER_META, (index, row['sido'], row['sigungu'], row['car_type'], row['car_purpose']))

#     for index, row in monthly_data.iterrows():
#         cursor.execute(query.INSERT_CAR_REGISTER_DATA, (index, row['register_index'], row['year'], row['month'], row['value']))

#     conn.commit()
#     conn.close()

#     print("데이터 삽입 완료")

# def insert_monthly_data():
#     """
#     csv 데이터를 데이터베이스에 삽입하는 함수
#     """
#     # DB 유효성 검증 및 생성
#     if not os.path.exists(DB_PATH / "car_register.db"):
#         print("DB 파일이 존재하지 않습니다. 테이블 생성 중...")
#         create_tables()
    
#     if not os.path.exists(DATA_PATH / "monthly_data.csv"):
#         print("monthly_data.csv 파일이 존재하지 않습니다.")
#         print(f"경로: {DATA_PATH / 'monthly_data.csv'}")
#         return 0
#     else:
#         data = pd.read_csv(DATA_PATH / "monthly_data.csv")

#     # DB 연결
#     print("DB 연결 중...")
#     conn = sqlite3.connect(DB_PATH / "car_register.db")
#     cursor = conn.cursor()

#     # 데이터 삽입
#     print("데이터 삽입 중...")
#     for index, row in data.iterrows():
#         cursor.execute(query.INSERT_CAR_REGISTER_DATA, row)

#     conn.commit()
#     conn.close()

#     print("데이터 삽입 완료")