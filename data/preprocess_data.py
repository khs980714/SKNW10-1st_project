import os
import pandas as pd
import json
from itertools import product
from tqdm.auto import tqdm

from pathlib import Path

# 프로젝트 경로
BASE_PATH = Path(__file__).parent.parent

# 데이터 경로
FOLDER_PATH = BASE_PATH / "data" / "register_car"
EXCEL_LIST = os.listdir(FOLDER_PATH)
SAVE_PATH = BASE_PATH / "data"


def make_dataframe_with_data(file_name: str) -> pd.DataFrame:
    """
    데이터 파일을 읽어서 데이터프레임으로 변환하는 코드
    Args:
        file_name: 데이터 파일 경로
    Returns:
        df: 데이터프레임
    """
    columns = ["sido", "sigungu", "car_type", "car_purpose", "year", "month", "value"]
    data_dict = {
        k: [] for k in columns
    }

    DATA_PATH = FOLDER_PATH / file_name
    df = pd.read_excel(DATA_PATH)

    year_month = file_name.split("시도별 ")[1].split(".")[0]
    year, month = year_month[:4], year_month[4:]

    sido_list = list(df.iloc[:, 1])[5:]
    sigungu_list = list(df.iloc[:, 2])[5:]
    sido_sigungu = list(zip(sido_list, sigungu_list))
    car_type_list = list(df.iloc[3, :].unique())[3:]
    car_purpose_list = list(df.iloc[4, :].unique())[3:]

    for sido_sigungu, car_type, car_purpose in tqdm(product(sido_sigungu, car_type_list, car_purpose_list), total=len(sido_sigungu)*len(car_type_list)*len(car_purpose_list)):
        filtered_index = df[(df.iloc[:, 1] == sido_sigungu[0]) & (df.iloc[:, 2] == sido_sigungu[1])].index
        value = df.loc[filtered_index, (df.iloc[3, :] == car_type) & (df.iloc[4, :] == car_purpose)].values[0][0]
        data_dict["sido"].append(sido_sigungu[0])
        data_dict["sigungu"].append(sido_sigungu[1])
        data_dict["car_type"].append(car_type)
        data_dict["car_purpose"].append(car_purpose)
        data_dict["year"].append(year)
        data_dict["month"].append(month)
        data_dict["value"].append(value)
    
    df = pd.DataFrame(data_dict)

    return df

def make_csv_with_dataframe(folder_path: str) -> pd.DataFrame:
    """
    다운로드 된 데이터를 저장한 data/register_car 폴더 내에 있는 모든 파일에 대해
    make_dataframe_with_data 함수를 실행하여 하나의 csv 파일로 저장 및 데이터프레임으로 반환하는 코드
    로직 순서:
    1. 월별 데이터 존재 여부 확인 및 연월 리스트 생성
    2. 폴더 내의 모든 파일에 대해 연월 확인 및 존재하지 않는 연월에 대해 make_dataframe_with_data 함수 실행
    3. 월별 데이터 저장
    Args:
        folder_path: 데이터 폴더 경로
    Returns:
        df: 데이터프레임
    """
    # 월별 데이터 존재 여부 확인 및 연월 리스트 생성
    print("월별 데이터 존재 확인")
    if os.path.exists(f"{SAVE_PATH}/monthly_data.csv"):
        base_df = pd.read_csv(f"{SAVE_PATH}/monthly_data.csv")
        date_list = [date for date in base_df["year"]+base_df["month"]]
        print("월별 데이터 존재")
    else:
        base_df = pd.DataFrame()
        date_list = []
        print("월별 데이터 존재하지 않음")

    # 폴더 내의 모든 파일에 대해 연월 확인 및 존재하지 않는 연월에 대해 make_dataframe_with_data 함수 실행
    print("폴더 내의 연도 월 확인")
    for file_name in tqdm(os.listdir(folder_path), total=len(os.listdir(folder_path))):
        file_date = file_name.split("시도별 ")[1].split(".xlsx")[0]
        if file_date not in date_list:
            df = make_dataframe_with_data(file_name)
            base_df = pd.concat([base_df, df])

    # 월별 데이터 저장
    base_df.to_csv(f"{SAVE_PATH}/monthly_data.csv", index=False, encoding="utf-8")
    print("데이터 저장 완료")

    return base_df

# ------------------------------------------------------------
# ERD 다이어그램 변경으로 인해 하위 코드 변경
# ------------------------------------------------------------

# def extract_meta(data_path: str) -> dict:
#     """
#     시도, 시군구, 차종, 용도를 추출하여 json 파일로 저장 및 dictionary 형태로 반환하는 코드
#     Args:
#         data_path: 데이터 파일 경로
#     Returns:
#         meta_data: dict(sido, sigungu, car_type, car_purpose)
#     """
#     df = pd.read_excel(data_path)
#     meta_data = {
#         "sido": list(df.iloc[:, 1])[5:],
#         "sigungu": list(df.iloc[:, 2])[5:],
#         "car_type": list(df.iloc[3, :].unique())[3:],
#         "car_purpose": list(df.iloc[4, :].unique())[3:],
#     }

#     with open(f"{SAVE_PATH}/meta_data.json", "w", encoding="utf-8") as f:
#         json.dump(meta_data, f, ensure_ascii=False)
    
#     return meta_data

# def make_csv_with_meta(meta_path: str) -> pd.DataFrame:
#     """
#     JSON 형태의 메타 데이터를 csv 파일로 저장하고 데이터프레임으로 변환하는 코드
#     Args:
#         meta_path: JSON 형태의 메타 데이터 파일 경로
#     Returns:
#         df: 메타 데이터를 데이터프레임으로 변환한 데이터
#     """
#     with open(meta_path, "r", encoding="utf-8") as f:
#         meta_data = json.load(f)

#     data_list = []
#     for sido, sigungu in zip(meta_data["sido"], meta_data["sigungu"]):
#         for car_type in meta_data["car_type"]:
#             for car_purpose in meta_data["car_purpose"]:
#                 data_list.append({"sido": sido, "sigungu": sigungu, "car_type": car_type, "car_purpose": car_purpose})
    
#     df = pd.DataFrame(data_list)

#     df.to_csv(f"{SAVE_PATH}/meta_data.csv", index=False, encoding="utf-8")

#     return df


# def make_csv_with_value(data_path: str) -> pd.DataFrame:
#     """
#     메타 데이터에 맞는 수치 데이터를 추출하여 csv로 저장하고 데이터프레임으로 반환하는 코드
#     Args:
#         data_path: 데이터 파일 경로(파일 이름에 연도월 포함)
#     Returns:
#         df: 월별 데이터를 데이터프레임으로 변환한 데이터
#     """
#     # 메타 데이터 및 데이터 파일 불러오기
#     original_df = pd.read_excel(data_path)
#     meta_data = pd.read_csv(f"{SAVE_PATH}/meta_data.csv")
#     data_dict = {
#         "sido": [],
#         "sigungu": [],
#         "car_type": [],
#         "car_purpose": [],
#         "year": [],
#         "month": [],
#         "register_count": [],
#     }

#     # 데이터 파일 이름에서 연도월 추출
#     year_month = data_path.split("시도별 ")[1].split(".xlsx")[0]
#     year, month = year_month[:4], year_month[4:]

#     # 메타 데이터 순회하여 original_df에서 해당하는 값 추출
#     for index, row in meta_data.iterrows():
#         sido = row["sido"]
#         sigungu = row["sigungu"]
#         car_type = row["car_type"]
#         car_purpose = row["car_purpose"]

#         data_dict["sido"].append(sido)
#         data_dict["sigungu"].append(sigungu)
#         data_dict["car_type"].append(car_type)
#         data_dict["car_purpose"].append(car_purpose)
#         data_dict["year"].append(year)
#         data_dict["month"].append(month)
#         filtered_index = original_df[(original_df.iloc[:, 1] == sido) & (original_df.iloc[:, 2] == sigungu)].index
#         value = original_df.loc[filtered_index, (original_df.iloc[3, :] == car_type) & (original_df.iloc[4, :] == car_purpose)].values[0][0]

#         data_dict["register_count"].append(value)

#     df = pd.DataFrame(data_dict)

#     return df


# def value_data_with_folder(folder_path: str) -> pd.DataFrame:
#     """
#     다운로드 된 데이터를 저장한 data/register_car 폴더 내에 있는 모든 파일에 대해
#     make_csv_with_value 함수를 실행하여 하나의 csv 파일로 저장 및 데이터프레임으로 반환하는 코드
#     Args:
#         folder_path: 데이터 폴더 경로
#     Returns:
#         df: 데이터프레임
#     """
#     # 월별 데이터 존재 여부 확인
#     print("월별 데이터 존재 확인")
#     if os.path.exists(f"{SAVE_PATH}/monthly_data.csv"):
#         base_df = pd.read_csv(f"{SAVE_PATH}/monthly_data.csv")
#         print("월별 데이터 존재")
#     else:
#         base_df = pd.DataFrame()
#         print("월별 데이터 존재하지 않음")

#     date_list = []

#     # 월별 데이터 내의 연도 월 확인
#     print("데이터 내의 연도 월 확인")
#     for date_iter in tqdm(base_df.iterrows(), total=len(base_df)):
#         year, month = date_iter["year"], date_iter["month"]
#         csv_date = f"{year}{month}"
#         if csv_date not in date_list:
#             date_list.append(csv_date)

#     # 연도 월 데이터를 확인하여 data/register_car 폴더 내에 존재하는 파일 중 존재하지 않는 파일 추출
#     print("값 입력 시작")
#     for file_name in tqdm(os.listdir(folder_path), total=len(os.listdir(folder_path))):
#         file_date = file_name.split("시도별 ")[1].split(".xlsx")[0]
#         if file_date not in date_list:
#             df = make_csv_with_value(f"{folder_path}/{file_name}")
#             base_df = pd.concat([base_df, df])
#     print("값 입력 완료")

#     base_df.to_csv(f"{SAVE_PATH}/monthly_data.csv", index=False, encoding="utf-8")
#     print("데이터 저장 완료")

#     return base_df