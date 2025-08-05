import os
import pandas as pd
import json
from pathlib import Path

# 프로젝트 경로
BASE_PATH = Path(__file__).parent.parent

# 데이터 경로
FOLDER_PATH = BASE_PATH / "data" / "register_car"
EXCEL_LIST = os.listdir(FOLDER_PATH)
META_PATH = BASE_PATH / "data" / "meta_data.json"


def extract_meta(data_path: str) -> dict:
    """
    시도, 시군구, 차종, 용도를 추출하여 json 파일로 저장 및 dictionary 형태로 반환하는 코드
    Args:
        data_path: 데이터 파일 경로
    Returns:
        meta_data: dict(sido, sigungu, car_type, car_purpose)
    """
    df = pd.read_excel(data_path)
    meta_data = {
        "sido": list(df.iloc[:, 1])[5:],
        "sigungu": list(df.iloc[:, 2])[5:],
        "car_type": list(df.iloc[3, :].unique())[3:],
        "car_purpose": list(df.iloc[4, :].unique())[3:],
    }

    with open(f"meta_data.json", "w", encoding="utf-8") as f:
        json.dump(meta_data, f, ensure_ascii=False)
    
    return meta_data


def make_csv_with_meta(meta_path: str) -> pd.DataFrame:
    """
    JSON 형태의 메타 데이터를 csv 파일로 저장하고 데이터프레임으로 변환하는 코드
    Args:
        meta_path: JSON 형태의 메타 데이터 파일 경로
    Returns:
        df: 메타 데이터를 데이터프레임으로 변환한 데이터
    """
    with open(meta_path, "r", encoding="utf-8") as f:
        meta_data = json.load(f)

    data_list = []
    for sido, sigungu in zip(meta_data["sido"], meta_data["sigungu"]):
        for car_type in meta_data["car_type"]:
            for car_purpose in meta_data["car_purpose"]:
                data_list.append({"sido": sido, "sigungu": sigungu, "car_type": car_type, "car_purpose": car_purpose})
    
    df = pd.DataFrame(data_list)

    df.to_csv("meta_data.csv", index=False, encoding="utf-8")

    return df


def make_csv_with_value(data_path: str) -> pd.DataFrame:
    """
    메타 데이터에 맞는 수치 데이터를 추출하여 csv로 저장하고 데이터프레임으로 반환하는 코드
    Args:
        data_path: 데이터 파일 경로(파일 이름에 연도월 포함)
    Returns:
        df: 월별 데이터를 데이터프레임으로 변환한 데이터
    """
    # 메타 데이터 및 데이터 파일 불러오기
    original_df = pd.read_excel(data_path)
    meta_data = pd.read_csv("value_data.csv")
    data_dict = {
        "year": [],
        "month": [],
        "register_count": [],
    }

    # 데이터 파일 이름에서 연도월 추출
    year_month = data_path.split("시도별 ")[1].split(".xlsx")[0]
    year, month = year_month[:4], year_month[4:]

    # 메타 데이터 순회하여 original_df에서 해당하는 값 추출
    for index, row in meta_data.iterrows():
        sido = row["sido"]
        sigungu = row["sigungu"]
        car_type = row["car_type"]
        car_purpose = row["car_purpose"]

        data_dict["year"].append(year)
        data_dict["month"].append(month)
        filtered_index = original_df[(original_df.iloc[:, 1] == sido) & (original_df.iloc[:, 2] == sigungu)].index
        value = original_df.loc[filtered_index, (original_df.iloc[3, :] == car_type) & (original_df.iloc[4, :] == car_purpose)].values[0]

        data_dict["register_count"].append(value)

    df = pd.DataFrame(data_dict)

    df.to_csv("value_data.csv", index=False, encoding="utf-8")

    return df
