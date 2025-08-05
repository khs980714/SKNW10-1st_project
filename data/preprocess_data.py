import os
import pandas as pd
import json

BASE_PATH = os.path.dirname(os.path.abspath(__file__))

# FOLDER_PATH = "/data/register_car/"
# EXCEL_LIST = os.listdir(FOLDER_PATH)

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

def reshape_data_meta(meta_path: str) -> pd.DataFrame:
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

    df.to_csv("reshape_data.csv", index=False, encoding="utf-8")

    return df
