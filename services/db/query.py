CREATE_CAR_REGISTER = """
CREATE TABLE IF NOT EXISTS car_register (
    register_index INTEGER PRIMARY KEY NOT NULL,
    sido VARCHAR(20),
    sigungu VARCHAR(20),
    car_type VARCHAR(10),
    car_purpose VARCHAR(10),
    year VARCHAR(4),
    month VARCHAR(2),
    value INTEGER
);
"""

SELECT_MAX_INDEX = """
SELECT MAX(register_index) FROM car_register;
"""

INSERT_CAR_REGISTER = """
INSERT INTO car_register (register_index, sido, sigungu, car_type, car_purpose, year, month, value) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
"""

# ------------------------------------------------------------
# ERD 수정으로 인해 하위 쿼리문 변경
# ------------------------------------------------------------

# # 자동차 등록 메타 테이블 생성 쿼리 - car_register.py
# CREATE_CAR_REGISTER_META= """
# CREATE TABLE IF NOT EXISTS car_register_meta (
#     register_index INTEGER PRIMARY KEY NOT NULL,
#     sido VARCHAR(20),
#     sigungu VARCHAR(20),
#     car_type VARCHAR(10),      -- 승용, 승합, 특수
#     car_purpose VARCHAR(10)    -- 관용, 자가용, 영업용, 계
# );
# """

# # 자동차 등록 데이터 테이블 생성 쿼리 - car_register.py
# CREATE_CAR_REGISTER_DATA = """
# CREATE TABLE IF NOT EXISTS car_register_data (
#     register_index INTEGER NOT NULL,
#     year INTEGER,
#     month INTEGER,
#     value INTEGER,
#     PRIMARY KEY (primary_key),
#     FOREIGN KEY (register_index) REFERENCES car_register_meta(register_index)
# );
# """

# # 자동차 등록 데이터 삽입 쿼리 - car_register.py
# INSERT_CAR_REGISTER_META = """
# INSERT INTO car_register_meta (register_index, sido, sigungu, car_type, car_purpose) VALUES (?, ?, ?, ?, ?);
# """

# # 자동차 등록 데이터 삽입 쿼리 - car_register.py
# INSERT_CAR_REGISTER_DATA = """
# INSERT INTO car_register_data (primary_key, register_index, year, month, value) VALUES (?, ?, ?, ?, ?);
# """