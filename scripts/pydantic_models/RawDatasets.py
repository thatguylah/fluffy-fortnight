from pydantic import BaseModel, validator, ValidationError
import pandas as pd
from typing import ClassVar


# Define the Pydantic model
class RawDatasetExcelModel(BaseModel):
    ORDER_ID: str
    ORDER_TIME_PST: str
    CITY_DISTRICT_ID: int
    RPTG_AMT: float
    CURRENCY_CD: str
    ORDER_QTY: int

    # Reference for RAW_MAPPING
    raw_mapping_ids: ClassVar[set] = set()

    @validator("ORDER_TIME_PST")
    def validate_order_time(cls, v):
        if not isinstance(v, str) or not v.isdigit():
            raise ValueError("ORDER_TIME_PST must be a numeric string")
        return v

    @validator("CITY_DISTRICT_ID")
    def validate_city_district_id(cls, v):
        if v <= 0 or v not in cls.raw_mapping_ids:
            raise ValueError(
                "CITY_DISTRICT_ID must be a positive integer and must exist in RAW_MAPPING"
            )
        return v

    @validator("RPTG_AMT")
    def validate_rptg_amt(cls, v):
        if v < 0:
            raise ValueError("RPTG_AMT must be non-negative")
        return v

    @validator("CURRENCY_CD")
    def validate_currency_cd(cls, v):
        allowed_currencies = {"USD", "RMB"}
        if v not in allowed_currencies:
            raise ValueError(f"CURRENCY_CD must be one of {allowed_currencies}")
        return v

    @validator("ORDER_QTY")
    def validate_order_qty(cls, v):
        if pd.isna(v) or v <= 0:
            raise ValueError("ORDER_QTY must be a positive integer")
        return v


class RawDatasetJSONModel(BaseModel):
    ORDER_ID: str
    ORDER_TIME_PST: int
    SHIP_TO_CITY_CD: str
    SHIP_TO_DISTRICT_NAME: str
    RPTG_AMT: float
    CURRENCY_CD: str
    ORDER_QTY: int

    # Reference for RAW_MAPPING
    raw_mapping_city_cd: ClassVar[set] = set()
    raw_mapping_district_names: ClassVar[set] = set()

    @validator("ORDER_TIME_PST")
    def validate_order_time(cls, v):
        if not isinstance(v, int) or v < 50000 or v > 120000:
            raise ValueError(
                "ORDER_TIME_PST must be an integer between 50000 and 120000 (5 AM to 12 PM)"
            )
        return v

    # @validator("SHIP_TO_CITY_CD")
    # def validate_ship_to_city_cd(cls, v):
    #     if v not in cls.raw_mapping_city_cd:
    #         raise ValueError("SHIP_TO_CITY_CD must exist in RAW_MAPPING")
    #     return v

    # @validator("SHIP_TO_DISTRICT_NAME")
    # def validate_ship_to_district_name(cls, v):
    #     if v not in cls.raw_mapping_district_names:
    #         raise ValueError("SHIP_TO_DISTRICT_NAME must exist in RAW_MAPPING")
    #     return v

    @validator("RPTG_AMT")
    def validate_rptg_amt(cls, v):
        if v < 0:
            raise ValueError("RPTG_AMT must be non-negative")
        return v

    @validator("CURRENCY_CD")
    def validate_currency_cd(cls, v):
        allowed_currencies = {"USD", "RMB"}
        if v not in allowed_currencies:
            raise ValueError(f"CURRENCY_CD must be one of {allowed_currencies}")
        return v

    @validator("ORDER_QTY")
    def validate_order_qty(cls, v):
        if pd.isna(v) or v <= 0:
            raise ValueError("ORDER_QTY must be a positive integer")
        return v


def validate_and_replace(df: pd.DataFrame) -> pd.DataFrame:
    errors = []
    for index, row in df.iterrows():
        try:
            RawDatasetExcelModel(**row.to_dict())
        except ValidationError as e:
            error_detail = {"ORDER_ID": row["ORDER_ID"], "errors": e.errors()}
            errors.append(error_detail)
            for error in e.errors():
                field = error["loc"][0]
                df.at[index, field] = pd.NA
    return df, errors


def validate_only(df: pd.DataFrame) -> (pd.DataFrame, list):
    errors = []
    for index, row in df.iterrows():
        try:
            RawDatasetJSONModel(**row.to_dict())
        except ValidationError as e:
            error_detail = {"ORDER_ID": row["ORDER_ID"], "errors": e.errors()}
            errors.append(error_detail)
    return df, errors
