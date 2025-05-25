import io
import re
import zipfile
from datetime import datetime, timedelta
from typing import List, Dict, TypeVar, Union, Optional, Any
from enum import Enum
import pandas as pd
import pytz
import requests
from dateutil.relativedelta import relativedelta

Response = requests.models.Response


class NoDataAvailableError(Exception):
    pass


class BadDateRangeError(Exception):
    pass


class OASISGroupReport(Enum):
    """Enumeration of available CAISO/OASIS Group reports.
    
    OASISGroupReport tuple format:
        (group_id, version, result_format)
        group_id: str = Group ID defining group report collected
        version: int = Version of group report (1 works for most, but swapping out for other values can sometimes return additional data; see docs for more info)
        result_format: int = filetype of rendered response data (6 = CSV, 1/None = XML)
    
    For more info on Group Report definitions, see Part 7 of docs for OASIS API Specs:
    https://www.caiso.com/Documents/OASIS-InterfaceSpecification_v4_3_5Clean_Spring2017Release.pdf
    """
    DAM_LMP = ("DAM_LMP_GRP", 12, 6) # Daily DAM LMPs
    DAM_SPTIE_LMP = ("DAM_SPTIE_LMP_GRP", 1, 6) # DAM Scheduling Point Tie LMPs
    RTM_LMP = ("RTM_LMP_GRP", 1, 6) # Interval RTM LMPs
    HASP_LMP = ("HASP_LMP_GRP", 1, 6) # Hour Ahead Scheduling Point LMPs
    RTD_SPTIE_LMP = ("RTD_SPTIE_LMP_GRP", 1, 6) # RTD Scheduling Point Tie LMPs

    def __init__(self, group_id: str, version: int, result_format: int):
        self.group_id = group_id
        self.version = version
        self.result_format = result_format # result format 6 = CSV (otherwise XML)


class Oasis:
    def __init__(self, timeout: int = 15) -> None:
        self.base_url: str = "http://oasis.caiso.com/oasisapi/SingleZip?"
        self.group_url: str = "http://oasis.caiso.com/oasisapi/GroupZip?"
        self.timeout: int = timeout

    @staticmethod
    def _validate_date_range(start: datetime, end: datetime) -> None:

        STRNOW: str = str(datetime.now().strftime("%Y-%m-%d"))
        error: Union[str, None] = None

        if start > end:
            error = "Start must be before end."

        if start > datetime.now():
            error = "Start must be before today, " + STRNOW

        elif end > datetime.now():
            error = "End must be before or equal to today, " + STRNOW

        if start.date() == end.date():
            error = "Start and end cannot be equal. To return data only from start date, pass only start."

        if error is not None:
            raise BadDateRangeError(error)

    def request(self, params: Dict[str, Any]) -> Response:
        """Make http request

        Base method to get request at base_url

        Args:
            params (dict): keyword params to construct request

        Returns:
            response: requests response object
        """

        resp: Response = requests.get(self.base_url, params=params, timeout=self.timeout)
        resp.raise_for_status()

        headers: str = resp.headers["content-disposition"]

        if re.search(r"\.xml\.zip;$", headers):
            raise NoDataAvailableError("No data available for this query.")

        return resp
    
    def request_group(self, params: Dict[str, Any]) -> Response:
        """Make http request for GroupZip endpoint

        Base method to get request at group_url

        Args:
            params (dict): keyword params to construct request

        Returns:
            response: requests response object
        """

        resp: Response = requests.get(self.group_url, params=params, timeout=self.timeout)
        resp.raise_for_status()
        
        headers: str = resp.headers["content-disposition"]
        
        if re.search(r"\.xml\.zip;$", headers):
            groupid = params.get("groupid", "<unknown>")
            raise NoDataAvailableError(f"No data available for this query (groupid {groupid}).")
        
        return resp
        
        

    def _get_UTC_string(
        self,
        dt: datetime,
        local_tz: str = "America/Los_Angeles",
        fmt: str = "%Y%m%dT%H:%M-0000",
    ) -> str:
        """Convert local datetime to UTC string

        Converts datetime.datetime or pandas.Timestamp in local time to
        to UTC string for constructing HTTP request

        Args:
            dt (datetime.datetime): datetime to convert
            local_tz (str): timezone

        Returns:
            utc (str): UTC string
        """

        tz_ = pytz.timezone(local_tz)
        return tz_.localize(dt).astimezone(pytz.UTC).strftime(fmt)

    def get_df(
        self,
        response: Response,
        parse_dates: Optional[Union[List[int], bool]] = False,
        sort_values: Optional[List[str]] = None,
        reindex_columns: Optional[List[str]] = None,
    ) -> pd.DataFrame:

        """Convert response to datframe

        Converts requests.response to pandas.DataFrame

        Args:
            r : requests response object
            parse_dates (bool, list): which columns to parse dates if any
            sort_values(list): which columsn to sort by if any

        Returns:
            df (pandas.DataFrame): pandas dataframe
        """

        with io.BytesIO() as buffer:
            try:
                buffer.write(response.content)
                buffer.seek(0)
                z: zipfile.ZipFile = zipfile.ZipFile(buffer)

            except zipfile.BadZipFile as e:
                print("Bad zip file", e)

            else:  # TODO need to annotate csv
                csv = z.open(z.namelist()[0])  # ignores all but first file in zip
                df: pd.DataFrame = pd.read_csv(csv, parse_dates=parse_dates)

                df = df.rename(columns={"PRC": "MW"})

                if sort_values:
                    df = df.sort_values(sort_values).reset_index(drop=True)

                if reindex_columns:
                    df = df.reindex(columns=reindex_columns)

        return df
    
    def get_group_df(self, response: Response) -> pd.DataFrame:
        """Convert group-report response to pandas DF.
        
        Args:
            response (requests.Response): response object
            
        Returns:
            df (pandas.DataFrame): pandas DF
        """

        with io.BytesIO(response.content) as buffer:
            with zipfile.ZipFile(buffer) as zip_file:
                frames = []
                for member in zip_file.namelist():
                    with zip_file.open(member) as f:
                        frames.append(pd.read_csv(f, low_memory=False))
        return pd.concat(frames, ignore_index=True)
    
    def fetch_group_report(
        self, 
        report: OASISGroupReport,
        trading_day: datetime,
        local_tz: str = "America/Los_Angeles",
    ) -> pd.DataFrame:
        """Fetch group report for given trading day.
        
        Args:
            report (OASISGroupReport): OASISGroupReport enum defining which report to fetch
            trading_day (datetime.datetime): trading day to fetch report for
            local_tz (str): timezone
            
        Returns:
            df (pandas.DataFrame): pandas DF
            
        See official documentation of OASIS API Specs for more details on group report definitions:
        https://www.caiso.com/documents/oasis-interfacespecification_v5_1_2clean_fall2017release.pdf
        """
        
        if trading_day.date() > datetime.now().date():
            raise BadDateRangeError(
                f"Trading day {trading_day:%Y-%m-%d} cannot be in the future."
            )
        
        params: Dict[str, Any] = {
            "groupid": report.group_id,
            "version": report.version, 
            "resultformat": report.result_format,
            "startdatetime": self._get_UTC_string(trading_day, local_tz),
        }
        
        resp: Response = self.request_group(params)
        
        df: pd.DataFrame = self.get_group_df(resp)
        
        if "OPR_DT" in df.columns:
            df["OPR_DT"] = pd.to_datetime(df["OPR_DT"]).dt.date
            
        return df


class Node(Oasis):
    """CAISO PNode"""

    def __init__(self, node: str) -> None:
        self.node = node
        super().__init__()

    def __repr__(self):
        return f"Node(node='{self.node}')"

    def get_lmps(
        self, start: datetime, end: Optional[datetime] = None, market: str = "DAM"
    ) -> pd.DataFrame:
        """Get LMPs

        Gets Locational Market Prices (LMPs) for a given pair of start and end dates

        Args:
            start (datetime.datetime): start date, inclusive
            end (datetime.datetime): end date, exclusive
            market (str): market for prices; must be "DAM", "RTM", or "RTPD"

        Returns:
            (pandas.DataFrame): Pandas dataframe containing the LMPs for given period, market
        """

        if end is None:
            end = start + timedelta(days=1)

        self._validate_date_range(start, end)

        QUERY_MAPPING: Dict[str, str] = {
            "DAM": "PRC_LMP",
            "RTM": "PRC_INTVL_LMP",
            "RTPD": "PRC_RTPD_LMP",
        }

        COLUMNS: List[str] = [
            "INTERVALSTARTTIME_GMT",
            "INTERVALENDTIME_GMT",
            "OPR_DT",
            "OPR_HR",
            "OPR_INTERVAL",
            "NODE_ID_XML",
            "NODE_ID",
            "NODE",
            "MARKET_RUN_ID",
            "LMP_TYPE",
            "XML_DATA_ITEM",
            "PNODE_RESMRID",
            "GRP_TYPE",
            "POS",
            "MW",
            "GROUP",
        ]

        if market not in QUERY_MAPPING.keys():
            raise ValueError("market must be 'DAM', 'RTM' or 'RTPD'")

        params: Dict[str, Any] = {
            "queryname": QUERY_MAPPING[market],
            "market_run_id": market,
            "startdatetime": self._get_UTC_string(start),
            "enddatetime": self._get_UTC_string(end),
            "version": 1,
            "node": self.node,
            "resultformat": 6,
        }

        resp: Response = self.request(params)

        return self.get_df(
            resp,
            parse_dates=[2],
            sort_values=["OPR_DT", "OPR_HR"],
            reindex_columns=COLUMNS,
        )

    def get_month_lmps(self, year: int, month: int) -> pd.DataFrame:

        """Get LMPs for entire month

        Helper method to get LMPs for a complete month

        Args:
            year(int): year of LMPs desired
            month(int): month of LMPs desired

        Returns:
            (pandas.DataFrame): Pandas dataframe containing the LMPs for given month
        """

        start: datetime = datetime(year, month, 1)
        end: datetime = start + relativedelta(months=1)

        return self.get_lmps(start, end)

    @classmethod
    def SP15(cls):
        return cls("TH_SP15_GEN-APND")

    @classmethod
    def NP15(cls):
        return cls("TH_NP15_GEN-APND")

    @classmethod
    def ZP26(cls):
        return cls("TH_ZP26_GEN-APND")

    @classmethod
    def SCEDLAP(cls):
        return cls("DLAP_SCE-APND")

    @classmethod
    def PGAEDLAP(cls):
        return cls("DLAP_PGAE-APND")

    @classmethod
    def SDGEDLAP(cls):
        return cls("DLAP_SDGE-APND")


class Atlas(Oasis):
    """Atlas data"""

    def __init__(self):
        super().__init__()

    def get_pnodes(self, start: datetime, end: datetime) -> pd.DataFrame:

        """Get pricing nodes

        Get list of pricing nodes and aggregated pricing nodes extant between
        start and stop period

        Args:
            start (datetime.datetime): start date
            end (datetime.datetime): end date

        Returns:
            (pandas.DataFrame): List of pricing nodes
        """

        self._validate_date_range(start, end)

        params: Dict[str, Any] = {
            "queryname": "ATL_PNODE",
            "startdatetime": self._get_UTC_string(start),
            "enddatetime": self._get_UTC_string(end),
            "Pnode_type": "ALL",
            "version": 1,
            "resultformat": 6,
        }

        response = self.request(params)

        return self.get_df(response)


class SystemDemand(Oasis):
    """System Demand"""

    def __init__(self):
        super().__init__()

    def get_peak_demand_forecast(self, start: datetime, end: datetime) -> pd.DataFrame:
        """Get peak demand forecast

        Get peak demand forecasted between start and end dates

        Args:
            start (datetime.datetime): start date
            end (datetime.datetime): end date

        Returns:
            (pandas.DataFrame): peak demand forecast
        """

        params: Dict[str, Any] = {
            "queryname": "SLD_FCST_PEAK",
            "startdatetime": self._get_UTC_string(start),
            "enddatetime": self._get_UTC_string(end),
            "version": 1,
            "resultformat": 6,
        }

        resp: Response = self.request(params)

        return self.get_df(resp)

    def get_demand_forecast(self, start: datetime, end: datetime) -> pd.DataFrame:

        """Get demand forecast

        Get demand forecasted between start and end dates

        Args:
            start (datetime.datetime): start date
            end (datetime.datetime): end date

        Returns:
            (pandas.DataFrame): demand forecast
        """

        params: Dict[str, Any] = {
            "queryname": "SLD_FCST",
            "startdatetime": self._get_UTC_string(start),
            "enddatetime": self._get_UTC_string(end),
            "version": 1,
            "resultformat": 6,
        }

        resp = self.request(params)

        return self.get_df(resp)


def get_lmps(
    node: str,
    start: datetime,
    end: Optional[datetime] = None,
    market: Optional[str] = "DAM",
) -> pd.DataFrame:

    """Get LMPs

    Gets Locational Market Prices (LMPs) at node for a given pair of start and end dates

    Args:
        node (str): pricing node
        start (datetime.datetime): start date, inclusive
        end (datetime.datetime): end date, exclusive
        market (str): market for prices; must be "DAM", "RTM", or "RTPD"

    Returns:
        (pandas.DataFrame): Pandas dataframe containing the LMPs for given period, market
    """

    oasis = Oasis()

    if end is None:
        end = start + timedelta(days=1)

    oasis._validate_date_range(start, end)

    QUERY_MAPPING: Dict[str, str] = {
        "DAM": "PRC_LMP",
        "RTM": "PRC_INTVL_LMP",
        "RTPD": "PRC_RTPD_LMP",
    }

    COLUMNS: List[str] = [
        "INTERVALSTARTTIME_GMT",
        "INTERVALENDTIME_GMT",
        "OPR_DT",
        "OPR_HR",
        "OPR_INTERVAL",
        "NODE_ID_XML",
        "NODE_ID",
        "NODE",
        "MARKET_RUN_ID",
        "LMP_TYPE",
        "XML_DATA_ITEM",
        "PNODE_RESMRID",
        "GRP_TYPE",
        "POS",
        "MW",
        "GROUP",
    ]

    if market not in QUERY_MAPPING.keys():
        raise ValueError("market must be 'DAM', 'RTM' or 'RTPD'")

    params: Dict[str, Any] = {
        "queryname": QUERY_MAPPING[market],
        "market_run_id": market,
        "startdatetime": oasis._get_UTC_string(start),
        "enddatetime": oasis._get_UTC_string(end),
        "version": 1,
        "node": node,
        "resultformat": 6,
    }

    resp: Response = oasis.request(params)

    return oasis.get_df(
        resp,
        parse_dates=[2],
        sort_values=["OPR_DT", "OPR_HR"],
        reindex_columns=COLUMNS,
    )


def get_daily_dam_lmps(trading_day: datetime) -> pd.DataFrame: 
    """Shorthand for fetching daily DAM LMPs for a given trading day.
    
    Args:
        trading_day (datetime.datetime): trading day to fetch report for
        local_tz (str): timezone
    
    Returns:
        df (pandas.DataFrame): pandas DF    
    """
    
    oasis = Oasis()
    
    return oasis.fetch_group_report(
        report=OASISGroupReport.DAM_LMP,
        trading_day=trading_day,
    )
    