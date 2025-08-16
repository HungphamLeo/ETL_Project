from dataclasses import dataclass, field
from typing import Optional, Any, List


#economy


@dataclass
class EconomyDataFrameInput:
    id: str = "all"
    labels:  bool = False
    skipAggs: bool = False
    db: Optional[Any] = None


@dataclass
class EconomyMetadataInput:
    id: str
    series: List[str] = field(default_factory=list)
    db: Optional[Any] = None



# income

@dataclass
class IncomeSeriesInput:
    id: str = "all"
    q: Optional[str] = None
    name: str = "IncomeGroupName"



# lending
@dataclass
class LendingSeriesInput:
    id: str = "all"
    q: Optional[str] = None
    name: str = "LendingGroupName"


#region
@dataclass
class RegionSeriesInput:
    id: str = "all"
    q: Optional[str] = None
    group: Optional[str] = None
    name: str = "RegionName"

#series
@dataclass
class SeriesMetadataInput:
    id: str
    economies: Optional[List[str]] = None
    time: Optional[List[str]] = None
    db: Optional[Any] = None


@dataclass
class SeriesGetInput:
    id: str = "all"
    q: Optional[Any] = None
    topic: Optional[Any] = None
    db: Optional[Any] = None
    name: str = "SeriesName"


#source

@dataclass
class SourceInfoInput:
    id: str = "all"
    q: Optional[str] = None

@dataclass
class SourceSeriesInput:
    id: str = "all"
    q: Optional[str] = None
    name: str = "SourceName"



#time

@dataclass
class TimeSeriesInput:
    id: str = "all"
    q: Optional[str] = None
    db: Optional[Any] = None
    name: str = "TimePeriodName"


#topic
@dataclass
class TopicListInput:
    id: str = "all"
    q: Optional[str] = None

@dataclass
class TopicInfoInput:
    id: str = "all"
    q: Optional[str] = None

@dataclass
class TopicGetInput:
    id: str

@dataclass
class TopicSeriesInput:
    id: str = "all"
    q: Optional[str] = None
    name: str = "TopicName"

@dataclass
class TopicMembersInput:
    id: str
    maximum_member_id: Optional[int] = 100