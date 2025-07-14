from dataclasses import dataclass, field
from typing import Optional, Any, List

@dataclass
class EconomyListInput:
    id: str = "all"
    q: Optional[Any] = None 
    labels: bool = False
    skipAggs: bool = False
    db: Optional[Any] = None

@dataclass
class EconomyInfoInput:
    id: str = "all"
    q: Optional[Any] = None
    skipAggs: bool = False
    db: Optional[Any] = None

@dataclass
class EconomyGetInput:
    id: str
    labels: bool = False
    db: Optional[Any] = None

@dataclass
class EconomySeriesInput:
    id: str = "all"
    q: Optional[Any] = None
    skipAggs: bool = False
    db: Optional[Any] = None
    name: str = "EconomyName"

@dataclass
class EconomyMetadataInput:
    id: str
    series: List[str] = field(default_factory=list)
    db: Optional[Any] = None



# income
@dataclass
class IncomeListInput:
    id: str = "all"
    q: Optional[str] = None

@dataclass
class IncomeInfoInput:
    id: str = "all"
    q: Optional[str] = None

@dataclass
class IncomeGetInput:
    id: str

@dataclass
class IncomeSeriesInput:
    id: str = "all"
    q: Optional[str] = None
    name: str = "IncomeGroupName"

@dataclass
class IncomeMembersInput:
    id: str



# lending

@dataclass
class LendingListInput:
    id: str = "all"
    q: Optional[str] = None

@dataclass
class LendingInfoInput:
    id: str = "all"
    q: Optional[str] = None

@dataclass
class LendingGetInput:
    id: str

@dataclass
class LendingSeriesInput:
    id: str = "all"
    q: Optional[str] = None
    name: str = "LendingGroupName"

@dataclass
class LendingMembersInput:
    id: str

#region
@dataclass
class RegionListInput:
    id: str = "all"
    q: Optional[str] = None
    group: Optional[str] = None

@dataclass
class RegionInfoInput:
    id: str = "all"
    q: Optional[str] = None
    group: Optional[str] = None

@dataclass
class RegionGetInput:
    id: str

@dataclass
class RegionSeriesInput:
    id: str = "all"
    q: Optional[str] = None
    group: Optional[str] = None
    name: str = "RegionName"

@dataclass
class RegionMembersInput:
    id: str
    param: str = "region"

#series
@dataclass
class SeriesMetadataInput:
    id: str
    economies: Optional[List[str]] = None
    time: Optional[List[str]] = None
    db: Optional[Any] = None

@dataclass
class SeriesInfoInput:
    id: str = "all"
    q: Optional[Any] = None
    topic: Optional[Any] = None
    db: Optional[Any] = None

@dataclass
class SeriesDataInput:
    id: str
    db: Any

@dataclass
class SeriesGetInput:
    id: str = "all"
    q: Optional[Any] = None
    topic: Optional[Any] = None
    db: Optional[Any] = None
    name: str = "SeriesName"


#source
@dataclass
class SourceListInput:
    id: str = "all"
    q: Optional[str] = None

@dataclass
class SourceInfoInput:
    id: str = "all"
    q: Optional[str] = None

@dataclass
class SourceGetInput:
    db: Optional[Any] = None

@dataclass
class SourceSeriesInput:
    id: str = "all"
    q: Optional[str] = None
    name: str = "SourceName"

@dataclass
class SourceHasMetadataInput:
    db: Optional[Any] = None

@dataclass
class SourceConceptsInput:
    db: Optional[Any] = None

@dataclass
class SourceFeatureInput:
    concept: str
    id: str
    db: Optional[Any] = None

@dataclass
class SourceFeaturesInput:
    concept: str
    id: str = "all"
    db: Optional[Any] = None


#time
@dataclass
class TimeListInput:
    id: str = "all"
    q: Optional[str] = None

@dataclass
class TimeInfoInput:
    id: str = "all"
    q: Optional[str] = None
    db: Optional[Any] = None

@dataclass
class TimeGetInput:
    id: str
    db: Optional[Any] = None

@dataclass
class TimeSeriesInput:
    id: str = "all"
    q: Optional[str] = None
    db: Optional[Any] = None
    name: str = "TimePeriodName"

@dataclass
class TimePeriodsInput:
    db: Optional[Any] = None

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