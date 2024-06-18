
# core python
from dataclasses import dataclass, field
import datetime
import os
import socket
from typing import Any, Callable, Dict, List, Type, Union

# native
from domain.models import Heartbeat
from infrastructure.in_memory_repositories import InMemorySingletonSQLRepository
from infrastructure.util.date import format_time
from infrastructure.util.logging import get_log_file_full_path
from infrastructure.util.stored_proc import BaseStoredProc
from infrastructure.util.table import BaseTable



@dataclass
class KafkaToStreamingDataColumnMapping:
    kafka_msg_column_name: str
    streaming_data_column_name: Union[str,None]=None

    def __post_init__(self):
        if not self.streaming_data_column_name:
            self.streaming_data_column_name = self.kafka_msg_column_name


@dataclass
class KafkaToInMemoryColumnMapping:
    kafka_msg_column_name: str
    in_memory_repo_column_name: Union[str,None]=None

    def __post_init__(self):
        if not self.in_memory_repo_column_name:
            self.in_memory_repo_column_name = self.kafka_msg_column_name


@dataclass
class StreamingDataToRefresh:
    table_class: Type[Union[BaseTable,BaseStoredProc]]
    column_mapping: List[KafkaToStreamingDataColumnMapping]
    filter_criteria: Dict[str, List[Any]] = field(default_factory=dict)


@dataclass
class InMemoryDataToRefresh:
    repo_class: Type[InMemorySingletonSQLRepository]
    column_mapping: List[KafkaToInMemoryColumnMapping]
    filter_criteria: Dict[str, List[Any]] = field(default_factory=dict)


@dataclass
class KafkaTopicStreamingDataRefresher:
    topic: str
    refreshes: List[StreamingDataToRefresh]


@dataclass
class MGMTDBHeartbeat(Heartbeat):
    log: str = 'HEARTBEAT'
    log_file_path: str = field(default_factory=get_log_file_full_path)

    def to_dict(self):
        """ Export an instance to dict format """
        base_instance_dict = super().to_dict()
        base_instance_dict.update({
            'run_group': self.group
            , 'run_name': self.name
            , 'data_dt': datetime.datetime.combine(self.data_date, datetime.datetime.min.time()).isoformat()
            , 'asofdate': format_time(self.modified_at)
            , 'log': self.log
            , 'log_file_path': self.log_file_path
            , 'run_type': 'INFO'
            , 'run_host': socket.gethostname().upper()
            , 'run_status':9000
            , 'run_status_text':'HEARTBEAT'
            , 'is_complete': 0
            , 'is_success': 0
            , 'asofuser': f"{os.getlogin()}_{os.environ.get('APP_NAME') or os.path.basename(__file__)}"
        })
        return base_instance_dict

    @classmethod
    def from_dict(cls, data: dict):
        """ Create an instance from dict """
        try:
            # Validate and prepare base class data
            base_data = {
                'group': data.get('group') or data.get('run_group'),
                'name': data.get('name') or data.get('run_name'),
                'data_date': data.get('data_date') or data.get('data_dt') or datetime.date.today(),
                'modified_at': data.get('modified_at') or data.get('asofdate') or datetime.datetime.now()
            }
            base_instance = super().from_dict(base_data)

            # Create MGMTDBHeartbeat instance
            log = data.get('log', 'HEARTBEAT')
            log_file_path = data.get('log_file_path', get_log_file_full_path())
            hb = cls(group=base_instance.group, name=base_instance.name, 
                       data_date=base_instance.data_date, modified_at=base_instance.modified_at,
                       log=log, log_file_path=log_file_path)
            return hb
        except KeyError as e:
            raise InvalidDictError(f"Missing required field: {e}")


@dataclass
class Txn2TableColMap:
    transaction_attribute: str
    table_column: str
    txn2table_format_function: Union[Callable, None] = None
    table2txn_format_function: Union[Callable, None] = None
    # round_to_decimal_places: Union[int, None] = None
    # populate_none_with_zero: bool = False

