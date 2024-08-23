
# core python
import json
from abc import ABC, abstractmethod
import datetime
import logging
import os
import sys
import threading
import time
import traceback
from typing import Any, Dict, List, Optional, Type, Union

# pypi
from confluent_kafka import Consumer, OFFSET_BEGINNING, OFFSET_END
from sqlalchemy import sql
import pandas as pd

# native
from domain.events import (Event, TransactionCreatedEvent, TransactionUpdatedEvent, TransactionDeletedEvent
    , TransactionCommentCreatedEvent, TransactionCommentUpdatedEvent, TransactionCommentDeletedEvent
)
from domain.event_handlers import EventHandler
from domain.message_brokers import MessageBroker
from domain.message_subscribers import MessageSubscriber
from domain.models import Transaction, TransactionComment
from domain.repositories import HeartbeatRepository

from infrastructure.in_memory_repositories import (
    APXDBvPortfolioInMemoryRepository, APXDBvPortfolioBaseInMemoryRepository,
    APXDBvPortfolioBaseCustomInMemoryRepository, APXDBvPortfolioBaseSettingExInMemoryRepository,
    APXDBvCurrencyInMemoryRepository, 
    APXDBvSecurityInMemoryRepository, APXRepDBSecurityHashInMemoryRepository,
)
from infrastructure.message_brokers import KafkaBroker
from infrastructure.models import (
    StreamingDataToRefresh, KafkaTopicStreamingDataRefresher, 
    InMemoryDataToRefresh, KafkaToInMemoryColumnMapping
)
from infrastructure.sql_procs import APXRepDBpAPXReadSecurityHashProc, APXDBTransactionActivityProcAndFunc
from infrastructure.sql_tables import (
    APXDBvPortfolioView, APXDBvPortfolioBaseView, APXDBvPortfolioBaseCustomView,
    APXDBvPortfolioBaseSettingExView, APXDBvCurrencyView, APXDBvSecurityView,
    APXDBvFXRateView,
    COREDBAPXfTransactionActivityQueueTable, COREDBAPXfTransactionActivityTable
)
from infrastructure.util.config import AppConfig
from infrastructure.util.dataframe import delete_rows, df_to_dict
from infrastructure.util.date import since_epoch_to_datetime
from infrastructure.util.logging import get_log_file_name


class DeserializationError(Exception):
    pass

class CriteriaNotMetException(Exception):
    pass



class KafkaMessageConsumer(MessageSubscriber):
    def __init__(self, topics, event_handler, heartbeat_repo: Union[HeartbeatRepository,None]=None):
        super().__init__(message_broker=KafkaBroker(), topics=topics, event_handler=event_handler)
        self.config = dict(self.message_broker.config)
        self.config.update(AppConfig().parser['kafka_consumer'])
        logging.info(f'Creating KafkaMessageConsumer with config: {self.config}')
        self.consumer = Consumer(self.config)
        self.heartbeat_repo = heartbeat_repo

    def consume(self, reset_offset: bool=False):
        
        logging.info(f'Consuming from topics: {self.topics}')

        self.reset_offset = reset_offset
        self.consumer.subscribe(self.topics, on_assign=self.on_assign)

        try:
            sleep_secs = int(AppConfig().get('kafka_consumer_lw', 'sleep_seconds', fallback=0))
            while True:
                msg = self.consumer.poll(5.0)
                if msg is None:
                    # Initial message consumption may take up to
                    # `session.timeout.ms` for the consumer group to
                    # rebalance and start consuming
                    logging.info("Waiting...")
                    
                    # Save heartbeat
                    if self.heartbeat_repo:
                        # Log file name provides a meaningful name, if app_name is not found
                        app_name = os.environ.get('APP_NAME') or get_log_file_name()
                        if not app_name:
                            # Still not found? Default to class name:
                            app_name = self.cn

                        # Create heartbeat 
                        hb = self.heartbeat_repo.heartbeat_class(group='LW-APX2SF-TXN', name=app_name)

                        # If it has a log attribute, populate it with something more meaningful:
                        if hasattr(hb, 'log'):
                            hb.log = f"HEARTBEAT => {self.cn} consuming {', '.join(self.topics)} messages from {self.config['bootstrap.servers']}; using event handler {self.event_handler}"

                        # Now we have the heartbeat ready to save. Save it: 
                        logging.debug(f'About to save heartbeat to {self.heartbeat_repo.cn}: {hb}')
                        res = self.heartbeat_repo.create(hb)

                elif msg.error():
                    logging.info(f"ERROR: {msg.error()}")
                elif msg.value() is not None:
                    # logging.info(f"Consuming message: {msg.value()}")
                    should_commit = True  # commit at the end, unless this gets overridden below
                    try:
                        event = self.deserialize(msg.value())

                        if event is None:
                            # A deserialize method returning None means the kafka message
                            # does not meet criteria for representing an Event that needs handling.
                            # Therefore if reaching here we should simply commit offset.
                            self.consumer.commit(message=msg)
                            continue
                        
                        # If reaching here, we have an Event that should be handled:
                        # logging.info(f"Handling {event}")
                        should_commit = self.event_handler.handle(event)
                        # logging.info(f"Done handling {event}")
                
                    except Exception as e:
                        if isinstance(e, DeserializationError):
                            logging.info(f'Exception while deserializing: {e}')
                            should_commit = self.event_handler.handle_deserialization_error(e)
                        else:
                            logging.info(e)  # TODO: any more valuable logging?
                    
                    # Commit, unless we should not based on above results
                    if should_commit:
                        self.consumer.commit(message=msg)
                        logging.info("Done committing offset")
                    else:
                        logging.info("Not committing offset, likely due to the most recent exception")


        except KeyboardInterrupt:
            pass
        finally:
            # Leave group and commit final offsets
            logging.info(f'Committing offset and closing {self.cn}...\n\n\n')
            self.consumer.close()

    def on_assign(self, consumer, partitions):
        if self.reset_offset:
            for p in partitions:
                logging.info(f"Resetting offset for {p}")
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)

    @abstractmethod
    def deserialize(self, message_value: bytes) -> Union[Event, None]:
        """ 
        Subclasses of KafkaMessageConsumer must implement a deserialize method.
        Returning None (rather than an Event) signifies that there is no Event to handle.
        This makes sense when the consumer is looking for specific criteria to represent 
        the desired Event, but that criteria is not necessarily met in every message from the topic(s).
        """
        
    def __del__(self):
        self.consumer.close()


class KafkaAPXMessageConsumer(KafkaMessageConsumer):
    def __init__(self, transaction_topics: List=[], in_memory_repo_refreshers: Dict[str, List[InMemoryDataToRefresh]]={}
                    , transaction_event_handler: Optional[EventHandler]=None, heartbeat_repo: Optional[HeartbeatRepository]=None):
        """ 
        Creates a KafkaMessageConsumer to do one (or both) of the following:
            - consume new/changed apxdb transactions/comments with the provided event handler 
            - consume new/changed apxdb supplementary data and update in-memory repositories accordingly
        """

        # Leverage base class __init__
        supplementary_data_topics = list(in_memory_repo_refreshers.keys())
        super().__init__(
            topics=transaction_topics + supplementary_data_topics,
            event_handler=transaction_event_handler,
            heartbeat_repo=heartbeat_repo,
        )

        # Additionally, assign the following
        self.transaction_topics = transaction_topics
        self.supplementary_data_topics = list(in_memory_repo_refreshers.keys())

    def consume(self, reset_offset: bool=False):
        
        if len(self.transaction_topics):
            logging.info(f"Consuming from transaction topics: {self.transaction_topics}")
        if len(self.supplementary_data_topics):
            logging.info(f"Consuming from supplementary data topics: {self.supplementary_data_topics}")
        
        self.reset_offset = reset_offset
        self.consumer.subscribe(self.transaction_topics + self.supplementary_data_topics, on_assign=self.on_assign)
        
        try:
            sleep_secs = int(AppConfig().get('kafka_consumer_lw', 'sleep_seconds', fallback=5))
            while True:
                msg = self.consumer.poll(sleep_secs)
                if msg is None:
                    # Initial message consumption may take up to
                    # `session.timeout.ms` for the consumer group to
                    # rebalance and start consuming
                    logging.debug("Waiting...")
                    
                    # Save heartbeat
                    if self.heartbeat_repo:
                        # Log file name provides a meaningful name, if app_name is not found
                        app_name = os.environ.get('APP_NAME') or get_log_file_name()
                        if not app_name:
                            # Still not found? Default to class name:
                            app_name = self.cn

                        # Create heartbeat 
                        hb = self.heartbeat_repo.heartbeat_class(group='LW-Transaction-Engine', name=app_name)

                        # If it has a log attribute, populate it with something more meaningful:
                        if hasattr(hb, 'log'):
                            hb.log = f"HEARTBEAT => {self.cn} consuming {', '.join(self.topics)} messages from {self.config['bootstrap.servers']}; using event handler {self.event_handler}"

                        # Now we have the heartbeat ready to save. Save it: 
                        logging.debug(f'About to save heartbeat to {self.heartbeat_repo.cn}: {hb}')
                        res = self.heartbeat_repo.create(hb)

                elif msg.error():
                    logging.info(f"ERROR: {msg.error()}")
                elif msg.value() is not None:
                    topic = msg.topic()
                    # logging.info(f"Consuming message: {msg.value()}")
                    should_commit = True  # commit at the end, unless this gets overridden below
                    
                    if topic in self.supplementary_data_topics:
                        try:
                            data = self.deserialize_streaming_data(msg.value())

                            # Now loop through each StreamingDataToRefresh and refresh accordingly:
                            for r in self.in_memory_repo_refreshers[topic]:
                                # Check if filter criteria are met
                                for k, v in r.filter_criteria.items():
                                    if data.get(k) not in v:
                                        criteria_not_met_msg = f'{topic}: Filter criteria {r.filter_criteria} not met by the following data: {data}'
                                        logging.info(criteria_not_met_msg)
                                        raise CriteriaNotMetException(criteria_not_met_msg)
                                
                                # If we made it here, any filter criteria is met
                                # Build params for read method call
                                params = {}
                                for cm in r.column_mapping:
                                    if cm.kafka_msg_column_name in data:
                                        params[cm.in_memory_repo_column_name] = data[cm.kafka_msg_column_name]
                                    else:
                                        logging.info(f'Column {cm.kafka_msg_column_name} not found from {topic}! {data}')
                                        # TODO_EH: raise exception?

                                # Now refresh the in-memory repo for params
                                logging.info(f'Refreshing {r.repo_class.__name__} for {params}')  # TODO_CLEANUP: too verbose
                                r.repo_class().refresh(params)

                        except Exception as e:
                            logging.info(f'{type(e).__name__} while reading streaming data: {e} {traceback.format_exc()}')
                            logging.info(f'{topic} msg: {msg.value()}')
                            should_commit = False
                            time.sleep(sleep_secs)
                    
                    if topic in self.transaction_topics:
                        try:
                            event = self.deserialize(msg.value())

                            if event is None:
                                # A deserialize method returning None means the kafka message
                                # does not meet criteria for representing an Event that needs handling.
                                # Therefore if reaching here we should simply commit offset.
                                self.consumer.commit(message=msg)
                                continue
                            
                            # If reaching here, we have an Event that should be handled:
                            # logging.info(f"Handling {event}")
                            should_commit = self.event_handler.handle(event)
                            # logging.info(f"Done handling {event}")
                    
                        except Exception as e:
                            if isinstance(e, DeserializationError):
                                logging.info(f'Exception while deserializing: {e}')
                                should_commit = self.event_handler.handle_deserialization_error(e)
                            else:
                                logging.info(e)  # TODO: any more valuable logging?
                        
                        # Commit, unless we should not based on above results
                        if should_commit:
                            self.consumer.commit(message=msg)
                            logging.info("Done committing offset")
                        else:
                            logging.info("Not committing offset, likely due to the most recent exception")


        except KeyboardInterrupt:
            pass
        finally:
            # Leave group and commit final offsets
            logging.info(f'Committing offset and closing {self.cn}...\n\n\n')
            self.consumer.close()

    def deserialize(self, message_value: bytes) -> Union[TransactionCreatedEvent, TransactionUpdatedEvent, TransactionDeletedEvent]:
        msg_dict = json.loads(message_value.decode('utf-8'))
        payload = msg_dict['payload']
        before = payload['before']
        after = payload['after']

        # Dates will be in days since 1/1/1970 ... make them datetime dates:
        if isinstance(before, dict):
            for k, v in before.items():
                if 'Date' in k and isinstance(v, int):
                    before[k] = (datetime.date(year=1970, month=1, day=1) + datetime.timedelta(days=v))
            if 'TradeDate' in before:  # Also add trade_date = TradeDate
                before['trade_date'] = before['TradeDate']
        if isinstance(after, dict):
            for k, v in after.items():
                if 'Date' in k and isinstance(v, int):
                    after[k] = (datetime.date(year=1970, month=1, day=1) + datetime.timedelta(days=v))
            if 'TradeDate' in after:  # Also add trade_date = TradeDate
                after['trade_date'] = after['TradeDate']

        if payload['op'] == 'c':
            # Get portfolio code from in-memory dict
            # portfolio_code = self.faust_tables[APXDBvPortfolioView][data['PortfolioID']].get('PortfolioCode')
            return (
                TransactionCommentCreatedEvent(TransactionComment(**after)) 
                    if after.get('TransactionCode').strip() == ';' 
                    else TransactionCreatedEvent(Transaction(**after))
            )

        elif payload['op'] == 'u':
            return (
                TransactionCommentUpdatedEvent(TransactionComment(**before), TransactionComment(**after))
                    if after.get('TransactionCode').strip() == ';' 
                    else TransactionUpdatedEvent(Transaction(**before), Transaction(**after))
            )

        elif payload['op'] == 'd':
            return (
                TransactionCommentDeletedEvent(TransactionComment(**before)) 
                    if before.get('TransactionCode').strip() == ';' 
                    else TransactionDeletedEvent(Transaction(**before))
            )

        else:
            return None  # No event

    def deserialize_streaming_data(self, message_value: bytes) -> Dict[str, Type[Any]]:
        msg_dict = json.loads(message_value.decode('utf-8'))
        payload = msg_dict['payload']
        before = payload['before']
        after = payload['after']

        # Dates will be in days since 1/1/1970 ... make them datetime dates:
        if isinstance(before, dict):
            for k, v in before.items():
                if 'Date' in k and isinstance(v, int):
                    before[k] = since_epoch_to_datetime(v)
        if isinstance(after, dict):
            for k, v in after.items():
                if 'Date' in k and isinstance(v, int):
                    after[k] = since_epoch_to_datetime(v)
        if payload['op'] == 'd':
            return before
        else:
            return after


class KafkaAPXTransactionMessageConsumer(KafkaAPXMessageConsumer):
    in_memory_repo_refreshers = {
        AppConfig().get('kafka_topics', 'apxdb_portfolio'): [
            InMemoryDataToRefresh(repo_class=APXDBvPortfolioInMemoryRepository, column_mapping=[KafkaToInMemoryColumnMapping('PortfolioID')]),
        ]
    }

    def __init__(self, transaction_event_handler: EventHandler, heartbeat_repo: Optional[HeartbeatRepository]=None):
        super().__init__(
            transaction_topics=[AppConfig().get('kafka_topics', 'apxdb_transaction')],
            in_memory_repo_refreshers=self.in_memory_repo_refreshers,
            transaction_event_handler=transaction_event_handler,
            heartbeat_repo=heartbeat_repo,
        )


class KafkaAPXSupplementaryDataMessageConsumer(KafkaAPXMessageConsumer):
    in_memory_repo_refreshers = {
        AppConfig().get('kafka_topics', 'apxdb_portfolio'): [
            InMemoryDataToRefresh(repo_class=APXDBvPortfolioInMemoryRepository, column_mapping=[KafkaToInMemoryColumnMapping('PortfolioID')]),
            InMemoryDataToRefresh(repo_class=APXDBvPortfolioBaseInMemoryRepository, column_mapping=[KafkaToInMemoryColumnMapping('PortfolioID', 'PortfolioBaseID')]),
            InMemoryDataToRefresh(repo_class=APXDBvPortfolioBaseCustomInMemoryRepository, column_mapping=[KafkaToInMemoryColumnMapping('PortfolioID', 'PortfolioBaseID')]),
            InMemoryDataToRefresh(repo_class=APXDBvPortfolioBaseSettingExInMemoryRepository, column_mapping=[KafkaToInMemoryColumnMapping('PortfolioID', 'PortfolioBaseID')]),
        ],
        AppConfig().get('kafka_topics', 'apxdb_aoobject'): [
            InMemoryDataToRefresh(repo_class=APXDBvPortfolioInMemoryRepository, column_mapping=[KafkaToInMemoryColumnMapping('ObjectID', 'PortfolioID')]),
            InMemoryDataToRefresh(repo_class=APXDBvPortfolioBaseInMemoryRepository, column_mapping=[KafkaToInMemoryColumnMapping('ObjectID', 'PortfolioBaseID')]),
            InMemoryDataToRefresh(repo_class=APXDBvPortfolioBaseCustomInMemoryRepository, column_mapping=[KafkaToInMemoryColumnMapping('ObjectID', 'PortfolioBaseID')]),
            InMemoryDataToRefresh(repo_class=APXDBvPortfolioBaseSettingExInMemoryRepository, column_mapping=[KafkaToInMemoryColumnMapping('ObjectID', 'PortfolioBaseID')]),
        ],
        AppConfig().get('kafka_topics', 'apxdb_portfoliobase'): [
            InMemoryDataToRefresh(repo_class=APXDBvPortfolioBaseInMemoryRepository, column_mapping=[KafkaToInMemoryColumnMapping('PortfolioBaseID')]),
            InMemoryDataToRefresh(repo_class=APXDBvPortfolioBaseSettingExInMemoryRepository, column_mapping=[KafkaToInMemoryColumnMapping('PortfolioBaseID')]),
        ],
        AppConfig().get('kafka_topics', 'apxdb_portfoliobaseext'): [
            InMemoryDataToRefresh(repo_class=APXDBvPortfolioBaseCustomInMemoryRepository, column_mapping=[KafkaToInMemoryColumnMapping('PortfolioBaseID')]),
        ],
        AppConfig().get('kafka_topics', 'apxdb_portfoliosetting'): [
            InMemoryDataToRefresh(repo_class=APXDBvPortfolioBaseSettingExInMemoryRepository, column_mapping=[KafkaToInMemoryColumnMapping('PortfolioSettingID', 'PortfolioBaseID')]),
        ],
        AppConfig().get('kafka_topics', 'apxdb_currency'): [
            InMemoryDataToRefresh(repo_class=APXDBvCurrencyInMemoryRepository, column_mapping=[KafkaToInMemoryColumnMapping('CurrencyCode')]),
            InMemoryDataToRefresh(repo_class=APXRepDBSecurityHashInMemoryRepository, column_mapping=[]),  # TODO: optimize this? Need currency param for stored proc?
        ],
        AppConfig().get('kafka_topics', 'apxdb_privateequity'): [
            InMemoryDataToRefresh(repo_class=APXDBvSecurityInMemoryRepository, column_mapping=[KafkaToInMemoryColumnMapping('SecurityID')]),
            InMemoryDataToRefresh(repo_class=APXRepDBSecurityHashInMemoryRepository, column_mapping=[KafkaToInMemoryColumnMapping('SecurityID')]),
        ],
        AppConfig().get('kafka_topics', 'apxdb_securitycontact'): [
            InMemoryDataToRefresh(repo_class=APXDBvSecurityInMemoryRepository, column_mapping=[KafkaToInMemoryColumnMapping('SecurityID')]),
            InMemoryDataToRefresh(repo_class=APXRepDBSecurityHashInMemoryRepository, column_mapping=[KafkaToInMemoryColumnMapping('SecurityID')]),
        ],
        AppConfig().get('kafka_topics', 'apxdb_securitypropertytoday'): [
            InMemoryDataToRefresh(repo_class=APXDBvSecurityInMemoryRepository, column_mapping=[KafkaToInMemoryColumnMapping('SecurityID')]),
            InMemoryDataToRefresh(repo_class=APXRepDBSecurityHashInMemoryRepository, column_mapping=[KafkaToInMemoryColumnMapping('SecurityID')]),
        ],
        AppConfig().get('kafka_topics', 'apxdb_creditrating'): [
            InMemoryDataToRefresh(repo_class=APXDBvSecurityInMemoryRepository, column_mapping=[KafkaToInMemoryColumnMapping('SecurityID')]),
            InMemoryDataToRefresh(repo_class=APXRepDBSecurityHashInMemoryRepository, column_mapping=[KafkaToInMemoryColumnMapping('SecurityID')]),
        ],
        AppConfig().get('kafka_topics', 'apxdb_derivedsourcemap'): [
            InMemoryDataToRefresh(repo_class=APXDBvSecurityInMemoryRepository, column_mapping=[KafkaToInMemoryColumnMapping('SourceLookupID', 'IndustryGroupID')], filter_criteria={'DerivedPropertyID': [-6]}),
            # TODO: do we need to also refresh APXRepDBSecurityHashInMemoryRepository?
        ],
        AppConfig().get('kafka_topics', 'apxdb_dividendrate'): [
            InMemoryDataToRefresh(repo_class=APXDBvSecurityInMemoryRepository, column_mapping=[KafkaToInMemoryColumnMapping('SecurityID')]),
            InMemoryDataToRefresh(repo_class=APXRepDBSecurityHashInMemoryRepository, column_mapping=[KafkaToInMemoryColumnMapping('SecurityID')]),
        ],
        AppConfig().get('kafka_topics', 'apxdb_securityassetclasstoday'): [
            InMemoryDataToRefresh(repo_class=APXDBvSecurityInMemoryRepository, column_mapping=[KafkaToInMemoryColumnMapping('SecurityID')]),
            InMemoryDataToRefresh(repo_class=APXRepDBSecurityHashInMemoryRepository, column_mapping=[KafkaToInMemoryColumnMapping('SecurityID')]),
        ],
        AppConfig().get('kafka_topics', 'apxdb_securitypropertytoday'): [
            InMemoryDataToRefresh(repo_class=APXDBvSecurityInMemoryRepository, column_mapping=[KafkaToInMemoryColumnMapping('SecurityID')], filter_criteria={'PropertyID': [-7, -21, -22, -23]}),
            InMemoryDataToRefresh(repo_class=APXRepDBSecurityHashInMemoryRepository, column_mapping=[KafkaToInMemoryColumnMapping('SecurityID')], filter_criteria={'PropertyID': [-7, -21, -22, -23]}),
        ],
        AppConfig().get('kafka_topics', 'apxdb_security'): [
            InMemoryDataToRefresh(repo_class=APXDBvSecurityInMemoryRepository, column_mapping=[KafkaToInMemoryColumnMapping('SecurityID')]),
            InMemoryDataToRefresh(repo_class=APXRepDBSecurityHashInMemoryRepository, column_mapping=[KafkaToInMemoryColumnMapping('SecurityID')]),
        ],
        AppConfig().get('kafka_topics', 'apxdb_aopropertylookup'): [
            InMemoryDataToRefresh(repo_class=APXDBvSecurityInMemoryRepository, column_mapping=[]),  # TODO: optimize this, rather than full view refresh every time?
            InMemoryDataToRefresh(repo_class=APXRepDBSecurityHashInMemoryRepository, column_mapping=[]),  # TODO: optimize this, rather than full view refresh every time?
        ],
        AppConfig().get('kafka_topics', 'apxdb_aoproperty'): [
            InMemoryDataToRefresh(repo_class=APXDBvSecurityInMemoryRepository, column_mapping=[], filter_criteria={'PropertyName': 'Today'}),
            InMemoryDataToRefresh(repo_class=APXRepDBSecurityHashInMemoryRepository, column_mapping=[], filter_criteria={'PropertyName': 'Today'}),
        ],
        AppConfig().get('kafka_topics', 'apxdb_fixedincome'): [
            InMemoryDataToRefresh(repo_class=APXDBvSecurityInMemoryRepository, column_mapping=[KafkaToInMemoryColumnMapping('SecurityID')]),
            InMemoryDataToRefresh(repo_class=APXRepDBSecurityHashInMemoryRepository, column_mapping=[KafkaToInMemoryColumnMapping('SecurityID')]),
        ],
        AppConfig().get('kafka_topics', 'apxdb_vrs'): [
            InMemoryDataToRefresh(repo_class=APXDBvSecurityInMemoryRepository, column_mapping=[KafkaToInMemoryColumnMapping('SecurityID')]),
            InMemoryDataToRefresh(repo_class=APXRepDBSecurityHashInMemoryRepository, column_mapping=[KafkaToInMemoryColumnMapping('SecurityID')]),
        ],
        AppConfig().get('kafka_topics', 'apxdb_securityproperty'): [
            InMemoryDataToRefresh(repo_class=APXRepDBSecurityHashInMemoryRepository, column_mapping=[KafkaToInMemoryColumnMapping('SecurityID')]),
        ],
        # TODO_CLEANUP: remove below once confirmed not keeping this in-memory to avoid hogging memory since it's date-series
        # AppConfig().get('kafka_topics', 'apxdb_fxratehistory'): [
        #     InMemoryDataToRefresh(repo_class=APXDBvFXRateView, column_mapping=[KafkaToInMemoryColumnMapping('NumeratorCurrCode'), KafkaToInMemoryColumnMapping('DenominatorCurrCode'), KafkaToInMemoryColumnMapping('AsOfDate')]),
        # ],
    }

    def __init__(self, heartbeat_repo: Optional[HeartbeatRepository]=None):
        super().__init__(
            in_memory_repo_refreshers=self.in_memory_repo_refreshers,
            heartbeat_repo=heartbeat_repo,
        )


