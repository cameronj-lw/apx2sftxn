
# core python
import json
from abc import ABC, abstractmethod
import datetime
import logging
import os
import sys
import time
import traceback
from typing import Any, Dict, List, Type, Union

# pypi
from confluent_kafka import Consumer, OFFSET_BEGINNING, OFFSET_END
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

from infrastructure.message_brokers import KafkaBroker
from infrastructure.models import KafkaToStreamingDataColumnMapping, StreamingDataToRefresh, KafkaTopicStreamingDataRefresher
from infrastructure.sql_procs import APXRepDBpAPXReadSecurityHashProc
from infrastructure.sql_tables import (
    APXDBvPortfolioView, APXDBvPortfolioBaseView, APXDBvPortfolioBaseCustomView,
    APXDBvPortfolioBaseSettingExView, APXDBvCurrencyView, APXDBvSecurityView,
    APXDBvFXRateView
)
from infrastructure.util.config import AppConfig
from infrastructure.util.dataframe import delete_rows
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


class KafkaAPXTransactionMessageConsumer(KafkaMessageConsumer):
    # streaming_data_refreshers = [
    #     KafkaTopicStreamingDataRefresher(topic=AppConfig().get('kafka_topics', 'apxdb_portfolio'), [
    #         StreamingDataToRefresh(table_class=APXDBvPortfolioView, column_mapping=[KafkaToStreamingDataColumnMapping('PortfolioID')]),
    #         StreamingDataToRefresh(table_class=APXDBvPortfolioBaseView, column_mapping=[KafkaToStreamingDataColumnMapping('PortfolioBaseID', 'PortfolioID')]),
    #         StreamingDataToRefresh(table_class=APXDBvPortfolioBaseCustomView, column_mapping=[KafkaToStreamingDataColumnMapping('PortfolioBaseID', 'PortfolioID')]),
    #         StreamingDataToRefresh(table_class=APXDBvPortfolioBaseSettingExView, column_mapping=[KafkaToStreamingDataColumnMapping('PortfolioBaseID', 'PortfolioID')]),
    #     ]),
    #     KafkaTopicStreamingDataRefresher(topic=AppConfig().get('kafka_topics', 'apxdb_aoobject'), [
    #         StreamingDataToRefresh(table_class=APXDBvPortfolioView, column_mapping=[KafkaToStreamingDataColumnMapping('ObjectID', 'PortfolioID')]),
    #         StreamingDataToRefresh(table_class=APXDBvPortfolioBaseView, column_mapping=[KafkaToStreamingDataColumnMapping('ObjectID', 'PortfolioBaseID')]),
    #         StreamingDataToRefresh(table_class=APXDBvPortfolioBaseCustomView, column_mapping=[KafkaToStreamingDataColumnMapping('ObjectID', 'PortfolioBaseID')]),
    #         StreamingDataToRefresh(table_class=APXDBvPortfolioBaseSettingExView, column_mapping=[KafkaToStreamingDataColumnMapping('ObjectID', 'PortfolioBaseID')]),
    #     ]),
    #     KafkaTopicStreamingDataRefresher(topic=AppConfig().get('kafka_topics', 'apxdb_portfoliobase'), [
    #         StreamingDataToRefresh(table_class=APXDBvPortfolioBaseView, column_mapping=[KafkaToStreamingDataColumnMapping('PortfolioBaseID')]),
    #         StreamingDataToRefresh(table_class=APXDBvPortfolioBaseSettingExView, column_mapping=[KafkaToStreamingDataColumnMapping('PortfolioBaseID')]),
    #     ]),
    #     KafkaTopicStreamingDataRefresher(topic=AppConfig().get('kafka_topics', 'apxdb_portfoliobaseext'), [
    #         StreamingDataToRefresh(table_class=APXDBvPortfolioBaseCustomView, column_mapping=[KafkaToStreamingDataColumnMapping('PortfolioBaseID')]),
    #     ]),
    #     KafkaTopicStreamingDataRefresher(topic=AppConfig().get('kafka_topics', 'apxdb_portfoliosetting'), [
    #         StreamingDataToRefresh(table_class=APXDBvPortfolioBaseSettingExView, column_mapping=[KafkaToStreamingDataColumnMapping('PortfolioSettingID', 'PortfolioBaseID')]),
    #     ]),
    #     KafkaTopicStreamingDataRefresher(topic=AppConfig().get('kafka_topics', 'apxdb_currency'), [
    #         StreamingDataToRefresh(table_class=APXDBvCurrencyView, column_mapping=[KafkaToStreamingDataColumnMapping('CurrencyCode')]),
    #         StreamingDataToRefresh(table_class=APXRepDBpAPXReadSecurityHashProc, column_mapping=[]),  # TODO: optimize this? Need currency param for stored proc?
    #     ]),
    #     KafkaTopicStreamingDataRefresher(topic=AppConfig().get('kafka_topics', 'apxdb_privateequity'), [
    #         StreamingDataToRefresh(table_class=APXDBvSecurityView, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')]),
    #         StreamingDataToRefresh(table_class=APXRepDBpAPXReadSecurityHashProc, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')]),
    #     ]),
    #     KafkaTopicStreamingDataRefresher(topic=AppConfig().get('kafka_topics', 'apxdb_securitycontact'), [
    #         StreamingDataToRefresh(table_class=APXDBvSecurityView, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')]),
    #         StreamingDataToRefresh(table_class=APXRepDBpAPXReadSecurityHashProc, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')]),
    #     ]),
    #     KafkaTopicStreamingDataRefresher(topic=AppConfig().get('kafka_topics', 'apxdb_securitypropertytoday'), [
    #         StreamingDataToRefresh(table_class=APXDBvSecurityView, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')]),
    #         StreamingDataToRefresh(table_class=APXRepDBpAPXReadSecurityHashProc, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')]),
    #     ]),
    #     KafkaTopicStreamingDataRefresher(topic=AppConfig().get('kafka_topics', 'apxdb_creditrating'), [
    #         StreamingDataToRefresh(table_class=APXDBvSecurityView, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')]),
    #         StreamingDataToRefresh(table_class=APXRepDBpAPXReadSecurityHashProc, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')]),
    #     ]),
    #     KafkaTopicStreamingDataRefresher(topic=AppConfig().get('kafka_topics', 'apxdb_derivedsourcemap'), [
    #         StreamingDataToRefresh(table_class=APXDBvSecurityView, column_mapping=[KafkaToStreamingDataColumnMapping('SourceLookupID', 'IndustryGroupLookupID')], filter_criteria={'DerivedPropertyID': [-6]}),
    #         # TODO: do we need to also refresh APXRepDBpAPXReadSecurityHashProc?
    #     ]),
    #     KafkaTopicStreamingDataRefresher(topic=AppConfig().get('kafka_topics', 'apxdb_dividendrate'), [
    #         StreamingDataToRefresh(table_class=APXDBvSecurityView, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')]),
    #         StreamingDataToRefresh(table_class=APXRepDBpAPXReadSecurityHashProc, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')]),
    #     ]),
    #     KafkaTopicStreamingDataRefresher(topic=AppConfig().get('kafka_topics', 'apxdb_securityassetclasstoday'), [
    #         StreamingDataToRefresh(table_class=APXDBvSecurityView, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')]),
    #         StreamingDataToRefresh(table_class=APXRepDBpAPXReadSecurityHashProc, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')]),
    #     ]),
    #     KafkaTopicStreamingDataRefresher(topic=AppConfig().get('kafka_topics', 'apxdb_securitypropertytoday'), [
    #         StreamingDataToRefresh(table_class=APXDBvSecurityView, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')], filter_criteria={'PropertyID': [-7, -21, -22, -23]}),
    #         StreamingDataToRefresh(table_class=APXRepDBpAPXReadSecurityHashProc, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')], filter_criteria={'PropertyID': [-7, -21, -22, -23]}),
    #     ]),
    #     KafkaTopicStreamingDataRefresher(topic=AppConfig().get('kafka_topics', 'apxdb_security'), [
    #         StreamingDataToRefresh(table_class=APXDBvSecurityView, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')]),
    #         StreamingDataToRefresh(table_class=APXRepDBpAPXReadSecurityHashProc, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')]),
    #     ]),
    #     KafkaTopicStreamingDataRefresher(topic=AppConfig().get('kafka_topics', 'apxdb_aopropertylookup'), [
    #         StreamingDataToRefresh(table_class=APXDBvSecurityView, column_mapping=[]),  # TODO: optimize this, rather than full view refresh every time?
    #         StreamingDataToRefresh(table_class=APXRepDBpAPXReadSecurityHashProc, column_mapping=[]),  # TODO: optimize this, rather than full view refresh every time?
    #     ]),
    #     KafkaTopicStreamingDataRefresher(topic=AppConfig().get('kafka_topics', 'apxdb_aoproperty'), [
    #         StreamingDataToRefresh(table_class=APXDBvSecurityView, column_mapping=[], filter_criteria={'PropertyName': 'Today'}),
    #         StreamingDataToRefresh(table_class=APXRepDBpAPXReadSecurityHashProc, column_mapping=[], filter_criteria={'PropertyName': 'Today'}),
    #     ]),
    #     KafkaTopicStreamingDataRefresher(topic=AppConfig().get('kafka_topics', 'apxdb_fixedincome'), [
    #         StreamingDataToRefresh(table_class=APXDBvSecurityView, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')]),
    #         StreamingDataToRefresh(table_class=APXRepDBpAPXReadSecurityHashProc, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')]),
    #     ]),
    #     KafkaTopicStreamingDataRefresher(topic=AppConfig().get('kafka_topics', 'apxdb_vrs'), [
    #         StreamingDataToRefresh(table_class=APXDBvSecurityView, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')]),
    #         StreamingDataToRefresh(table_class=APXRepDBpAPXReadSecurityHashProc, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')]),
    #     ]),
    #     KafkaTopicStreamingDataRefresher(topic=AppConfig().get('kafka_topics', 'apxdb_securityproperty'), [
    #         StreamingDataToRefresh(table_class=APXRepDBpAPXReadSecurityHashProc, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')]),
    #     ]),
    #     KafkaTopicStreamingDataRefresher(topic=AppConfig().get('kafka_topics', 'apxdb_fxratehistory'), [
    #         StreamingDataToRefresh(table_class=APXDBvFXRateView, column_mapping=[KafkaToStreamingDataColumnMapping('NumeratorCurrCode'), KafkaToStreamingDataColumnMapping('DenominatorCurrCode'), KafkaToStreamingDataColumnMapping('AsOfDate')]),
    #     ]),
    # ]

    streaming_data_refreshers = {
        AppConfig().get('kafka_topics', 'apxdb_portfolio'): [
            StreamingDataToRefresh(table_class=APXDBvPortfolioView, column_mapping=[KafkaToStreamingDataColumnMapping('PortfolioID')]),
            StreamingDataToRefresh(table_class=APXDBvPortfolioBaseView, column_mapping=[KafkaToStreamingDataColumnMapping('PortfolioID', 'PortfolioBaseID')]),
            StreamingDataToRefresh(table_class=APXDBvPortfolioBaseCustomView, column_mapping=[KafkaToStreamingDataColumnMapping('PortfolioID', 'PortfolioBaseID')]),
            StreamingDataToRefresh(table_class=APXDBvPortfolioBaseSettingExView, column_mapping=[KafkaToStreamingDataColumnMapping('PortfolioID', 'PortfolioBaseID')]),
        ],
        AppConfig().get('kafka_topics', 'apxdb_aoobject'): [
            StreamingDataToRefresh(table_class=APXDBvPortfolioView, column_mapping=[KafkaToStreamingDataColumnMapping('ObjectID', 'PortfolioID')]),
            StreamingDataToRefresh(table_class=APXDBvPortfolioBaseView, column_mapping=[KafkaToStreamingDataColumnMapping('ObjectID', 'PortfolioBaseID')]),
            StreamingDataToRefresh(table_class=APXDBvPortfolioBaseCustomView, column_mapping=[KafkaToStreamingDataColumnMapping('ObjectID', 'PortfolioBaseID')]),
            StreamingDataToRefresh(table_class=APXDBvPortfolioBaseSettingExView, column_mapping=[KafkaToStreamingDataColumnMapping('ObjectID', 'PortfolioBaseID')]),
        ],
        AppConfig().get('kafka_topics', 'apxdb_portfoliobase'): [
            StreamingDataToRefresh(table_class=APXDBvPortfolioBaseView, column_mapping=[KafkaToStreamingDataColumnMapping('PortfolioBaseID')]),
            StreamingDataToRefresh(table_class=APXDBvPortfolioBaseSettingExView, column_mapping=[KafkaToStreamingDataColumnMapping('PortfolioBaseID')]),
        ],
        AppConfig().get('kafka_topics', 'apxdb_portfoliobaseext'): [
            StreamingDataToRefresh(table_class=APXDBvPortfolioBaseCustomView, column_mapping=[KafkaToStreamingDataColumnMapping('PortfolioBaseID')]),
        ],
        AppConfig().get('kafka_topics', 'apxdb_portfoliosetting'): [
            StreamingDataToRefresh(table_class=APXDBvPortfolioBaseSettingExView, column_mapping=[KafkaToStreamingDataColumnMapping('PortfolioSettingID', 'PortfolioBaseID')]),
        ],
        AppConfig().get('kafka_topics', 'apxdb_currency'): [
            StreamingDataToRefresh(table_class=APXDBvCurrencyView, column_mapping=[KafkaToStreamingDataColumnMapping('CurrencyCode')]),
            StreamingDataToRefresh(table_class=APXRepDBpAPXReadSecurityHashProc, column_mapping=[]),  # TODO: optimize this? Need currency param for stored proc?
        ],
        # AppConfig().get('kafka_topics', 'apxdb_privateequity'): [
        #     StreamingDataToRefresh(table_class=APXDBvSecurityView, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')]),
        #     StreamingDataToRefresh(table_class=APXRepDBpAPXReadSecurityHashProc, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')]),
        # ],
        # AppConfig().get('kafka_topics', 'apxdb_securitycontact'): [
        #     StreamingDataToRefresh(table_class=APXDBvSecurityView, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')]),
        #     StreamingDataToRefresh(table_class=APXRepDBpAPXReadSecurityHashProc, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')]),
        # ],
        AppConfig().get('kafka_topics', 'apxdb_securitypropertytoday'): [
            StreamingDataToRefresh(table_class=APXDBvSecurityView, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')]),
            StreamingDataToRefresh(table_class=APXRepDBpAPXReadSecurityHashProc, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')]),
        ],
        AppConfig().get('kafka_topics', 'apxdb_creditrating'): [
            StreamingDataToRefresh(table_class=APXDBvSecurityView, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')]),
            StreamingDataToRefresh(table_class=APXRepDBpAPXReadSecurityHashProc, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')]),
        ],
        AppConfig().get('kafka_topics', 'apxdb_derivedsourcemap'): [
            StreamingDataToRefresh(table_class=APXDBvSecurityView, column_mapping=[KafkaToStreamingDataColumnMapping('SourceLookupID', 'IndustryGroupID')], filter_criteria={'DerivedPropertyID': [-6]}),
            # TODO: do we need to also refresh APXRepDBpAPXReadSecurityHashProc?
        ],
        AppConfig().get('kafka_topics', 'apxdb_dividendrate'): [
            StreamingDataToRefresh(table_class=APXDBvSecurityView, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')]),
            StreamingDataToRefresh(table_class=APXRepDBpAPXReadSecurityHashProc, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')]),
        ],
        AppConfig().get('kafka_topics', 'apxdb_securityassetclasstoday'): [
            StreamingDataToRefresh(table_class=APXDBvSecurityView, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')]),
            StreamingDataToRefresh(table_class=APXRepDBpAPXReadSecurityHashProc, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')]),
        ],
        AppConfig().get('kafka_topics', 'apxdb_securitypropertytoday'): [
            StreamingDataToRefresh(table_class=APXDBvSecurityView, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')], filter_criteria={'PropertyID': [-7, -21, -22, -23]}),
            StreamingDataToRefresh(table_class=APXRepDBpAPXReadSecurityHashProc, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')], filter_criteria={'PropertyID': [-7, -21, -22, -23]}),
        ],
        AppConfig().get('kafka_topics', 'apxdb_security'): [
            StreamingDataToRefresh(table_class=APXDBvSecurityView, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')]),
            StreamingDataToRefresh(table_class=APXRepDBpAPXReadSecurityHashProc, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')]),
        ],
        AppConfig().get('kafka_topics', 'apxdb_aopropertylookup'): [
            StreamingDataToRefresh(table_class=APXDBvSecurityView, column_mapping=[]),  # TODO: optimize this, rather than full view refresh every time?
            StreamingDataToRefresh(table_class=APXRepDBpAPXReadSecurityHashProc, column_mapping=[]),  # TODO: optimize this, rather than full view refresh every time?
        ],
        AppConfig().get('kafka_topics', 'apxdb_aoproperty'): [
            StreamingDataToRefresh(table_class=APXDBvSecurityView, column_mapping=[], filter_criteria={'PropertyName': 'Today'}),
            StreamingDataToRefresh(table_class=APXRepDBpAPXReadSecurityHashProc, column_mapping=[], filter_criteria={'PropertyName': 'Today'}),
        ],
        AppConfig().get('kafka_topics', 'apxdb_fixedincome'): [
            StreamingDataToRefresh(table_class=APXDBvSecurityView, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')]),
            StreamingDataToRefresh(table_class=APXRepDBpAPXReadSecurityHashProc, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')]),
        ],
        AppConfig().get('kafka_topics', 'apxdb_vrs'): [
            StreamingDataToRefresh(table_class=APXDBvSecurityView, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')]),
            StreamingDataToRefresh(table_class=APXRepDBpAPXReadSecurityHashProc, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')]),
        ],
        AppConfig().get('kafka_topics', 'apxdb_securityproperty'): [
            StreamingDataToRefresh(table_class=APXRepDBpAPXReadSecurityHashProc, column_mapping=[KafkaToStreamingDataColumnMapping('SecurityID')]),
        ],
        # AppConfig().get('kafka_topics', 'apxdb_fxratehistory'): [
        #     StreamingDataToRefresh(table_class=APXDBvFXRateView, column_mapping=[KafkaToStreamingDataColumnMapping('NumeratorCurrCode'), KafkaToStreamingDataColumnMapping('DenominatorCurrCode'), KafkaToStreamingDataColumnMapping('AsOfDate')]),
        # ],
    }

    def __init__(self, event_handler: EventHandler, heartbeat_repo: Union[HeartbeatRepository,None]=None):
        """ Creates a KafkaMessageConsumer to consume new/changed apxdb transactions/comments with the provided event handler """
        super().__init__(event_handler=event_handler, heartbeat_repo=heartbeat_repo, topics=[AppConfig().get('kafka_topics', 'apxdb_transaction')])

        # Initialize streaming data
        self.init_streaming_data()

    def init_streaming_data(self):
        self.streaming_data = {}
        self.streaming_data_topics = []
        
        # Find all BaseTables which need to be queried in order to be initialized
        # Also find all topics to consume from for streaming data
        base_table_classes = []
        for k, v in self.streaming_data_refreshers.items():
            self.streaming_data_topics.append(k)
            for data_to_refresh in v:
                base_table_classes.append(data_to_refresh.table_class)

        # Now loop through them and initialize by reading all.
        # By the end, self.streaming_data is a dict where the keys are table classes and values are pd df's
        for table_class in list(set(base_table_classes)):
            logging.info(f'Starting initial read of {table_class.__name__}...')
            self.streaming_data[table_class] = table_class().read()

    def consume(self, reset_offset: bool=False):
        
        logging.info(f'Consuming from topics: {self.topics}')
        logging.info(f'Also consuming from data streaming topics: {self.streaming_data_topics}')

        self.reset_offset = reset_offset
        self.consumer.subscribe(self.topics + self.streaming_data_topics, on_assign=self.on_assign)

        try:
            sleep_secs = int(AppConfig().get('kafka_consumer_lw', 'sleep_seconds', fallback=5))
            while True:
                msg = self.consumer.poll(sleep_secs)
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
                    
                    if msg.topic() in self.streaming_data_topics:
                        try:
                            data = self.deserialize_streaming_data(msg.value())

                            # Now loop through each StreamingDataToRefresh and refresh accordingly:
                            for r in self.streaming_data_refreshers[msg.topic()]:
                                # Check if filter criteria are met
                                for k, v in r.filter_criteria.items():
                                    if data.get(k) not in v:
                                        criteria_not_met_msg = f'{msg.topic()}: Filter criteria {r.filter_criteria} not met by the following data: {data}'
                                        logging.info(criteria_not_met_msg)
                                        raise CriteriaNotMetException(criteria_not_met_msg)
                                
                                # If we made it here, any filter criteria is met
                                # Build params for read method call
                                params = {}
                                for cm in r.column_mapping:
                                    if cm.kafka_msg_column_name in data:
                                        params[cm.streaming_data_column_name] = data[cm.kafka_msg_column_name]
                                    else:
                                        logging.info(f'Column {cm.kafka_msg_column_name} not found from {msg.topic()}! {data}')
                                        # TODO_EH: raise exception?

                                # Now read from the defined table class, providing filter params
                                logging.info(f'About to read {r.table_class.__name__} for {params}...')
                                if len(params):
                                    # Read with params
                                    new_streaming_data = r.table_class().read(**params)

                                    # Delete rows from df 
                                    # TODO_PERF: if performance gain is needed, below may be changed to: 
                                    # delete_rows(self.streaming_data[r.table_class], params)
                                    self.streaming_data[r.table_class] = delete_rows(self.streaming_data[r.table_class], params, in_place=False)

                                    # Now add new data to the streaming data
                                    if len(new_streaming_data):
                                        self.streaming_data[r.table_class] = pd.concat([self.streaming_data[r.table_class], new_streaming_data])

                                else:
                                    # If no params, full overwrite:
                                    self.streaming_data[r.table_class] = r.table_class().read()

                        except Exception as e:
                            logging.info(f'{type(e).__name__} while reading streaming data: {e} {traceback.format_exc()}')
                            logging.info(f'{msg.topic()} msg: {msg.value()}')
                            should_commit = False
                            time.sleep(sleep_secs)
                    
                    if msg.topic() in self.topics:
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
        if isinstance(after, dict):
            for k, v in after.items():
                if 'Date' in k and isinstance(v, int):
                    after[k] = (datetime.date(year=1970, month=1, day=1) + datetime.timedelta(days=v))

        if payload['op'] == 'c':
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
                    if abs(v) < 220898880000:  # TODO: is this a desirable threshold to "guess" if ms or days?
                        before[k] = (datetime.date(year=1970, month=1, day=1) + datetime.timedelta(days=v))
                    else:
                        before[k] = (datetime.date(year=1970, month=1, day=1) + datetime.timedelta(days=v/1000/60/60/24))
        if isinstance(after, dict):
            for k, v in after.items():
                if 'Date' in k and isinstance(v, int):
                    if abs(v) < 220898880000:  # TODO: is this a desirable threshold to "guess" if ms or days?
                        before[k] = (datetime.date(year=1970, month=1, day=1) + datetime.timedelta(days=v))
                    else:
                        before[k] = (datetime.date(year=1970, month=1, day=1) + datetime.timedelta(days=v/1000/60/60/24))
        if payload['op'] == 'd':
            return before
        else:
            return after


