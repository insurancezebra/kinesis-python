import collections
import logging
import multiprocessing
try:
    import Queue
except ImportError:
    # Python 3
    import queue as Queue
import sys
import time
import random

import json
import base64

import boto3
import six

from offspring.process import SubprocessLoop

log = logging.getLogger(__name__)


def sizeof(obj, seen=None):
    """Recursively and fully calculate the size of an object"""
    obj_id = id(obj)
    try:
        if obj_id in seen:
            return 0
    except TypeError:
        seen = set()

    seen.add(obj_id)

    size = sys.getsizeof(obj)

    # since strings are iterabes we return their size explicitly first
    if isinstance(obj, six.string_types):
        return size
    elif isinstance(obj, collections.Mapping):
        return size + sum(
            sizeof(key, seen) + sizeof(val, seen)
            for key, val in six.iteritems(obj)
        )
    elif isinstance(obj, collections.Iterable):
        return size + sum(
            sizeof(item, seen)
            for item in obj
        )

    return size


class AsyncProducer(SubprocessLoop):
    """Async accumulator and producer based on a multiprocessing Queue"""
    # Tell our subprocess loop that we don't want to terminate on shutdown since we want to drain our queue first
    TERMINATE_ON_SHUTDOWN = False

    # Max size & count
    # Per: https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html &
    #      https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html
    #
    # * The maximum size of a data blob (the data payload before base64-encoding) is up to 1 MB.
    # * Each shard can support up to 1,000 records per second for writes, up to a maximum total data write rate of 1 MB
    #   per second (including partition keys).
    # * Each PutRecords request can support up to 500 records, up to a limit of 5MB for the entire request
    MAX_SIZE = (2 ** 20)
    MAX_COUNT = 500
    MAX_RETRY_COUNT = 10

    def __init__(self,
                 stream_name,
                 buffer_time,
                 queue,
                 max_count=None,
                 max_size=None,
                 max_retry_count=None,
                 boto3_session=None,
                 kinesis_endpoint=None):
        self.stream_name = stream_name
        self.buffer_time = buffer_time
        self.queue = queue
        self.records = []
        self.next_records = []
        self.alive = True
        self.ending = False
        self.max_count = max_count or self.MAX_COUNT
        self.max_size = max_size or self.MAX_SIZE
        self.max_retry_count = max_retry_count or self.MAX_RETRY_COUNT

        if boto3_session is None:
            boto3_session = boto3.Session()
        self.client = boto3_session.client('kinesis', endpoint_url=kinesis_endpoint)

        self.start()

    def get_records(self):
        total_size = sum([sizeof(x) for x in self.records])
        records_count = len(self.records)
        timer_start = time.time()

        while self.alive and (self.ending or (time.time() - timer_start) < self.buffer_time):
            # we want our queue to block up until the end of this buffer cycle, so we set out timeout to the amount
            # remaining in buffer_time by substracting how long we spent so far during this cycle
            queue_timeout = self.buffer_time - (time.time() - timer_start)
            try:
                log.debug(dict(msg="Fetching from queue with timeout: %s" % queue_timeout, type='kinesis_queue_fetch'))
                data, explicit_hash_key, partition_key = self.queue.get(block=True, timeout=queue_timeout)
            except Queue.Empty:
                if self.ending:
                    break

                continue

            record = {
                'Data': data,
                'PartitionKey': partition_key or '{0}{1}'.format(time.clock(), time.time()),
            }
            if explicit_hash_key is not None:
                record['ExplicitHashKey'] = explicit_hash_key

            record_size = sizeof(record)
            if record_size > self.max_size:
                log.error(dict(msg='Record exceeded MAX_SIZE (%s)! Dropping record' % self.max_size, type='kinesis_record_too_large'))
                self.log_unsent_data([record]) 
                continue

            total_size += record_size
            if total_size >= self.max_size:
                self.next_records.append(record)
                if not self.ending:
                    log.debug(dict(msg="Records exceed MAX_SIZE (%s)!  Adding to next_records" % self.max_size, type='kinesis_max_size_exceeded'))
                    break

            if records_count == self.max_count:
                # we could get here due to the previous batch of records being equal to the max count
                self.next_records.append(record)
                if not self.ending:
                    log.info(dict(msg="Records have reached MAX_COUNT (%s)!  Flushing records." % self.max_count, type='kinesis_max_count_reached'))
                    break

            self.records.append(record)
            records_count += 1

            if records_count == self.max_count and not self.ending:
                log.info(dict(msg="Records have reached MAX_COUNT (%s)!  Flushing records." % self.max_count, type='kinesis_max_count_reached'))
                break

    def loop(self):
        self.get_records()
        self.flush_records()
        return 0

    def end(self):
        # At the end of our loop (before we exit, i.e. via a signal) we change our buffer time to 1ms and then re-call
        # the loop() method to ensure that we've drained any remaining items from our queue before we exit.
        self.buffer_time = 0.001
        self.ending = True
        self.loop()

        while self.records:
            self.flush_records()

    def send_records(self, records):
        try:
            response = self.client.put_records(
                    StreamName=self.stream_name,
                    Records=records
            )
            failed_record_count = response['FailedRecordCount']

            if failed_record_count > 0:
                log.error(dict(msg='There were %i failures in a batch of %i' % (failed_record_count, len(records)), type='kinesis_send_failure'))
                response_records = response['Records']
                failed_records = []
                for index, record in enumerate(response_records):
                    if 'ErrorCode' in record:
                        failed_records.append(records[index])
                return False, failed_records
            else:
                return True, None
        except Exception:
            log.exception(dict(msg='Could not send records to Kinesis', type='kinesis_send_error'))
            return False, records

    def flush_records(self):
        if self.records:
            log.debug(dict(msg="Flushing %d records" % len(self.records), type='kinesis_batch_flushing'))
            should_send_records = True
            records_to_send = self.records
            retries = 0
            while should_send_records:
                success, failed_records = self.send_records(records_to_send)

                if success:
                    should_send_records = False
                else:
                    if retries >= self.max_retry_count:
                        log.error(dict(msg='Exceeded the maximum number of retries while trying to send records to Kinesis.  Dropping %i records' % len(failed_records), type='kinesis_max_retry_exceeded'))
                        self.log_unsent_data(failed_records)
                        should_send_records = False
                    else:
                        records_to_send = failed_records
                        retries += 1
                        # sleep a random amount so that we do not bombard Kinesis
                        sleep_time = random.random() * 2.0
                        log.debug(dict(msg='Failed sending %i records to Kinesis.  Sleeping for %f seconds' % (len(failed_records), sleep_time), type='kinesis_failure_sleep'))
                        time.sleep(sleep_time)

        self.set_next_batch()

    def set_next_batch(self):
        total_size = 0
        records_count = 0
        batch = []

        while self.next_records:
            record = self.next_records.pop(0)
            record_size = sizeof(record)

            if total_size + record_size >= self.max_size:
                self.next_records.insert(0, record)
                break
            else:
                batch.append(record)
                total_size += record_size
                records_count += 1

            if records_count == self.max_count:
                break

        self.records = batch

    def log_unsent_data(self, unsent_records):
        if unsent_records:
            for record in unsent_records:
                record_data = base64.b64encode(json.dumps(record).encode()).decode()
                log.error(dict(msg='Could not send record to Kinesis before termination', type='unsent_kinesis_data', record_data=record_data))



class KinesisProducer(object):
    """Produce to Kinesis streams via an AsyncProducer"""
    def __init__(self, stream_name, buffer_time=0.5, max_count=None, max_size=None, max_retry_count=None, boto3_session=None, kinesis_endpoint=None):
        self.queue = multiprocessing.Queue()
        self.async_producer = AsyncProducer(stream_name, buffer_time, self.queue, max_count=max_count,
                                            max_size=max_size, max_retry_count=max_retry_count,
                                            boto3_session=boto3_session, kinesis_endpoint=kinesis_endpoint)

    def put(self, data, explicit_hash_key=None, partition_key=None):
        self.queue.put((data, explicit_hash_key, partition_key))
