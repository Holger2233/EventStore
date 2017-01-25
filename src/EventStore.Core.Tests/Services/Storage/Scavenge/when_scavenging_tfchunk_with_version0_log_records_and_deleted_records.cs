using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Core.Data;
using EventStore.Core.TransactionLog;
using EventStore.Core.TransactionLog.LogRecords;
using NUnit.Framework;
using ReadStreamResult = EventStore.Core.Services.Storage.ReaderIndex.ReadStreamResult;

namespace EventStore.Core.Tests.Services.Storage.Scavenge
{
    [TestFixture]
    public class when_scavenging_tfchunk_with_version0_log_records_and_deleted_records : ReadIndexTestScenario
    {
        private string _eventStreamId = "ES";
        private string _deletedEventStreamId = "Deleted-ES";
        private Guid _id1, _id2, _id3, _id4, _deletedId;

        public when_scavenging_tfchunk_with_version0_log_records_and_deleted_records()
        {
            _rebuildIndexAfterScaveng = true;
        }

        protected override void WriteTestScenario()
        {
            _id1 = Guid.NewGuid();
            _id2 = Guid.NewGuid();
            _id3 = Guid.NewGuid();
            _id4 = Guid.NewGuid();
            _deletedId = Guid.NewGuid();

            // Stream that will be kept
            var pos1 = WriteSingelEventWithLogVersion0(_id1, _eventStreamId, 0, 0);
            var pos2 = WriteSingelEventWithLogVersion0(_id2, _eventStreamId, pos1, 1);

            // Stream that will be deleted
            var pos3 = WriteSingelEventWithLogVersion0(Guid.NewGuid(), _deletedEventStreamId, pos2, 0);
            var pos4 = WriteSingelEventWithLogVersion0(Guid.NewGuid(), _deletedEventStreamId, pos3, 1);
            var pos5 = WriteSingelEventWithLogVersion0(_deletedId, _deletedEventStreamId, pos4, int.MaxValue, PrepareFlags.StreamDelete | PrepareFlags.TransactionBegin | PrepareFlags.TransactionEnd);

            // Stream that will be kept
            var pos6 = WriteSingelEventWithLogVersion0(_id3, _eventStreamId, pos5, 2);
            WriteSingelEventWithLogVersion0(_id4, _eventStreamId, pos6, 3);

            Writer.CompleteChunk();

            Scavenge(completeLast: false, mergeChunks: true);
        }

        [Test]
        public void should_be_able_to_read_the_all_stream()
        {
            var events = ReadIndex.ReadAllEventsForward(new TFPos(0, 0), 100).Records.Select(r => r.Event).ToArray();
            Assert.AreEqual(5, events.Count());
            Assert.AreEqual(_id1, events[0].EventId);
            Assert.AreEqual(_id2, events[1].EventId);
            Assert.AreEqual(_deletedId, events[2].EventId);
            Assert.AreEqual(_id3, events[3].EventId);
            Assert.AreEqual(_id4, events[4].EventId);
        }

        [Test]
        public void should_have_updated_deleted_stream_event_number()
        {
            var chunk = Db.Manager.GetChunk(0);
            var chunkRecords = new List<LogRecord>();
            RecordReadResult result = chunk.TryReadFirst();
            while (result.Success)
            {
                chunkRecords.Add(result.LogRecord);
                result = chunk.TryReadClosestForward(result.NextPosition);
            }
            var deletedRecord = (PrepareLogRecord)chunkRecords.First(x=>x.RecordType == LogRecordType.Prepare 
                && ((PrepareLogRecord)x).EventStreamId == _deletedEventStreamId);
            
            Assert.AreEqual(EventNumber.DeletedStream, deletedRecord.ExpectedVersion);
        }

        [Test]
        public void the_new_log_records_are_version_1()
        {
            var chunk = Db.Manager.GetChunk(0);
            var chunkRecords = new List<LogRecord>();
            RecordReadResult result = chunk.TryReadFirst();
            while (result.Success)
            {
                chunkRecords.Add(result.LogRecord);
                result = chunk.TryReadClosestForward(result.NextPosition);
            }
            Assert.IsTrue(chunkRecords.All(x=>x.Version == LogRecordVersion.LogRecordV1));
            Assert.AreEqual(10, chunkRecords.Count);
        }

        [Test]
        public void should_be_able_to_read_the_stream()
        {
            var events = ReadIndex.ReadStreamEventsForward(_eventStreamId, 0, 10);
            Assert.AreEqual(4, events.Records.Length);
            Assert.AreEqual(_id1, events.Records[0].EventId);
            Assert.AreEqual(_id2, events.Records[1].EventId);
            Assert.AreEqual(_id3, events.Records[2].EventId);
            Assert.AreEqual(_id4, events.Records[3].EventId);
        }

        [Test]
        public void the_deleted_stream_should_be_deleted()
        {
            var lastNumber = ReadIndex.GetStreamLastEventNumber(_deletedEventStreamId);
            Assert.AreEqual(EventNumber.DeletedStream, lastNumber);
        }

    }
}