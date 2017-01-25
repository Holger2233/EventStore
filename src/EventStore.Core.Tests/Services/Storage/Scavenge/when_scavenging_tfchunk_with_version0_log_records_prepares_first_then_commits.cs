using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Core.Data;
using EventStore.Core.Services.Storage.ReaderIndex;
using EventStore.Core.Tests.Services.Storage;
using EventStore.Core.TransactionLog;
using EventStore.Core.TransactionLog.LogRecords;
using NUnit.Framework;

namespace EventStore.Core.Tests.Services.Storage.Scavenge
{
    [TestFixture]
    public class when_scavenging_tfchunk_with_version0_log_records_prepares_first_then_commits : ReadIndexTestScenario
    {
        private string eventStreamId = "ES";
        private Guid _id1, _id2, _id3;
        private long _pos1, _pos2, _pos3, _pos4, _pos5;

        public when_scavenging_tfchunk_with_version0_log_records_prepares_first_then_commits()
        {
            _rebuildIndexAfterScaveng = true;
        }

        protected override void WriteTestScenario()
        {
            _id1 = Guid.NewGuid();
            _id2 = Guid.NewGuid();
            _id3 = Guid.NewGuid();
            Writer.Write(new PrepareLogRecord(0, _id1, _id1, 0, 0, eventStreamId, 0, DateTime.UtcNow,
                                              PrepareFlags.SingleWrite, "type", new byte[0], new byte[0], LogRecordVersion.LogRecordV0),
                         out _pos1);
            Writer.Write(new PrepareLogRecord(_pos1, _id2, _id2, _pos1, 0, eventStreamId, 1, DateTime.UtcNow,
                                              PrepareFlags.SingleWrite, "type", new byte[0], new byte[0], LogRecordVersion.LogRecordV0),
                         out _pos2);
            Writer.Write(new PrepareLogRecord(_pos2, _id3, _id3, _pos2, 0, eventStreamId, 2, DateTime.UtcNow,
                                              PrepareFlags.SingleWrite, "type", new byte[0], new byte[0], LogRecordVersion.LogRecordV0),
                         out _pos3);
            long pos6;
            Writer.Write(new CommitLogRecord(_pos3, _id1, 0, DateTime.UtcNow, 0, LogRecordVersion.LogRecordV0), out _pos4);
            Writer.Write(new CommitLogRecord(_pos4, _id3, _pos1, DateTime.UtcNow, 2, LogRecordVersion.LogRecordV0), out _pos5);
            Writer.Write(new CommitLogRecord(_pos5, _id2, _pos2, DateTime.UtcNow, 1, LogRecordVersion.LogRecordV0), out pos6);

            Writer.CompleteChunk();

            Scavenge(completeLast: false, mergeChunks: true);
        }

        [Test]
        public void should_be_able_to_read_the_stream()
        {
            var events = ReadIndex.ReadAllEventsForward(new TFPos(0, 0), 100).Records.Select(r => r.Event).ToArray();
            Assert.AreEqual(3, events.Length);
        }

        [Test]
        public void should_be_able_to_read_all_records()
        {
            var result = ReadIndex.ReadAllEventsForward(new TFPos(0,0), 100);
            Assert.AreEqual(3, result.Records.Count());
            Assert.AreEqual(_id1, result.Records[0].Event.EventId);
            Assert.AreEqual(_id2, result.Records[1].Event.EventId);
            Assert.AreEqual(_id3, result.Records[2].Event.EventId);
        }

        [Test]
        public void should_be_able_to_read_record_one()
        {
            var result = ReadIndex.ReadAllEventsForward(new TFPos(_pos3, 0), 1);
            var evnt = result.Records[0].Event;
            Assert.AreEqual(_id1, evnt.EventId);
        }

        [Test]
        public void should_be_able_to_read_record_two()
        {
            var result = ReadIndex.ReadAllEventsForward(new TFPos(_pos4, _pos1), 1);
            var evnt = result.Records[0].Event;
            Assert.AreEqual(_id2, evnt.EventId);
        }

        [Test]
        public void should_be_able_to_read_record_three()
        {
            var result = ReadIndex.ReadAllEventsForward(new TFPos(_pos5, _pos2), 1);
            var evnt = result.Records[0].Event;
            Assert.AreEqual(_id3, evnt.EventId);
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
            Assert.IsTrue(chunkRecords.All(x => x.Version == LogRecordVersion.LogRecordV1));
            Assert.AreEqual(6, chunkRecords.Count);
        }
    }
}