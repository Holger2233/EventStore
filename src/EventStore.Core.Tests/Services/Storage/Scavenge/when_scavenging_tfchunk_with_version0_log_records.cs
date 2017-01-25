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
    public class when_scavenging_tfchunk_with_version0_log_records : ReadIndexTestScenario
    {
        private string _eventStreamId = "ES";
        private Guid _id1, _id2, _id3;
        private long _pos1, _pos2;

        protected override void WriteTestScenario()
        {
            _id1 = Guid.NewGuid();
            _id2 = Guid.NewGuid();
            _id3 = Guid.NewGuid();
            _pos1 = WriteSingelEventWithLogVersion0(_id1, _eventStreamId, 0, 0);
            _pos2 = WriteSingelEventWithLogVersion0(_id2, _eventStreamId, _pos1, 1);
            WriteSingelEventWithLogVersion0(_id3, _eventStreamId, _pos2, 2);

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
        public void should_be_able_to_read_record_one()
        {
            var result = ReadIndex.ReadAllEventsForward(new TFPos(0, 0), 1);
            var evnt = result.Records[0].Event;
            Assert.AreEqual(_id1, evnt.EventId);
        }

        [Test]
        public void should_be_able_to_read_record_two()
        {
            var result = ReadIndex.ReadAllEventsForward(new TFPos(_pos1 + 8, _pos1 + 8), 1);
            var evnt = result.Records[0].Event;
            Assert.AreEqual(_id2, evnt.EventId);
        }


        [Test]
        public void should_be_able_to_read_record_three()
        {
            var result = ReadIndex.ReadAllEventsForward(new TFPos(_pos2 + 16, _pos2 + 16), 1);
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
                result = chunk.TryReadClosestForward((int)result.NextPosition);
            }
            Assert.IsTrue(chunkRecords.All(x=>x.Version == LogRecordVersion.LogRecordV1));
            Assert.AreEqual(6, chunkRecords.Count);
        }
    }
}
