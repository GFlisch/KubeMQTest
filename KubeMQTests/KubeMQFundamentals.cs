using KubeMQ.Grpc;
using KubeMQ.SDK.csharp.QueueStream;
using KubeMQTests.XUnit;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace KubeMQTests
{
    public class KubeMQFundamentals : IClassFixture<LoggerFixture>
    {
        protected readonly ITestOutputHelper _output;

        public KubeMQFundamentals(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public async Task TestBasicSendAndReceive()
        {
            QueueStream queue = new QueueStream("localhost:50000", "Demo.Test", null);

            var messages = new List<Message>();
            var tags = new Dictionary<string, string>();
            tags.Add("key", "value");

            for (int i = 0; i < 1000; i++)
                messages.Add(
                    new Message("Order.Test.OutQ", Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()), string.Empty, Guid.NewGuid().ToString(), tags)
                );

            var res = queue.Send(new SendRequest(messages));

            Assert.False(res.IsFaulted);


            PollRequest pollRequest = new()
            {
                Queue = "Order.Test.OutQ",
                WaitTimeout = 1000,
                MaxItems = 100,
                AutoAck = false
            };

            var read = 0;

            while (read < 1000)
            {
                PollResponse response = await queue.Poll(pollRequest);

                if (!response.HasMessages) continue;

                _output.WriteLine($"{response.Messages.Count()} message(s) received from the PollResponse.");

                Parallel.ForEach(response.Messages, msg =>
                {
                    try
                    {
                        Assert.Equal(1, msg.Tags.Count());

                        Assert.True(Guid.TryParse(Encoding.UTF8.GetString(msg.Body), out var guid));

                        msg.Ack();

                    }
                    catch (Exception)
                    {
                        msg.NAck();
                    }

                });

                read += response.Messages.Count();
            }

            Assert.Equal(1000, read);

        }


        [Fact]
        public async Task TestBasicSendWithDelayAndReceive()
        {
            QueueStream queue = new QueueStream("localhost:50000", "Demo.Test", null);

            var messages = new List<Message>();
            var tags = new Dictionary<string, string>();
            tags.Add("key", "value");

            for (int i = 0; i < 1000; i++)
                messages.Add(
                    new Message("Order.Test.OutQ", Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()), string.Empty, Guid.NewGuid().ToString(), tags)
                    {
                        Policy = new QueueMessagePolicy() { DelaySeconds = 11 }
                    }
                );

            var res = queue.Send(new SendRequest(messages));

            Assert.False(res.IsFaulted);


            PollRequest pollRequest = new()
            {
                Queue = "Order.Test.OutQ",
                WaitTimeout = 1000,
                MaxItems = 100,
                AutoAck = false
            };

            var read = 0;
            var waitCount = 0;

            while (read < 1000)
            {
                PollResponse response = await queue.Poll(pollRequest);

                if (!response.HasMessages)
                {
                    waitCount++;
                    continue;
                }

                _output.WriteLine($"{response.Messages.Count()} message(s) received from the PollResponse.");

                Parallel.ForEach(response.Messages, msg =>
                {
                    try
                    {
                        Assert.Equal(1, msg.Tags.Count());

                        Assert.True(Guid.TryParse(Encoding.UTF8.GetString(msg.Body), out var guid));



                    }
                    catch (Exception)
                    {
                        msg.NAck();
                    }

                });

                response.AckAll();

                read += response.Messages.Count();
            }

            Assert.Equal(1000, read);
            Assert.True(waitCount > 9, $"Counted = {waitCount}");
        }


        private static long touched;

        [Fact]
        public async Task TestIisHostReceiver()
        {
            touched = 0;
            CancellationTokenSource tokenSource = new CancellationTokenSource();

            // Start the listener on another thread.
            await Task.Factory.StartNew(Listener2, tokenSource.Token, tokenSource.Token);

            var stopwatch = new Stopwatch();

            Assert.Equal(0, Interlocked.Read(ref touched));

            var messages = new List<Message>();
            var tags = new Dictionary<string, string>();
            tags.Add("key", "value");

            for (int i = 0; i < 1000; i++)
                messages.Add(
                    new Message("Order.Test.OutQ", Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()), string.Empty, Guid.NewGuid().ToString(), tags)
                );

            stopwatch.Start();

            QueueStream queue = new QueueStream("localhost:50000", "Demo.Test", null);
            var res = queue.Send(new SendRequest(messages));

            stopwatch.Stop();

            _output.WriteLine($"Messages sent in {stopwatch.ElapsedMilliseconds} ms.");

            stopwatch.Reset();
            stopwatch.Start();

            Assert.False(res.IsFaulted);

            do
            {
                await Task.Delay(10);
            } while (Interlocked.Read(ref touched) < 1000 && stopwatch.ElapsedMilliseconds < 10000);

            stopwatch.Stop();

            _output.WriteLine($"Messages read in {stopwatch.ElapsedMilliseconds} ms.");

            tokenSource.Cancel();

            await Task.Delay(150);

            Assert.Equal(1000, Interlocked.Read(ref touched));

        }

        private async Task Listener2(object parameter)
        {
            CancellationToken cancellationToken = (CancellationToken)parameter;

            QueueStream queue = new QueueStream("localhost:50000", "Demo.Test", null);

            PollRequest pollRequest = new()
            {
                Queue = "Order.Test.OutQ",
                WaitTimeout = 1000,
                MaxItems = 100,
                AutoAck = false
            };

            var messagesToAck = new ConcurrentBag<Message>();
            do
            {
                PollResponse response = await queue.Poll(pollRequest);

                if (!response.HasMessages) continue;

                _output.WriteLine($"{response.Messages.Count()} message(s) received from the PollResponse.");

                Parallel.ForEach(response.Messages, async msg =>
                {
                    try
                    {
                        Assert.Equal(1, msg.Tags.Count());

                        Assert.True(Guid.TryParse(Encoding.UTF8.GetString(msg.Body), out var guid));

                        Interlocked.Increment(ref touched);

                        await Task.Delay(100);

                        msg.Ack();
                    }
                    catch (Exception)
                    {
                        msg.NAck();
                    }
                });

            } while (!cancellationToken.IsCancellationRequested);


        }
    }
}
