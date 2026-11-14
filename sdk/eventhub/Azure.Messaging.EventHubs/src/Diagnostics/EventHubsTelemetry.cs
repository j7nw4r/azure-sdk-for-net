// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Threading;
using Azure.Core.Pipeline;
using Azure.Core.Shared;

namespace Azure.Messaging.EventHubs.Diagnostics
{
    /// <summary>Optional instrumentation for logical operations and AMQP component lifetimes.</summary>
    internal sealed class EventHubsTelemetry
    {
        internal const string MeterName = "Azure.Messaging.EventHubs";
        private static readonly Meter Meter = new Meter(MeterName);
        private static readonly Counter<long> Operations = Meter.CreateCounter<long>("az.eventhubs.operations", "{operation}", "Completed logical send and receive operations.");
        private static readonly Counter<long> Events = Meter.CreateCounter<long>("az.eventhubs.events", "{event}", "Events in accepted sends and successful receives.");
        private static readonly Histogram<double> Duration = Meter.CreateHistogram<double>(
            "az.eventhubs.operation.duration", "s", "Logical operation duration, excluding buffered batch-fill waits and callbacks.",
            advice: new InstrumentAdvice<double>
            {
                HistogramBucketBoundaries = new double[] { 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60 }
            });
        private static readonly Counter<long> Lifecycle = Meter.CreateCounter<long>("az.eventhubs.amqp.lifecycle", "{transition}", "Completed AMQP creation attempts and connection invalidations.");
        private static readonly AsyncLocal<Operation> BufferedSend = new AsyncLocal<Operation>();
        private readonly MessagingClientDiagnostics _diagnostics;
        private readonly string _host;
        private readonly string _eventHub;
        private readonly string _identifier = Guid.NewGuid().ToString("N");
        private long _generation;

        internal EventHubsTelemetry(string host, string eventHub)
        {
            _host = host;
            _eventHub = eventHub;
            _diagnostics = new MessagingClientDiagnostics(MeterName, DiagnosticProperty.ResourceProviderNamespace, DiagnosticProperty.EventHubsServiceContext, host, eventHub);
        }

        internal Operation StartOperation(string operation, string activityName = null, bool buffered = false)
        {
            // The buffered worker owns the logical send across preparation and throttling retries.
            if (operation == "send" && !buffered && BufferedSend.Value != null)
            {
                return null;
            }

            var scope = activityName == null ? default : _diagnostics.CreateScope(activityName, ActivityKind.Client,
                operation == "send" ? MessagingDiagnosticOperation.Publish : MessagingDiagnosticOperation.Receive);
            if (!scope.IsEnabled && !Operations.Enabled && !Events.Enabled && !Duration.Enabled)
            {
                return null;
            }
            scope.Start();
            var result = new Operation(this, scope, operation);
            if (buffered)
            {
                BufferedSend.Value = result;
            }
            return result;
        }

        internal Operation StartAmqp(string activityName)
        {
            var scope = _diagnostics.CreateScope(activityName, ActivityKind.Client);
            if (!scope.IsEnabled)
            {
                return null;
            }
            scope.Start();
            return new Operation(this, scope, null);
        }

        internal Operation StartLifecycle(string component)
        {
            var generation = Interlocked.Increment(ref _generation);
            var scope = _diagnostics.CreateScope("EventHubs.amqp." + component + ".open", ActivityKind.Client);
            if (!scope.IsEnabled && !Lifecycle.Enabled && !EventHubsEventSource.Log.IsEnabled())
            {
                return null;
            }
            scope.AddAttribute("az.eventhubs.client_id", _identifier);
            scope.AddLongAttribute("az.eventhubs.generation", generation);
            scope.Start();
            RecordLifecycle(component, generation, "started");
            return new Operation(this, scope, null, component, generation);
        }

        private void RecordLifecycle(string component, long generation, string outcome)
        {
            EventHubsEventSource.Log.AmqpLifecycle(_host, _eventHub, _identifier, component, generation, outcome);
            if (outcome != "started")
            {
                var tags = new TagList { { "server.address", _host }, { "messaging.destination.name", _eventHub }, { "component", component }, { "outcome", outcome } };
                Lifecycle.Add(1, tags);
            }
        }

        /// <summary>A single completion, independent of transport retry attempts.</summary>
        internal sealed class Operation : IDisposable
        {
            private readonly EventHubsTelemetry _owner;
            private readonly DiagnosticScope _scope;
            private readonly string _operation;
            private readonly string _component;
            private readonly long _generation;
            private long _started = Stopwatch.GetTimestamp();
            private double _elapsed;
            private bool _paused;
            private bool _completed;
            private int _invalidated;

            internal Operation(EventHubsTelemetry owner, DiagnosticScope scope, string operation, string component = null, long generation = 0)
            {
                _owner = owner;
                _scope = scope;
                _operation = operation;
                _component = component;
                _generation = generation;
            }

            internal void Pause()
            {
                if (!_paused)
                {
                    _elapsed += (Stopwatch.GetTimestamp() - _started) / (double)Stopwatch.Frequency;
                    _paused = true;
                }
            }

            internal void Resume()
            {
                _started = Stopwatch.GetTimestamp();
                _paused = false;
            }

            internal void Discard() => Finish("success", 0, false);

            internal void Complete(int eventCount = 0) => Finish("success", eventCount);

            internal void Failed(Exception exception)
            {
                if (_completed)
                {
                    return;
                }
                // Broker descriptions, exception messages, and message contents are never added to these spans.
                _scope.Failed(exception.GetType().FullName);
                Finish(exception is OperationCanceledException ? "cancelled" : "error", 0);
            }

            internal void Invalidate()
            {
                if (Interlocked.Exchange(ref _invalidated, 1) == 0)
                {
                    _owner.RecordLifecycle(_component, _generation, "invalidated");
                }
            }

            public void Dispose()
            {
                if (!_completed)
                {
                    _scope.Failed("cancelled");
                    Finish("cancelled", 0);
                }
            }

            private void Finish(string outcome, int eventCount, bool record = true)
            {
                if (_completed)
                {
                    return;
                }
                _completed = true;
                Pause();
                if (BufferedSend.Value == this)
                {
                    BufferedSend.Value = null;
                }
                _scope.AddAttribute("az.eventhubs.outcome", outcome);
                if (_operation != null && record)
                {
                    _scope.AddIntegerAttribute(MessagingClientDiagnostics.BatchCount, eventCount);
                    var tags = new TagList
                    {
                        { "server.address", _owner._host },
                        { "messaging.destination.name", _owner._eventHub },
                        { "messaging.operation", _operation },
                        { "outcome", outcome }
                    };
                    Operations.Add(1, tags);
                    Duration.Record(_elapsed, tags);
                    if (eventCount > 0)
                    {
                        Events.Add(eventCount, tags);
                    }
                }
                if (_component != null)
                {
                    _owner.RecordLifecycle(_component, _generation, outcome);
                }
                _scope.Dispose();
            }
        }
    }
}
