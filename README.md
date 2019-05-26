# NuKafka

A Kafka client, but ++

**Warning: This is an experimental library.**

## Background

There is a situation when we would like to implement retry mechanism in Kafka. Requeue the message usually solved the problem, however things get harder when we want to give the interval (i.e: fixed or with backoff exp) while consuming message.

In this library, we created a protocol/mechanism to make the retry with interval possible by using a simple debouncing mechanism such as:

1. In background, the message is retry like a normal message. However, we pass additional information to the retry message.
2. The consumer will read the custom wrapper message which contain the expected time where the message can be process.
3. If the current time (`time.Now()`) is exceeding the expected time in message, then, the message will be proceed as a valid message.
4. Else, the consumer will republish the wrapped message, to specific retry topic `retry_[original_topic]_[group_id]`
5. Both `original_topic` and `retry_topic` listens to the same handler

## How to use

A. Publisher

```go
p := nukafka.NewTopicPublisherCtx(sctx, topic, &nukafka.PubConfig{
    Brokers:     []string{
        "localhost:9092",
    },
    Protocol:    "tcp",
    SendTimeout: time.Second,
    DialTimeout: time.Second * 10,
})

//send data
data := Data{
    Name: "xxx",
    Age: 28,
}
go p.SendCtx(ctx, data)
```

B. Consumer

```go
var retry = true
config := &nukafka.SubConfig{
    Brokers:     []string{
        "localhost:9092",
    },
    Protocol:    "tcp",
    Offset:      42,
    GroupID:     "my-group-id",
    MaxInFlight: 200,
    MinBytes:    0,
    MaxBytes:    1000000, //10Mb
    DialTimeout: time.Second * 5,
}

//create consumer
s := nukafka.NewTopicConsumerCtx(context.Background(), topic, retry, config)
s.RegisterFunc(consume)
```

```go
func consume(ctx context.Context, message nukafka.Message) {
    log.Debugln("message", string(message.Value))
    if message.RetryAttempt < 10 {
        log.Infoln("pending for 1 seconds ret attempt", message.RetryAttempt)
        message.ProcessAferDelaySecondsCtx(1)
    }
}
```

Sample Output:

```text
DEBU[0033] message {"topic":"kafka.do_something","name":"Faris","age":28}
INFO[0033] pending for 1 seconds ret attempt 0
DEBU[0035] message {"age":28,"name":"Faris","topic":"kafka.do_something"}
INFO[0035] pending for 1 seconds ret attempt 1
DEBU[0037] message {"age":28,"name":"Faris","topic":"kafka.do_something"}
INFO[0037] pending for 1 seconds ret attempt 2
DEBU[0038] message {"age":28,"name":"Faris","topic":"kafka.do_something"}
INFO[0038] pending for 1 seconds ret attempt 3
DEBU[0040] message {"age":28,"name":"Faris","topic":"kafka.do_something"}
INFO[0040] pending for 1 seconds ret attempt 4
DEBU[0041] message {"age":28,"name":"Faris","topic":"kafka.do_something"}
INFO[0041] pending for 1 seconds ret attempt 5
DEBU[0043] message {"age":28,"name":"Faris","topic":"kafka.do_something"}
INFO[0043] pending for 1 seconds ret attempt 6
DEBU[0044] message {"age":28,"name":"Faris","topic":"kafka.do_something"}
INFO[0044] pending for 1 seconds ret attempt 7
DEBU[0046] message {"age":28,"name":"Faris","topic":"kafka.do_something"}
INFO[0046] pending for 1 seconds ret attempt 8
DEBU[0047] message {"age":28,"name":"Faris","topic":"kafka.do_something"}
INFO[0047] pending for 1 seconds ret attempt 9
DEBU[0049] message {"age":28,"name":"Faris","topic":"kafka.do_something"}
```