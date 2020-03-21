package main

import (
    "context"
    "fmt"
    "log"
    "time"

    badger "github.com/dgraph-io/badger"
    kafka "github.com/segmentio/kafka-go"
)

const (
    MEGABYTES = 1000000
    GIGABYTES = 1000000000
)

func main() {
    options := badger.
        DefaultOptions("./badger").
        WithValueLogFileSize(25 * MEGABYTES)

    db, err := badger.Open(options)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    db.RunValueLogGC(0.1)
    go RunGC(db, 0.1, 30)

    RunKafkaConsumer(KafkaReader(), db)
}

func KafkaReader() *kafka.Reader {
    return kafka.NewReader(kafka.ReaderConfig{
        Brokers         : []string{"localhost:9092"},
        GroupID         : "regista",
        Topic           : "order",
        Partition       : 0,
        MinBytes        : 0,
        MaxBytes        : 10e6,
        CommitInterval  : time.Second,
        MaxWait         : 10 * time.Millisecond,
    })
}

func RunKafkaConsumer(r *kafka.Reader, db *badger.DB) {
    for {
        m, err := r.ReadMessage(context.Background())
        if err != nil {
            fmt.Println("kafka read message error:", err)
        }

//        fmt.Printf("message topic/partition/offset %v/%v/%v\n", m.Topic, m.Partition, m.Offset)
        go func(m kafka.Message, db *badger.DB) {
            CommitMessage(m, db)

            ThrowMessage(m)
            DiscardMessage(m, db)
        }(m, db)
    }
}

func CommitMessage(m kafka.Message, db *badger.DB) {
    key := fmt.Sprintf("%v-%v", m.Topic, m.Partition)
    val := fmt.Sprintf("%v", m.Value)

    err := NewEntry(db, []byte(key), []byte(val))
    if err != nil {
        fmt.Println("badger new entry error:", err)
    }
}

func ThrowMessage(m kafka.Message) int {
//    fmt.Println("Throw message: %s\n", string(m.Value))
    return 0
}

func DiscardMessage(m kafka.Message, db *badger.DB) {
    key := fmt.Sprintf("%v-%v", m.Topic, m.Partition)

    err := DeleteEntry(db, []byte(key))
    if err != nil {
        fmt.Println("badger delete entry error:", err)
    }
}

func RunGC(db *badger.DB, discardRatio float64, sec int) {
    ticker := time.NewTicker(time.Duration(sec) * time.Second)
    defer ticker.Stop()

    for range ticker.C {
        sizeLsm, sizeVlog := db.Size()
        if sizeLsm > GIGABYTES || sizeVlog > GIGABYTES {
        again:
            err := db.RunValueLogGC(discardRatio)
            fmt.Printf("GC Report: %v. LSMSize: %v, VLogSize: %v\n", err, sizeLsm, sizeVlog)
            if err == nil{
                goto again
            }
        } else {
            fmt.Println("GC no needed yet", sizeLsm, sizeVlog)
        }
    }
}

func NewEntry(db *badger.DB, key, val []byte) error {
    err := db.Update(func(txn *badger.Txn) error {
        entry := badger.NewEntry(key, val).WithTTL(time.Second)
        err := txn.SetEntry(entry)

        return err
    })

    return err
}

func UpdateEntry(db *badger.DB, key, newVal []byte) error {
    err := db.Update(func(txn *badger.Txn) error {
        return txn.Set(key, newVal)
    })

    return err
}

func DeleteEntry(db *badger.DB, key []byte) error {
    err := db.Update(func(txn *badger.Txn) error {
        return txn.Delete(key)
    })

    return err
}

func GetValue(db *badger.DB, key []byte) ([]byte, error) {
    var valCopy []byte
    err := db.View(func(txn *badger.Txn) error {
        item, err := txn.Get(key)
        if err != nil {
            return err
        }

        valCopy, err = item.ValueCopy(nil)
        if err != nil {
            return err
        }

        return nil
    })

    return valCopy, err
}

