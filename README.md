# boltstore

bolt implementation of eventsource.Store

## Using the bolt store

```go
db, _ := bolt.Open("sample.db", 0644, nil)
defer db.Close()

store, _ := boltstore.New(db)

repo := eventsource.New(&User{}, eventsource.WithStore(store))
```
