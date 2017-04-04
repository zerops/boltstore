package boltstore

import (
	"context"
	"fmt"
	"strconv"

	"github.com/boltdb/bolt"
	"github.com/zerops/eventsource"
)

var (
	idBucketName    = []byte("ids")
	eventBucketName = []byte("events")
)

type Store struct {
	db *bolt.DB
}

func New(db *bolt.DB) (*Store, error) {
	err := db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(idBucketName); err != nil {
			return err
		}

		if _, err := tx.CreateBucketIfNotExists(eventBucketName); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return &Store{
		db: db,
	}, nil
}

func (s *Store) Save(ctx context.Context, aggregateID string, records ...eventsource.Record) error {
	if len(records) == 0 {
		return nil
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		version, ok := s.findVersion(tx, aggregateID)
		if !ok {
			version = -1
		}

		// new events may not overlap version numbers
		//
		if version >= records[0].Version {
			return fmt.Errorf("overlapping version, %v, for aggregate, %v", version, aggregateID)
		}

		// store all the events to the events bucket
		//
		bucket := tx.Bucket(eventBucketName)
		for _, record := range records {
			key := makeKey(aggregateID, record.Version)
			err := bucket.Put(key, record.Data)
			if err != nil {
				return err
			}
		}

		// update the version associated with the aggregateID in the id bucket
		//
		highestVersion := records[len(records)-1].Version
		data := []byte(strconv.Itoa(highestVersion))
		return tx.Bucket(idBucketName).Put([]byte(aggregateID), data)
	})
}

func (s *Store) Fetch(ctx context.Context, aggregateID string, version int) (eventsource.History, error) {
	var history eventsource.History

	err := s.db.View(func(tx *bolt.Tx) error {
		v, ok := s.findVersion(tx, aggregateID)
		if !ok {
			history = eventsource.History{}
			return nil
		}

		if version == 0 || v < version {
			version = v
		}

		history = make(eventsource.History, 0, version+1)

		bucket := tx.Bucket(eventBucketName)
		for i := 0; i <= version; i++ {
			key := makeKey(aggregateID, i)
			data := bucket.Get(key)
			if data == nil {
				continue
			}

			record := eventsource.Record{
				Version: i,
				Data:    data,
			}

			history = append(history, record)
		}

		return nil
	})

	return history, err
}

func (s *Store) findVersion(tx *bolt.Tx, aggregateID string) (int, bool) {
	data := tx.Bucket(idBucketName).Get([]byte(aggregateID))
	if version, err := strconv.Atoi(string(data)); err == nil {
		return version, true
	}

	return 0, false
}

func makeKey(aggregateID string, version int) []byte {
	return []byte(strconv.Itoa(version) + ":" + aggregateID)
}
