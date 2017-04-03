package boltstore_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/boltdb/bolt"
	"github.com/eventsource-pkg/boltstore"
	"github.com/eventsource-pkg/eventsource"
	"github.com/stretchr/testify/assert"
)

type User struct {
	Name  string
	Email string
}

func (u *User) On(event eventsource.Event) bool {
	switch v := event.(type) {
	case *UserCreated:
		u.Name = v.Name

	case *UserEmailSet:
		u.Email = v.Email

	default:
		return false
	}

	return true
}

type UserCreated struct {
	eventsource.Model
	Name string
}

func (u UserCreated) New() bool {
	return true
}

type UserEmailSet struct {
	eventsource.Model
	Email string
}

func TestStore(t *testing.T) {
	db, err := bolt.Open("sample.db", 0644, nil)
	assert.Nil(t, err)
	defer db.Close()

	store, err := boltstore.New(db)
	assert.Nil(t, err)

	repo := eventsource.New(&User{}, eventsource.WithStore(store))
	repo.Bind(UserCreated{}, UserEmailSet{})

	ctx := context.Background()
	aggregateID := strconv.FormatInt(time.Now().UnixNano(), 36)
	name := "joe"
	email := "joe@example.com"

	err = repo.Save(ctx,
		UserCreated{
			Model: eventsource.Model{ID: aggregateID, Version: 1, At: time.Now()},
			Name:  name,
		},
		UserEmailSet{
			Model: eventsource.Model{ID: aggregateID, Version: 2, At: time.Now()},
			Email: email,
		},
	)
	assert.Nil(t, err)

	v, err := repo.Load(ctx, aggregateID)
	assert.Nil(t, err)

	user, ok := v.(*User)
	assert.True(t, ok)
	assert.Equal(t, name, user.Name)
	assert.Equal(t, email, user.Email)
}
