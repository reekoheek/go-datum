package migrate

import (
	"database/sql"
	"errors"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/reekoheek/go-datum"
)

type (
	Callback func(profile *Profile) error

	Profiles struct {
		slice   []*Profile
		index   map[string]int
		context *datum.Context
		db      *sql.DB
		version string
		data    map[string]interface{}
	}

	Profile struct {
		*Profiles
		name      string
		key       string
		upgrade   Callback
		downgrade Callback
	}
)

func New() *Profiles {
	return &Profiles{
		data: make(map[string]interface{}),
	}
}

func (ps *Profiles) Set(key string, value interface{}) *Profiles {
	ps.data[key] = value
	return ps
}

func (ps *Profiles) Interface(key string) interface{} {
	return ps.data[key]
}

func (ps *Profiles) Add(key string, name string, upgrade Callback, downgrade Callback) *Profile {
	profile := &Profile{
		Profiles:  ps,
		key:       key,
		name:      name,
		upgrade:   upgrade,
		downgrade: downgrade,
	}

	if len(ps.index) == 0 {
		ps.index = make(map[string]int)
	}
	i := len(ps.slice)
	ps.index[profile.key] = i
	ps.slice = append(ps.slice, profile)

	return profile
}

func (ps *Profiles) Do(action string, target string) error {
	switch strings.ToLower(action) {
	case "reset":
		return ps.Reset()
	case "refresh":
		return ps.Refresh(target)
	case "up":
		return ps.Up(target)
	case "down":
		return ps.Down(target)
	}

	return errors.New("Unrecognize action " + action)
}

func (ps *Profiles) Refresh(target string) error {
	log.Println("Refreshing state of application...")
	if err := ps.Down(""); err != nil {
		return err
	}

	return ps.Up(target)
}

func (ps *Profiles) Reset() error {
	log.Println("Resetting state of application...")
	return ps.Down("")
}

func (ps *Profiles) Up(target string) error {
	var (
		from int = ps.Index(ps.Version())
		to   int
	)
	switch target {
	case "*", "":
		to = len(ps.slice) - 1
	default:
		to = ps.Index(target)
	}

	for i := from; i < to; i++ {
		next := ps.slice[i+1]
		log.Printf("Upgrading to %s ...", next.key)
		log.Printf("| %s", next.name)
		if err := next.upgrade(next); err != nil {
			return err
		}
		if err := ioutil.WriteFile(".migration-version", []byte(next.key), 0644); err != nil {
			return err
		}
	}

	return nil
}

func (ps *Profiles) Down(target string) error {
	current := ps.Current()
	if ps.GetProfile(target) == current {
		log.Println("Application is uninitialized yet")
		return nil
	}

	var (
		from int = ps.Index(current.key)
		to   int = ps.Index(target)
	)

	for i := from; i > to; i-- {
		cur := ps.slice[i]
		log.Printf("Downgrading from %s ...", cur.key)
		log.Printf("| %s", cur.name)
		if err := cur.downgrade(cur); err != nil {
			return err
		}
		if i > 0 {
			prev := ps.slice[i-1]
			if err := ioutil.WriteFile(".migration-version", []byte(prev.key), 0644); err != nil {
				return err
			}
			ps.version = prev.key
		} else {
			os.Remove(".migration-version")
			ps.version = ""
		}
	}
	return nil
}

func (ps *Profiles) Index(target string) int {
	if target == "" {
		return -1
	}
	return ps.index[target]
}

func (ps *Profiles) Version() string {
	if ps.version != "" {
		return ps.version
	}
	version, err := ioutil.ReadFile(".migration-version")
	if err == nil {
		ps.version = strings.Trim(string(version), "\n\r\t ")
	}
	return ps.version
}

func (ps *Profiles) Current() *Profile {
	return ps.GetProfile(ps.Version())
}

func (ps *Profiles) GetProfile(target string) *Profile {
	if target == "" {
		return nil
	}

	return ps.slice[ps.index[target]]
}
