package session_buntdb

import (
	"github.com/infrago/infra"
	"github.com/infrago/session"
)

func Driver() session.Driver {
	return &buntdbDriver{}
}

func init() {
	infra.Register("buntdb", Driver())
}
