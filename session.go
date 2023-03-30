package session_buntdb

import (
	"encoding/base64"
	"errors"
	"os"
	"path"
	"sync"
	"time"

	"github.com/infrago/session"
	"github.com/tidwall/buntdb"
)

var (
	errInvalidSessionConnection = errors.New("Invalid session connection.")
	errEmptyData                = errors.New("Empty session data.")
)

type (
	buntdbDriver struct {
	}
	buntdbConnect struct {
		mutex sync.RWMutex

		name   string
		config session.Config

		instance *session.Instance
		setting  buntdbSetting

		db *buntdb.DB
	}
	buntdbSetting struct {
		Store string
	}
)

// 连接
func (driver *buntdbDriver) Connect(inst *session.Instance) (session.Connect, error) {
	//获取配置信息
	setting := buntdbSetting{
		Store: "store/session.db",
	}

	//创建目录，如果不存在
	dir := path.Dir(setting.Store)
	_, e := os.Stat(dir)
	if e != nil {
		os.MkdirAll(dir, 0700)
	}

	if vv, ok := inst.Setting["file"].(string); ok && vv != "" {
		setting.Store = vv
	}
	if vv, ok := inst.Setting["store"].(string); ok && vv != "" {
		setting.Store = vv
	}

	return &buntdbConnect{
		instance: inst, setting: setting,
	}, nil
}

// 打开连接
func (this *buntdbConnect) Open() error {
	if this.setting.Store == "" {
		return errors.New("无效会话存储")
	}
	db, err := buntdb.Open(this.setting.Store)
	if err != nil {
		return err
	}
	this.db = db
	return nil
}

// 关闭连接
func (this *buntdbConnect) Close() error {
	if this.db != nil {
		if err := this.db.Close(); err != nil {
			return err
		}
	}
	return nil
}

// 查询会话，
func (this *buntdbConnect) Read(id string) ([]byte, error) {
	if this.db == nil {
		return nil, errInvalidSessionConnection
	}

	value := ""
	err := this.db.View(func(tx *buntdb.Tx) error {
		vvv, err := tx.Get(id)
		if err != nil {
			return err
		}
		value = vvv
		return nil
	})
	if err == buntdb.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	// 使用base64转换
	return base64.StdEncoding.DecodeString(value)
}

// 更新会话
func (this *buntdbConnect) Write(id string, data []byte, expiry time.Duration) error {
	if this.db == nil {
		return errInvalidSessionConnection
	}

	value := base64.StdEncoding.EncodeToString(data)
	if value == "" {
		return errEmptyData
	}

	return this.db.Update(func(tx *buntdb.Tx) error {
		opts := &buntdb.SetOptions{Expires: false}
		if expiry > 0 {
			opts.Expires = true
			opts.TTL = expiry
		}
		_, _, err := tx.Set(id, value, opts)
		return err
	})
}

// 查询会话，
func (this *buntdbConnect) Exists(id string) (bool, error) {
	if this.db == nil {
		return false, errInvalidSessionConnection
	}

	err := this.db.View(func(tx *buntdb.Tx) error {
		_, err := tx.Get(id)
		return err
	})
	if err != nil {
		if err == buntdb.ErrNotFound {
			return true, nil
		}
	}
	return false, nil
}

// 删除会话
func (this *buntdbConnect) Delete(id string) error {
	if this.db == nil {
		return errInvalidSessionConnection
	}

	return this.db.Update(func(tx *buntdb.Tx) error {
		_, err := tx.Delete(id)
		return err
	})
}

func (this *buntdbConnect) Clear(prefix string) error {
	if this.db == nil {
		return errors.New("连接失败")
	}

	ids, err := this.Keys(prefix)
	if err != nil {
		return err
	}

	return this.db.Update(func(tx *buntdb.Tx) error {
		for _, id := range ids {
			_, err := tx.Delete(id)
			if err != nil {
				return err
			}
		}
		return nil
	})
}
func (this *buntdbConnect) Keys(prefix string) ([]string, error) {
	if this.db == nil {
		return nil, errors.New("连接失败")
	}

	ids := []string{}
	err := this.db.View(func(tx *buntdb.Tx) error {
		tx.AscendKeys(prefix+"*", func(k, v string) bool {
			ids = append(ids, k)
			return true
		})

		return nil
	})
	if err != nil {
		return nil, err
	}

	return ids, nil
}
