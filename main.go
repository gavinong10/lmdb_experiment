package main

import (
	"encoding/binary"
	"fmt"

	"code.storreduce.com/dcss/database"
	"code.storreduce.com/dcss/mdbi"
	mdb "github.com/hughe/gomdb"
)

type tableBase struct {
	handle mdb.DBI
}

type MyTable struct {
	tableBase
}

func (t *MyTable) putInt64(txn mdbi.Txn, name string, val int64) (err error) {
	data := make([]byte, binary.MaxVarintLen64)
	off := binary.PutVarint(data, val)
	data = data[:off]

	return t.putBytes(txn, name, data)
}

func (t *MyTable) getInt64(txn mdbi.Txn, name string) (val int64, err error) {
	var data []byte
	if data, err = t.getBytes(txn, name); err != nil {
		return
	}
	val, n := binary.Varint(data)
	if n == 0 {
		err = database.DecodeError
		return
	}
	return
}

func (t *MyTable) getBytes(txn mdbi.Txn, name string) (val []byte, err error) {
	return txn.Get(t.handle, []byte(name))
}

func (t *MyTable) putBytes(txn mdbi.Txn, name string, val []byte) (err error) {
	return txn.Put(t.handle, []byte(name), val, 0)
}

func main() {
	setMaxes := func(dbEnv mdbi.Env) error {
		var err error
		if err := dbEnv.SetMaxDBs(10); err != nil {
			fmt.Println("db", err)
			return err
		}

		if err := dbEnv.SetMapSize(uint64(10)); err != nil {
			fmt.Println("map", err)
			return err
		}
		return err
	}
	//blk
	blkEnv, err := mdbi.NewRealEnv()
	var openFlags uint = mdb.NORDAHEAD
	dirName := "/Users/macbook/deleteme"
	if err = blkEnv.Open(dirName, openFlags, 0600); err != nil {
		fmt.Printf("MDB Open %q %x returned %q (%#v)", dirName, openFlags, err.Error(), err)
		// return nil, err
	}
	setMaxes(blkEnv)
	//key
	keyEnv, err := mdbi.NewRealEnv()
	if err = keyEnv.Open(dirName, openFlags, 0600); err != nil {
		fmt.Printf("MDB Open %q %x returned %q (%#v)", dirName, openFlags, err.Error(), err)
		// return nil, err
	}
	setMaxes(keyEnv)
	db := database.NewDatabase(blkEnv, keyEnv)

	// // With a database lock, for blk,
	// // with a transaction, -> openTopTable, openNameLocTables
	// if err = DB.Open(); err != nil {
	// 	fmt.Printf("error")
	// }
	var myTable *MyTable
	db.WithLock(func() error {
		return db.Blk.WithTxn(func(txn database.BlkTxn) error {

			myTableHandle, err := database.CreateOrOpenTable(txn, "myTable", 0)
			if err != nil {
				fmt.Println("Error1!", err)
				return err
			}
			// blk.FileNoVersion = &FileNoVersionTable{tableBase{fileNoVersionHandle}
			myTable = &MyTable{tableBase{myTableHandle}}

			// fmt.Println(myTable)
			return nil
		})
	})
	db.WithLock(func() error {
		return db.Blk.WithTxn(func(txn database.BlkTxn) error {
			myTableHandle, err := database.CreateOrOpenTable(txn, "myTable", 0)
			if err != nil {
				fmt.Println("Error2!", err)
				return err
			}
			// blk.FileNoVersion = &FileNoVersionTable{tableBase{fileNoVersionHandle}
			myTable = &MyTable{tableBase{myTableHandle}}
			if err = myTable.putInt64(txn, "myString", 5); err != nil {
				fmt.Println(err)
			}

			return nil
		})
	})

	db.WithLock(func() error {
		return db.Blk.WithTxn(func(txn database.BlkTxn) error {
			// Can try reading some stuff from here
			myTableHandle, err := database.CreateOrOpenTable(txn, "myTable", 0)
			if err != nil {
				fmt.Println("Error3!", err)
				return err
			}
			// blk.FileNoVersion = &FileNoVersionTable{tableBase{fileNoVersionHandle}
			myTable = &MyTable{tableBase{myTableHandle}}

			val, err := myTable.getInt64(txn, "myString")
			if err != nil {
				fmt.Println(err)
			} else {
				fmt.Println("The value is: ", val)
			}

			return nil
		})
	})

	// time.Sleep(3 * time.Second)

}

// ??? WHAT IS THE TOP TABLE??
// Why do the name2loc and loc2name tables have multiple references?
// e.g. prefixed with name2loc-%d; something to do with garbage collection?
// What is "db.upgradeDBAfterSplit()" for?
