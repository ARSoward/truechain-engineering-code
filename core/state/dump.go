// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package state

import (
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/log"
	"strconv"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/truechain/truechain-engineering-code/trie"
)

type DumpAccount struct {
	Balance  string            `json:"balance"`
	Nonce    uint64            `json:"nonce"`
	Root     string            `json:"root"`
	CodeHash string            `json:"codeHash"`
	Code     string            `json:"code"`
	Storage  map[string]string `json:"storage"`
}

type Dump struct {
	Root     string                 `json:"root"`
	Accounts map[string]DumpAccount `json:"accounts"`
}

type DumpSize struct {
	count    string `json:"count"`
	size     string `json:"size"`
	sizeByte string `json:"sizeByte"`
	sizeM    string `json:"sizeM"`
}

func (self *StateDB) RawDump() DumpSize {
	it := trie.NewIterator(self.trie.NodeIterator(nil))
	i := 0
	sum := 0
	for it.Next() {
		i++
		addr := self.trie.GetKey(it.Key)
		log.Info("RawDump", "it.Key", len(it.Key), "addr", len(addr), "it.Value", len(it.Value), "i", i, "sum", sum)
		sum = sum + len(it.Key) + len(it.Value)
		var data Account
		if err := rlp.DecodeBytes(it.Value, &data); err != nil {
			panic(err)
		}

		obj := newObject(nil, common.BytesToAddress(addr), data)
		storageIt := trie.NewIterator(obj.getTrie(self.db).NodeIterator(nil))
		storage := 0
		for storageIt.Next() {
			storage = len(storageIt.Key) + len(storageIt.Value)
		}
		sum = sum + storage
		log.Info("RawDump", "i", i, "storage", storage, "sum", sum)
	}
	log.Info("RawDump", "i", i, "sum", sum, "sumB", sum/1024, "sumM", sum/1024/1024)
	return DumpSize{count: strconv.Itoa(i), size: strconv.Itoa(sum), sizeByte: strconv.Itoa(sum / 1024), sizeM: strconv.Itoa((sum / 1024) / 1024)}
}

func (self *StateDB) Dump() []byte {
	json, err := json.MarshalIndent(self.RawDump(), "", "    ")
	if err != nil {
		fmt.Println("dump err", err)
	}

	return json
}
