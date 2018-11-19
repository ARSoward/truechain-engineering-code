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

package etrue

import (
	"fmt"
	"io"
	"math/big"

	"github.com/truechain/truechain-engineering-code/common"
	"github.com/truechain/truechain-engineering-code/core/types"
	"github.com/truechain/truechain-engineering-code/event"
	"github.com/truechain/truechain-engineering-code/rlp"
)

// Constants to match up protocol versions and messages
const (
	eth62 = 62
	eth63 = 63
)

// ProtocolName is the official short name of the protocol used during capability negotiation.
var ProtocolName = "etrue"

// ProtocolVersions are the upported versions of the etrue protocol (first is primary).
var ProtocolVersions = []uint{eth63, eth62}

// ProtocolLengths are the number of implemented message corresponding to different protocol versions.
var ProtocolLengths = []uint64{27, 8}

const ProtocolMaxMsgSize = 10 * 1024 * 1024 // Maximum cap on the size of a protocol message

// etrue protocol message codes
const (
	// Protocol messages belonging to eth/62
	StatusMsg              = 0x00
	NewFastBlockHashesMsg  = 0x01
	TxMsg                  = 0x02
	GetFastBlockHeadersMsg = 0x03
	FastBlockHeadersMsg    = 0x04
	GetFastBlockBodiesMsg  = 0x05
	FastBlockBodiesMsg     = 0x06
	NewFastBlockMsg        = 0x07

	BlockSignMsg    = 0x08
	PbftNodeInfoMsg = 0x09

	FruitMsg      = 0x0a
	SnailBlockMsg = 0x0b
	// Protocol messages belonging to eth/63
	GetNodeDataMsg = 0x0d
	NodeDataMsg    = 0x0e
	GetReceiptsMsg = 0x0f
	ReceiptsMsg    = 0x10

	//snail sync
	GetSnailBlockHeadersMsg   = 0x11
	SnailBlockHeadersMsg      = 0x12
	GetSnailBlockBodiesMsg    = 0x13
	SnailBlockBodiesMsg       = 0x14
	GetFastOneBlockHeadersMsg = 0x15
	FastOneBlockHeadersMsg    = 0x16
)

type errCode int

const (
	ErrMsgTooLarge              = iota
	ErrDecode
	ErrInvalidMsgCode
	ErrProtocolVersionMismatch
	ErrNetworkIdMismatch
	ErrGenesisBlockMismatch
	ErrFastGenesisBlockMismatch
	ErrNoStatusMsg
	ErrExtraStatusMsg
	ErrSuspendedPeer
)

func (e errCode) String() string {
	return errorToString[int(e)]
}

// XXX change once legacy code is out
var errorToString = map[int]string{
	ErrMsgTooLarge:              "Message too long",
	ErrDecode:                   "Invalid message",
	ErrInvalidMsgCode:           "Invalid message code",
	ErrProtocolVersionMismatch:  "Protocol version mismatch",
	ErrNetworkIdMismatch:        "NetworkId mismatch",
	ErrGenesisBlockMismatch:     "Genesis block mismatch",
	ErrFastGenesisBlockMismatch: "Fast Genesis block mismatch",
	ErrNoStatusMsg:              "No status message",
	ErrExtraStatusMsg:           "Extra status message",
	ErrSuspendedPeer:            "Suspended peer",
}

type txPool interface {
	// AddRemotes should add the given transactions to the pool.
	AddRemotes([]*types.Transaction) []error

	// Pending should return pending transactions.
	// The slice should be modifiable by the caller.
	Pending() (map[common.Address]types.Transactions, error)

	// SubscribeNewTxsEvent should return an event subscription of
	// NewTxsEvent and send events to the given channel.
	SubscribeNewTxsEvent(chan<- types.NewTxsEvent) event.Subscription
	// for fruits and records
	//SubscribeNewFruitsEvent(chan<- types.NewFruitsEvent) event.Subscription
}

type SnailPool interface {
	AddRemoteFruits([]*types.SnailBlock) []error
	//AddRemoteSnailBlocks([]*types.SnailBlock) []error
	PendingFruits() map[common.Hash]*types.SnailBlock
	SubscribeNewFruitEvent(chan<- types.NewFruitsEvent) event.Subscription
	//SubscribeNewSnailBlockEvent(chan<- core.NewSnailBlocksEvent) event.Subscription
	//AddRemoteRecords([]*types.PbftRecord) []error
	//AddRemoteRecords([]*types.PbftRecord) []error
	//SubscribeNewRecordEvent(chan<- core.NewRecordsEvent) event.Subscription

	RemovePendingFruitByFastHash(fasthash common.Hash)
}

type AgentNetworkProxy interface {
	// SubscribeNewFastBlockEvent should return an event subscription of
	// NewBlockEvent and send events to the given channel.
	SubscribeNewFastBlockEvent(chan<- types.NewBlockEvent) event.Subscription
	// SubscribeNewPbftSignEvent should return an event subscription of
	// PbftSignEvent and send events to the given channel.
	SubscribeNewPbftSignEvent(chan<- types.PbftSignEvent) event.Subscription
	// SubscribeNodeInfoEvent should return an event subscription of
	// NodeInfoEvent and send events to the given channel.
	SubscribeNodeInfoEvent(chan<- types.NodeInfoEvent) event.Subscription
	// AddRemoteNodeInfo should add the given NodeInfo to the pbft agent.
	AddRemoteNodeInfo(*types.EncryptNodeMessage) error
}

// statusData is the network packet for the status message.
type statusData struct {
	ProtocolVersion  uint32
	NetworkId        uint64
	TD               *big.Int
	CurrentBlock     common.Hash
	GenesisBlock     common.Hash
	CurrentFastBlock common.Hash
	GenesisFastBlock common.Hash
}

// newBlockHashesData is the network packet for the block announcements.
type newBlockHashesData []struct {
	Hash   common.Hash // Hash of one particular block being announced
	Number uint64      // Number of one particular block being announced
	Sign   *types.PbftSign
}

// getBlockHeadersData represents a block header query.
type getBlockHeadersData struct {
	Origin  hashOrNumber // Block from which to retrieve headers
	Amount  uint64       // Maximum number of headers to retrieve
	Skip    uint64       // Blocks to skip between consecutive headers
	Reverse bool         // Query direction (false = rising towards latest, true = falling towards genesis)
}

// hashOrNumber is a combined field for specifying an origin block.
type hashOrNumber struct {
	Hash   common.Hash // Block hash from which to retrieve headers (excludes Number)
	Number uint64      // Block hash from which to retrieve headers (excludes Hash)
}

// EncodeRLP is a specialized encoder for hashOrNumber to encode only one of the
// two contained union fields.
func (hn *hashOrNumber) EncodeRLP(w io.Writer) error {
	if hn.Hash == (common.Hash{}) {
		return rlp.Encode(w, hn.Number)
	}
	if hn.Number != 0 {
		return fmt.Errorf("both origin hash (%x) and number (%d) provided", hn.Hash, hn.Number)
	}
	return rlp.Encode(w, hn.Hash)
}

// DecodeRLP is a specialized decoder for hashOrNumber to decode the contents
// into either a block hash or a block number.
func (hn *hashOrNumber) DecodeRLP(s *rlp.Stream) error {
	_, size, _ := s.Kind()
	origin, err := s.Raw()
	if err == nil {
		switch {
		case size == 32:
			err = rlp.DecodeBytes(origin, &hn.Hash)
		case size <= 8:
			err = rlp.DecodeBytes(origin, &hn.Number)
		default:
			err = fmt.Errorf("invalid input size %d for origin", size)
		}
	}
	return err
}

// newFastBlockData is the network packet for the block propagation message.
type newBlockData struct {
	Block *types.Block
}

// newFastBlockData is the network packet for the block propagation message.
type newSnailBlockData struct {
	Block *types.SnailBlock
	TD    *big.Int
}

// blockBody represents the data content of a single block.
type blockBody struct {
	Transactions []*types.Transaction // Transactions contained within a block
	Signs        []*types.PbftSign    // Signs contained within a block
}

// blockBodiesData is the network packet for block content distribution.
type blockBodiesData []*blockBody

// blockBody represents the data content of a single block.
type snailBlockBody struct {
	Fruits []*types.SnailBlock
	Signs  []*types.PbftSign
}

// blockBodiesData is the network packet for block content distribution.
type snailBlockBodiesData []*snailBlockBody
