// Copyright 2018 The TrueChain Authors
// This file is part of the truechain-engineering-code library.
//
// The truechain-engineering-code library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The truechain-engineering-code library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the truechain-engineering-code library. If not, see <http://www.gnu.org/licenses/>.

package election

import (
	"bytes"
	"crypto/ecdsa"
	"encoding/hex"
	"errors"
	"math/big"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/hashicorp/golang-lru"
	"github.com/truechain/truechain-engineering-code/consensus"
	"github.com/truechain/truechain-engineering-code/core"
	"github.com/truechain/truechain-engineering-code/core/snailchain/rawdb"
	"github.com/truechain/truechain-engineering-code/core/types"
	"github.com/truechain/truechain-engineering-code/etruedb"
	"github.com/truechain/truechain-engineering-code/event"
	"github.com/truechain/truechain-engineering-code/params"
)

const (
	fastChainHeadSize  = 4096
	snailchainHeadSize = 64

	committeeCacheLimit = 256
)

type ElectMode uint

const (
	// ElectModeEtrue for etrue
	ElectModeEtrue = iota
	// ElectModeFake for Test purpose
	ElectModeFake
)

var (
	// maxUint256 is a big integer representing 2^256-1
	maxUint256 = new(big.Int).Exp(big.NewInt(2), big.NewInt(256), big.NewInt(0))
)

var (
	ErrCommittee     = errors.New("get committee failed")
	ErrInvalidMember = errors.New("invalid committee member")
)

type candidateMember struct {
	coinbase   common.Address
	address    common.Address
	publickey  *ecdsa.PublicKey
	difficulty *big.Int
	upper      *big.Int
	lower      *big.Int
}

type committee struct {
	id                  *big.Int
	beginFastNumber     *big.Int // the first fast block proposed by this committee
	endFastNumber       *big.Int // the last fast block proposed by this committee
	firstElectionNumber *big.Int // the begin snailblock to elect members
	lastElectionNumber  *big.Int // the end snailblock to elect members
	switchCheckNumber   *big.Int // the snailblock that start switch next committee
	members             types.CommitteeMembers
	backupMembers       types.CommitteeMembers
	switches            []uint64 // blocknumbers whose block include switchinfos
}

// Members returns dump of the committee members
func (c *committee) Members() []*types.CommitteeMember {
	members := make([]*types.CommitteeMember, len(c.members))
	copy(members, c.members)
	return members
}

// Members returns dump of the backup committee members
func (c *committee) BackupMembers() []*types.CommitteeMember {
	members := make([]*types.CommitteeMember, len(c.backupMembers))
	copy(members, c.backupMembers)
	return members
}

func (c *committee) setMemberState(pubkey []byte, flag int32) {
	for i, m := range c.members {
		if bytes.Equal(crypto.FromECDSAPub(m.Publickey), pubkey) {
			c.members[i] = &types.CommitteeMember{
				Coinbase:  m.Coinbase,
				Publickey: m.Publickey,
				Flag:      flag,
			}
			break
		}
	}
	for i, m := range c.backupMembers {
		if bytes.Equal(crypto.FromECDSAPub(m.Publickey), pubkey) {
			c.backupMembers[i] = &types.CommitteeMember{
				Coinbase:  m.Coinbase,
				Publickey: m.Publickey,
				Flag:      flag,
			}
			break
		}
	}
}

type Election struct {
	genesisCommittee []*types.CommitteeMember
	defaultMembers   []*types.CommitteeMember

	commiteeCache *lru.Cache

	electionMode    ElectMode
	committee       *committee
	nextCommittee   *committee
	mu              sync.RWMutex
	testPrivateKeys []*ecdsa.PrivateKey

	startSwitchover bool //Flag bit for handling event switching
	singleNode      bool

	electionFeed event.Feed
	scope        event.SubscriptionScope

	fastChainEventCh  chan types.ChainFastEvent
	fastChainEventSub event.Subscription
	switchEventCh  chan types.ChainFastEvent
	switchEventSub event.Subscription

	snailChainEventCh  chan types.ChainSnailEvent
	snailChainEventSub event.Subscription

	fastchain  *core.BlockChain
	snailchain SnailBlockChain

	engine consensus.Engine
}

// SnailLightChain encapsulates functions required to synchronise a light chain.
type SnailLightChain interface {
	// CurrentHeader retrieves the head header from the local chain.
	CurrentHeader() *types.SnailHeader
}

// SnailBlockChain encapsulates functions required to sync a (full or fast) blockchain.
type SnailBlockChain interface {
	SnailLightChain

	// CurrentBlock retrieves the head block from the local chain.
	CurrentBlock() *types.SnailBlock

	GetGenesisCommittee() []*types.CommitteeMember

	SubscribeChainEvent(ch chan<- types.ChainSnailEvent) event.Subscription

	GetDatabase() etruedb.Database

	GetFruitByFastHash(fastHash common.Hash) (*types.SnailBlock, uint64)

	GetBlockByNumber(number uint64) *types.SnailBlock
}

type Config interface {
	GetNodeType() bool
}

// NewElection create election processor and load genesis committee
func NewElection(fastBlockChain *core.BlockChain, snailBlockChain SnailBlockChain, config Config) *Election {
	// init
	election := &Election{
		fastchain:         fastBlockChain,
		snailchain:        snailBlockChain,
		fastChainEventCh:  make(chan types.ChainFastEvent, fastChainHeadSize),
		switchEventCh:     make(chan types.ChainFastEvent, fastChainHeadSize),
		snailChainEventCh: make(chan types.ChainSnailEvent, snailchainHeadSize),
		singleNode:        config.GetNodeType(),
		electionMode:      ElectModeEtrue,
	}

	// get genesis committee
	election.genesisCommittee = election.snailchain.GetGenesisCommittee()
	if len(election.genesisCommittee) == 0 {
		log.Error("Election creation get no genesis committee members")
	}

	election.fastChainEventSub = election.fastchain.SubscribeChainEvent(election.fastChainEventCh)
	election.switchEventSub = election.fastchain.SubscribeChainEvent(election.switchEventCh)
	election.snailChainEventSub = election.snailchain.SubscribeChainEvent(election.snailChainEventCh)
	election.commiteeCache, _ = lru.New(committeeCacheLimit)

	if election.singleNode {
		election.genesisCommittee = election.snailchain.GetGenesisCommittee()[:1]
	}
	if !election.singleNode && len(election.genesisCommittee) < 4 {
		log.Error("Election creation get insufficient genesis committee members")
	}
	for _, m := range election.genesisCommittee {
		var member = *m
		member.Flag = types.StateUnusedFlag
		election.defaultMembers = append(election.defaultMembers, &member)
	}

	return election
}

// NewFakeElection create fake mode election only for testing
func NewFakeElection() *Election {
	var priKeys []*ecdsa.PrivateKey
	var members []*types.CommitteeMember

	for i := 0; i < params.MinimumCommitteeNumber; i++ {
		priKey, err := crypto.GenerateKey()
		priKeys = append(priKeys, priKey)
		if err != nil {
			log.Error("initMembers", "error", err)
		}
		coinbase := crypto.PubkeyToAddress(priKey.PublicKey)
		m := &types.CommitteeMember{coinbase, &priKey.PublicKey, types.StateUsedFlag, types.TypeFixed}
		members = append(members, m)
	}

	// Backup members are empty in FakeMode Election
	elected := &committee{
		id:                  new(big.Int).Set(common.Big0),
		beginFastNumber:     new(big.Int).Set(common.Big1),
		endFastNumber:       new(big.Int).Set(common.Big0),
		firstElectionNumber: new(big.Int).Set(common.Big0),
		lastElectionNumber:  new(big.Int).Set(common.Big0),
		switchCheckNumber:   params.ElectionPeriodNumber,
		members:             members,
	}

	election := &Election{
		fastchain:         nil,
		snailchain:        nil,
		fastChainEventCh:  make(chan types.ChainFastEvent, fastChainHeadSize),
		snailChainEventCh: make(chan types.ChainSnailEvent, snailchainHeadSize),
		singleNode:        false,
		committee:         elected,
		electionMode:      ElectModeFake,
		testPrivateKeys:   priKeys,
	}
	return election
}

func (e *Election) GenerateFakeSigns(fb *types.Block) ([]*types.PbftSign, error) {
	var signs []*types.PbftSign
	for _, privateKey := range e.testPrivateKeys {
		voteSign := &types.PbftSign{
			Result:     types.VoteAgree,
			FastHeight: fb.Header().Number,
			FastHash:   fb.Hash(),
		}
		var err error
		signHash := voteSign.HashWithNoSign().Bytes()
		voteSign.Sign, err = crypto.Sign(signHash, privateKey)
		if err != nil {
			log.Error("fb GenerateSign error ", "err", err)
		}
		signs = append(signs, voteSign)
	}
	return signs, nil
}

func (e *Election) GetGenesisCommittee() []*types.CommitteeMember {
	return e.genesisCommittee
}

func (e *Election) GetCurrentCommittee() *committee {
	return e.committee
}

// GetMemberByPubkey returns committeeMember specified by public key bytes
func (e *Election) GetMemberByPubkey(members []*types.CommitteeMember, publickey []byte) *types.CommitteeMember {
	if len(members) == 0 {
		log.Error("GetMemberByPubkey method len(members)= 0")
		return nil
	}
	for _, member := range members {
		if bytes.Equal(publickey, crypto.FromECDSAPub(member.Publickey)) {
			return member
		}
	}
	return nil
}

// IsCommitteeMember reports whether the provided public key is in committee
func (e *Election) GetMemberFlag(members []*types.CommitteeMember, publickey []byte) int32 {
	if len(members) == 0 {
		log.Error("IsCommitteeMember method len(members)= 0")
		return 0
	}
	for _, member := range members {
		if bytes.Equal(publickey, crypto.FromECDSAPub(member.Publickey)) {
			return member.Flag
		}
	}
	return 0
}

func (e *Election) IsCommitteeMember(members []*types.CommitteeMember, publickey []byte) bool {
	flag := e.GetMemberFlag(members, publickey)
	return flag == types.StateUsedFlag
}

// VerifyPublicKey get the committee member by public key
func (e *Election) VerifyPublicKey(fastHeight *big.Int, pubKeyByte []byte) (*types.CommitteeMember, error) {
	members := e.GetCommittee(fastHeight)
	if members == nil {
		log.Info("GetCommittee members is nil", "fastHeight", fastHeight)
		return nil, ErrCommittee
	}
	member := e.GetMemberByPubkey(members, pubKeyByte)
	/*if member == nil {
		return nil, ErrInvalidMember
	}*/
	return member, nil
}

// VerifySign lookup the pbft sign and return the committee member who signs it
func (e *Election) VerifySign(sign *types.PbftSign) (*types.CommitteeMember, error) {
	pubkey, err := crypto.SigToPub(sign.HashWithNoSign().Bytes(), sign.Sign)
	if err != nil {
		return nil, err
	}
	pubkeyByte := crypto.FromECDSAPub(pubkey)
	member, err := e.VerifyPublicKey(sign.FastHeight, pubkeyByte)
	return member, err
}

// VerifySigns verify signatures of bft committee in batches
func (e *Election) VerifySigns(signs []*types.PbftSign) ([]*types.CommitteeMember, []error) {
	members := make([]*types.CommitteeMember, len(signs))
	errs := make([]error, len(signs))

	if len(signs) == 0 {
		log.Warn("Veriry signs get nil pbftsigns")
		return nil, nil
	}
	// All signs should have the same fastblock height
	committeeMembers := e.GetCommittee(signs[0].FastHeight)
	if len(committeeMembers) == 0 {
		log.Error("Election get none committee for verify pbft signs")
		for i := range errs {
			errs[i] = ErrCommittee
		}
		return members, errs
	}

	for i, sign := range signs {
		// member, err := e.VerifySign(sign)
		pubkey, _ := crypto.SigToPub(sign.HashWithNoSign().Bytes(), sign.Sign)
		member := e.GetMemberByPubkey(committeeMembers, crypto.FromECDSAPub(pubkey))
		if member == nil {
			errs[i] = ErrInvalidMember
		} else {
			members[i] = member
		}
	}
	return members, errs
}

func (e *Election) getElectionMembers(snailBeginNumber *big.Int, snailEndNumber *big.Int) *types.ElectionCommittee {
	// Locate committee id by election snailblock interval
	committeeNum := new(big.Int).Div(new(big.Int).Add(snailEndNumber, params.SnailConfirmInterval), params.ElectionPeriodNumber)

	if new(big.Int).Add(snailEndNumber, params.SnailConfirmInterval).Cmp(params.ElectionPeriodNumber) < 0 {
		committeeNum = common.Big0
	}

	if cache, ok := e.commiteeCache.Get(committeeNum.Uint64()); ok {
		committee := cache.(*types.ElectionCommittee)
		return committee
	}

	members := rawdb.ReadCommittee(e.snailchain.GetDatabase(), committeeNum.Uint64())
	if members != nil {
		e.commiteeCache.Add(committeeNum.Uint64(), members)
		return members
	}

	// Elect members from snailblock
	members = e.electCommittee(snailBeginNumber, snailEndNumber)

	// Cache committee members for next access
	e.commiteeCache.Add(committeeNum.Uint64(), members)
	rawdb.WriteCommittee(e.snailchain.GetDatabase(), committeeNum.Uint64(), members)

	return members
}

// getCommittee returns the committee members who propose this fast block
func (e *Election) getCommittee(fastNumber *big.Int, snailNumber *big.Int) *committee {
	log.Debug("get committee ..", "fastnumber", fastNumber, "snailnumber", snailNumber)
	committeeNumber := new(big.Int).Div(snailNumber, params.ElectionPeriodNumber)
	lastSnailNumber := new(big.Int).Mul(committeeNumber, params.ElectionPeriodNumber)
	firstSnailNumber := new(big.Int).Add(new(big.Int).Sub(lastSnailNumber, params.ElectionPeriodNumber), common.Big1)

	switchCheckNumber := new(big.Int).Sub(lastSnailNumber, params.SnailConfirmInterval)

	log.Debug("get pre committee ", "committee", committeeNumber, "first", firstSnailNumber, "last", lastSnailNumber, "switchcheck", switchCheckNumber)

	if committeeNumber.Cmp(common.Big0) == 0 {
		// genesis committee
		log.Debug("get genesis committee")
		return &committee{
			id:                  new(big.Int).Set(common.Big0),
			beginFastNumber:     new(big.Int).Set(common.Big1),
			endFastNumber:       new(big.Int).Set(common.Big0),
			firstElectionNumber: new(big.Int).Set(common.Big0),
			lastElectionNumber:  new(big.Int).Set(common.Big0),
			switchCheckNumber:   params.ElectionPeriodNumber,
			members:             e.genesisCommittee,
			switches:            rawdb.ReadCommitteeStates(e.snailchain.GetDatabase(), 0),
		}
	}

	endElectionNumber := new(big.Int).Set(switchCheckNumber)
	beginElectionNumber := new(big.Int).Add(new(big.Int).Sub(endElectionNumber, params.ElectionPeriodNumber), common.Big1)
	if beginElectionNumber.Cmp(common.Big0) < 1 {
		beginElectionNumber = new(big.Int).Set(common.Big1)
	}

	// find the last committee end fastblock number
	lastFastNumber := e.getLastNumber(beginElectionNumber, endElectionNumber)
	if lastFastNumber == nil {
		return nil
	}

	log.Debug("check last fast block", "committee", committeeNumber, "last fast", lastFastNumber, "current", fastNumber)
	//genesis committee is long committee ,it's 180 snail block and 9600 fast black
	if lastFastNumber.Cmp(fastNumber) > -1 {
		if committeeNumber.Cmp(common.Big1) == 0 {
			// still at genesis committee
			log.Debug("get genesis committee")
			return &committee{
				id:                  new(big.Int).Set(common.Big0),
				beginFastNumber:     new(big.Int).Set(common.Big1),
				endFastNumber:       lastFastNumber,
				firstElectionNumber: new(big.Int).Set(common.Big0),
				lastElectionNumber:  new(big.Int).Set(common.Big0),
				switchCheckNumber:   params.ElectionPeriodNumber,
				members:             e.genesisCommittee,
				switches:            rawdb.ReadCommitteeStates(e.snailchain.GetDatabase(), 0),
			}
		}
		// get pre snail block to elect current committee
		preEndElectionNumber := new(big.Int).Sub(switchCheckNumber, params.ElectionPeriodNumber)
		preBeginElectionNumber := new(big.Int).Add(new(big.Int).Sub(preEndElectionNumber, params.ElectionPeriodNumber), common.Big1)
		if preBeginElectionNumber.Cmp(common.Big0) < 1 {
			preBeginElectionNumber = new(big.Int).Set(common.Big1)
		}
		preEndFast := e.getLastNumber(preBeginElectionNumber, preEndElectionNumber)
		if preEndFast == nil {
			return nil
		}

		log.Debug("get committee", "electFirst", preBeginElectionNumber, "electLast", preEndElectionNumber, "lastFast", preEndFast)

		members := e.getElectionMembers(preBeginElectionNumber, preEndElectionNumber)
		return &committee{
			id:                  new(big.Int).Sub(committeeNumber, common.Big1),
			beginFastNumber:     new(big.Int).Add(preEndFast, common.Big1),
			endFastNumber:       lastFastNumber,
			firstElectionNumber: preBeginElectionNumber,
			lastElectionNumber:  preEndElectionNumber,
			switchCheckNumber:   lastSnailNumber,
			members:             members.Members,
			backupMembers:       members.Backups,
			switches:            rawdb.ReadCommitteeStates(e.snailchain.GetDatabase(), new(big.Int).Sub(committeeNumber, common.Big1).Uint64()),
		}
	}

	log.Debug("get committee", "electFirst", beginElectionNumber, "electLast", endElectionNumber, "lastFast", lastFastNumber)

	members := e.getElectionMembers(beginElectionNumber, endElectionNumber)
	return &committee{
		id:                  committeeNumber,
		beginFastNumber:     new(big.Int).Add(lastFastNumber, common.Big1),
		endFastNumber:       new(big.Int).Set(common.Big0),
		firstElectionNumber: beginElectionNumber,
		lastElectionNumber:  endElectionNumber,
		switchCheckNumber:   new(big.Int).Add(lastSnailNumber, params.ElectionPeriodNumber),
		members:             members.Members,
		backupMembers:       members.Backups,
		switches:            rawdb.ReadCommitteeStates(e.snailchain.GetDatabase(), committeeNumber.Uint64()),
	}
}

// GetCommittee gets committee members which propose this fast block
func (e *Election) electedCommittee(fastNumber *big.Int) *committee {
	if e.electionMode == ElectModeFake {
		return e.committee
	}

	fastHeadNumber := e.fastchain.CurrentHeader().Number
	snailHeadNumber := e.snailchain.CurrentHeader().Number
	e.mu.RLock()
	currentCommittee := e.committee
	nextCommittee := e.nextCommittee
	e.mu.RUnlock()

	if nextCommittee != nil {
		//log.Debug("next committee info..", "id", nextCommittee.id, "firstNumber", nextCommittee.beginFastNumber)
		if fastNumber.Cmp(nextCommittee.beginFastNumber) >= 0 {
			log.Debug("get committee nextCommittee", "fastNumber", fastNumber, "nextfast", nextCommittee.beginFastNumber)
			return nextCommittee
		}
	}
	if currentCommittee != nil {
		//log.Debug("current committee info..", "id", currentCommittee.id, "firstNumber", currentCommittee.beginFastNumber)
		if fastNumber.Cmp(currentCommittee.beginFastNumber) >= 0 {
			return currentCommittee
		}
	}

	fastBlock := e.fastchain.GetBlockByNumber(fastNumber.Uint64())
	if fastBlock == nil {
		log.Info("get committee failed (no fast block)", "fastnumber", fastNumber, "currentNumber", fastHeadNumber)
		return nil
	}
	// get snail number
	var snailNumber *big.Int
	snailBlock, _ := e.snailchain.GetFruitByFastHash(fastBlock.Hash())
	if snailBlock == nil {
		// fast block has not stored in snail chain
		// TODO: when fast number is so far away from snail block
		snailNumber = snailHeadNumber
	} else {
		snailNumber = snailBlock.Number()
	}

	committee := e.getCommittee(fastNumber, snailNumber)
	if committee == nil {
		return nil
	}

	return committee
}

// GetCommittee gets committee members propose this fast block
func (e *Election) GetCommittee(fastNumber *big.Int) []*types.CommitteeMember {
	var members []*types.CommitteeMember

	committee := e.electedCommittee(fastNumber)
	if committee == nil {
		log.Error("Failed to fetch elected committee", "fast", fastNumber)
		return nil
	}
	if len(committee.switches) == 0 {
		return committee.Members()
	}

	states := make(map[string]int32)
	if fastNumber.Uint64() > committee.switches[len(committee.switches)-1] {
		// Apply all committee state switches for latest block
		for _, num := range committee.switches {
			b := e.fastchain.GetBlockByNumber(num)
			for _, s := range b.SwitchInfos().Vals {
				switch s.Flag {
				case types.StateAppendFlag:
					states[hex.EncodeToString(s.Pk)] = types.StateAppendFlag
				case types.StateRemovedFlag:
					states[hex.EncodeToString(s.Pk)] = types.StateRemovedFlag
				}
			}
		}
	} else {
		for _, num := range committee.switches {
			if num >= fastNumber.Uint64() {
				break
			}
			b := e.fastchain.GetBlockByNumber(num)
			for _, s := range b.SwitchInfos().Vals {
				switch s.Flag {
				case types.StateAppendFlag:
					states[hex.EncodeToString(s.Pk)] = types.StateAppendFlag
				case types.StateRemovedFlag:
					states[hex.EncodeToString(s.Pk)] = types.StateRemovedFlag
				}
			}
		}
	}

	for _, m := range committee.Members() {
		if flag, ok := states[hex.EncodeToString(crypto.FromECDSAPub(m.Publickey))]; ok {
			if flag != types.StateRemovedFlag {
				members = append(members, m)
			}
		} else {
			members = append(members, m)
		}
	}
	for _, m := range committee.BackupMembers() {
		if flag, ok := states[hex.EncodeToString(crypto.FromECDSAPub(m.Publickey))]; ok {
			if flag == types.StateAppendFlag {
				members = append(members, m)
			}
		}
	}

	return members
}

// GetComitteeById return committee info sepecified by Committee ID
func (e *Election) GetComitteeById(id *big.Int) map[string]interface{} {
	e.mu.RLock()
	currentCommittee := e.committee
	e.mu.RUnlock()

	info := make(map[string]interface{})

	if currentCommittee.id.Cmp(id) < 0 {
		return nil
	}
	if id.Cmp(common.Big0) <= 0 {
		// Use genesis committee
		info["id"] = 0
		info["beginSnailNumber"] = 0
		info["endSnailNumber"] = 0
		info["memberCount"] = len(e.genesisCommittee)
		info["members"] = membersDisplay(e.genesisCommittee)
		info["beginNumber"] = 1
		info["endNumber"] = nil
		if currentCommittee.id.Cmp(id) == 0 {
			// Committee end fast number may not be available when current snail lower than commiteeId * period
			if currentCommittee.endFastNumber != nil && currentCommittee.endFastNumber.Uint64() > 0 {
				info["endNumber"] = currentCommittee.endFastNumber.Uint64()
			}
		} else {
			end := new(big.Int).Sub(params.ElectionPeriodNumber, params.SnailConfirmInterval)
			info["endNumber"] = e.getLastNumber(big.NewInt(1), end).Uint64()
		}
		return info
	}
	// Calclulate election members from previous election period
	endElectionNumber := new(big.Int).Mul(id, params.ElectionPeriodNumber)
	endElectionNumber.Sub(endElectionNumber, params.SnailConfirmInterval)
	beginElectionNumber := new(big.Int).Add(new(big.Int).Sub(endElectionNumber, params.ElectionPeriodNumber), common.Big1)
	if beginElectionNumber.Cmp(common.Big0) <= 0 {
		beginElectionNumber = new(big.Int).Set(common.Big1)
	}

	elected := e.getElectionMembers(beginElectionNumber, endElectionNumber)
	if elected != nil {
		info["id"] = id.Uint64()
		info["memberCount"] = len(elected.Members) + len(elected.Backups)
		info["beginSnailNumber"] = beginElectionNumber.Uint64()
		info["endSnailNumber"] = endElectionNumber.Uint64()
		info["members"] = membersDisplay(elected.Members)
		info["backups"] = membersDisplay(elected.Backups)
		info["beginNumber"] = new(big.Int).Add(e.getLastNumber(beginElectionNumber, endElectionNumber), common.Big1).Uint64()
		info["endNumber"] = nil
		// Committee end fast number may be nil if current committee is working on
		if currentCommittee.id.Cmp(id) == 0 {
			// Committee end fast number may not be available when current snail lower than commiteeId * period
			if currentCommittee.endFastNumber != nil && currentCommittee.endFastNumber.Uint64() > 0 {
				info["endNumber"] = currentCommittee.endFastNumber.Uint64()
			}
		} else {
			begin := new(big.Int).Add(beginElectionNumber, params.ElectionPeriodNumber)
			end := new(big.Int).Add(endElectionNumber, params.ElectionPeriodNumber)
			info["endNumber"] = new(big.Int).Sub(e.getLastNumber(begin, end), common.Big1).Uint64()
		}
		return info
	}

	return nil
}

func membersDisplay(members []*types.CommitteeMember) []map[string]interface{} {
	var attrs []map[string]interface{}
	for _, member := range members {
		attrs = append(attrs, map[string]interface{}{
			"coinbase": member.Coinbase,
			"PKey":     hex.EncodeToString(crypto.FromECDSAPub(member.Publickey)),
			"flag":     member.Flag,
			"type":     member.MType,
		})
	}
	return attrs
}

// getCandinates get candinate miners and seed from given snail blocks
func (e *Election) getCandinates(snailBeginNumber *big.Int, snailEndNumber *big.Int) (common.Hash, []*candidateMember) {
	var fruitsCount = make(map[common.Address]uint64)
	var members []*candidateMember

	var seed []byte

	// get all fruits want to be elected and their pubic key is valid
	for blockNumber := snailBeginNumber; blockNumber.Cmp(snailEndNumber) <= 0; {
		block := e.snailchain.GetBlockByNumber(blockNumber.Uint64())
		if block == nil {
			return common.Hash{}, nil
		}

		seed = append(seed, block.Hash().Bytes()...)

		fruits := block.Fruits()
		for _, f := range fruits {
			if f.ToElect() {
				pubkey, err := f.GetPubKey()
				if err != nil {
					continue
				}
				addr := crypto.PubkeyToAddress(*pubkey)

				act, diff := e.engine.GetDifficulty(f.Header(), true)

				member := &candidateMember{
					coinbase:   f.Coinbase(),
					publickey:  pubkey,
					address:    addr,
					difficulty: new(big.Int).Sub(act, diff),
				}

				members = append(members, member)
				if _, ok := fruitsCount[addr]; ok {
					fruitsCount[addr]++
				} else {
					fruitsCount[addr] = 1
				}
			}
		}
		blockNumber = new(big.Int).Add(blockNumber, big.NewInt(1))
	}

	log.Debug("get committee candidate", "fruit", len(members), "members", len(fruitsCount))

	var candidates []*candidateMember
	td := big.NewInt(0)
	for _, member := range members {
		if cnt, ok := fruitsCount[member.address]; ok {
			log.Trace("get committee candidate", "keyAddr", member.address, "count", cnt, "diff", member.difficulty)
			if cnt >= params.ElectionFruitsThreshold {
				td.Add(td, member.difficulty)

				candidates = append(candidates, member)
			}
		}
	}
	log.Debug("get final candidate", "count", len(candidates), "td", td)
	if len(candidates) == 0 {
		log.Warn("getCandinates not get candidates")
		return common.Hash{}, nil
	}

	dd := big.NewInt(0)
	rate := new(big.Int).Div(maxUint256, td)
	for i, member := range candidates {
		member.lower = new(big.Int).Mul(rate, dd)

		dd = new(big.Int).Add(dd, member.difficulty)

		if i == len(candidates)-1 {
			member.upper = new(big.Int).Set(maxUint256)
		} else {
			member.upper = new(big.Int).Mul(rate, dd)
		}

		log.Trace("get power", "member", member.address, "lower", member.lower, "upper", member.upper)
	}

	return crypto.Keccak256Hash(seed), candidates
}

//getLastNumber is the endSanil's last fruit's number add 9600
func (e *Election) getLastNumber(beginSnail, endSnail *big.Int) *big.Int {

	beginElectionBlock := e.snailchain.GetBlockByNumber(beginSnail.Uint64())
	if beginElectionBlock == nil {
		return nil
	}
	endElectionBlock := e.snailchain.GetBlockByNumber(endSnail.Uint64())
	if endElectionBlock == nil {
		return nil
	}

	fruits := endElectionBlock.Fruits()
	lastFruitNumber := fruits[len(fruits)-1].FastNumber()
	lastFastNumber := new(big.Int).Add(lastFruitNumber, params.ElectionSwitchoverNumber)

	return lastFastNumber
}

// elect is a lottery function that select committee members from candidates miners
func (e *Election) elect(candidates []*candidateMember, seed common.Hash) []*types.CommitteeMember {
	var addrs = make(map[common.Address]uint)
	var members []*types.CommitteeMember
	var defaults = make(map[common.Address]*types.CommitteeMember)

	for _, g := range e.defaultMembers {
		defaults[crypto.PubkeyToAddress(*g.Publickey)] = g
	}
	log.Debug("elect committee members ..", "count", len(candidates), "seed", seed)
	round := new(big.Int).Set(common.Big1)
	for {
		seedNumber := new(big.Int).Add(seed.Big(), round)
		hash := crypto.Keccak256Hash(seedNumber.Bytes())
		//prop := new(big.Int).Div(maxUint256, hash.Big())
		prop := hash.Big()

		for _, cm := range candidates {
			if prop.Cmp(cm.lower) < 0 {
				continue
			}
			if prop.Cmp(cm.upper) >= 0 {
				continue
			}

			log.Trace("get member", "seed", hash, "member", cm.address, "prop", prop)
			if _, ok := defaults[cm.address]; ok {
				// No need to select default committee member
				break
			}
			if _, ok := addrs[cm.address]; ok {
				break
			}
			addrs[cm.address] = 1
			member := &types.CommitteeMember{
				Coinbase:  cm.coinbase,
				Publickey: cm.publickey,
				Flag:      types.StateUnusedFlag,
			}
			members = append(members, member)

			break
		}

		round = new(big.Int).Add(round, common.Big1)
		if round.Cmp(params.MaximumCommitteeNumber) > 0 {
			break
		}
	}

	log.Debug("get new committee members", "count", len(members))

	return members
}

// electCommittee elect committee members from snail block.
func (e *Election) electCommittee(snailBeginNumber *big.Int, snailEndNumber *big.Int) *types.ElectionCommittee {
	log.Info("elect new committee..", "begin", snailBeginNumber, "end", snailEndNumber,
		"threshold", params.ElectionFruitsThreshold, "max", params.MaximumCommitteeNumber)

	var committee types.ElectionCommittee

	seed, candidates := e.getCandinates(snailBeginNumber, snailEndNumber)
	if candidates == nil {
		log.Warn("can't get election candidates, retain default committee", "begin", snailBeginNumber, "end", snailEndNumber)
	} else {
		members := e.elect(candidates, seed)
		if len(members) > params.MinimumCommitteeNumber {
			committee.Members = members[:params.MinimumCommitteeNumber]
			committee.Backups = members[params.MinimumCommitteeNumber:]
		} else {
			committee.Members = members
		}
	}

	for _, member := range committee.Members {
		member.Flag = types.StateUsedFlag
		member.MType = types.TypeWorked
	}
	for _, member := range committee.Backups {
		member.MType = types.TypeBack
	}
	if len(committee.Members) >= 4 {
		committee.Backups = append(committee.Backups, e.defaultMembers...)
	} else {
		// PBFT need a minimum 3f+1 members
		// Use genesis committee as default committee
		log.Warn("can't elect new committee, use default committee", "count", len(committee.Members), "begin", snailBeginNumber, "end", snailEndNumber)
		committee.Members = e.genesisCommittee
	}

	return &committee
}

// filterWithSwitchInfo return committee members which are applied all switchinfo changes
func (e *Election) filterWithSwitchInfo(c *committee) (members, backups []*types.CommitteeMember) {
	members = c.Members()
	backups = c.BackupMembers()
	if len(c.switches) == 0 {
		log.Info("Committee filter get no switch infos", "id", c.id)
		return
	}

	// Apply all committee state switches for latest block
	states := make(map[string]int32)
	for _, num := range c.switches {
		b := e.fastchain.GetBlockByNumber(num)
		for _, s := range b.SwitchInfos().Vals {
			switch s.Flag {
			case types.StateAppendFlag:
				states[hex.EncodeToString(s.Pk)] = types.StateAppendFlag
			case types.StateRemovedFlag:
				states[hex.EncodeToString(s.Pk)] = types.StateRemovedFlag
			}
		}
	}
	for k, flag := range states {
		enums := map[int32]string{
			types.StateAppendFlag:  "add",
			types.StateRemovedFlag: "drop",
		}
		log.Info("Committee switch info transition", "bftkey", k, "state", enums[flag], "committee", c.id)
	}

	for i, m := range members {
		if flag, ok := states[hex.EncodeToString(crypto.FromECDSAPub(m.Publickey))]; ok {
			if flag == types.StateRemovedFlag {
				// Update the committee member state
				var switched = *m
				switched.Flag = types.StateRemovedFlag
				members[i] = &switched
			}
		}
	}
	for i, m := range backups {
		if flag, ok := states[hex.EncodeToString(crypto.FromECDSAPub(m.Publickey))]; ok {
			if flag == types.StateAppendFlag {
				// Update the committee member state
				var switched = *m
				switched.Flag = types.StateUsedFlag
				backups[i] = &switched
			}
			if flag == types.StateRemovedFlag {
				// Update the committee member state
				var switched = *m
				switched.Flag = types.StateRemovedFlag
				backups[i] = &switched
			}
		}
	}
	return
}

// updateMembers update Committee members if switchinfo found in block
func (e *Election) updateMembers(fastNumber *big.Int, infos *types.SwitchInfos) {
	if infos == nil || len(infos.Vals) == 0 {
		return
	}
	log.Info("Election update committee member state", "committee", infos.CID, "block", fastNumber)

	var (
		committee *committee
		endfast   *big.Int
	)

	e.mu.Lock()
	defer e.mu.Unlock()
	if infos.CID == e.committee.id.Uint64() {
		committee = e.committee
	} else if infos.CID == e.nextCommittee.id.Uint64() {
		committee = e.nextCommittee
	} else {
		log.Warn("Election switchinfo not in current Committee", "committee", infos.CID)
		return
	}

	committee.switches = append(committee.switches, fastNumber.Uint64())
	rawdb.WriteCommitteeStates(e.snailchain.GetDatabase(), infos.CID, committee.switches)

	// Update pbft server's committee info via pbft agent proxy
	members, backups := e.filterWithSwitchInfo(committee)
	if committee.endFastNumber != nil {
		endfast = committee.endFastNumber
	} else {
		endfast = big.NewInt(0)
	}
	e.electionFeed.Send(types.ElectionEvent{
		Option:           types.CommitteeUpdate,
		CommitteeID:      committee.id,
		BeginFastNumber:  fastNumber,
		EndFastNumber:    endfast,
		CommitteeMembers: members,
		BackupMembers:    backups,
	})
}

// Start load current committ and starts election processing
func (e *Election) Start() error {
	// get current committee info
	fastHeadNumber := e.fastchain.CurrentBlock().Number()
	snailHeadNumber := e.snailchain.CurrentBlock().Number()

	currentCommittee := e.getCommittee(fastHeadNumber, snailHeadNumber)
	if currentCommittee == nil {
		log.Crit("Election faiiled to get committee on start")
		return nil
	}
	// Rewind committee swtichinfo storage if blockchain rollbacks
	for i := 0; i < len(currentCommittee.switches); i++ {
		if currentCommittee.switches[i] > fastHeadNumber.Uint64() {
			log.Info("Rewind committee switchinfo", "committee", currentCommittee.id, "current", fastHeadNumber)
			currentCommittee.switches = currentCommittee.switches[:i]
			rawdb.WriteCommitteeStates(e.snailchain.GetDatabase(), currentCommittee.id.Uint64(), currentCommittee.switches)
			break
		}
	}

	e.committee = currentCommittee

	if currentCommittee.endFastNumber.Cmp(common.Big0) > 0 {
		// over the switch block, to elect next committee
		electEndSnailNumber := new(big.Int).Add(currentCommittee.lastElectionNumber, params.ElectionPeriodNumber)
		electBeginSnailNumber := new(big.Int).Add(new(big.Int).Sub(electEndSnailNumber, params.ElectionPeriodNumber), common.Big1)

		members := e.getElectionMembers(electBeginSnailNumber, electEndSnailNumber)

		// get next committee
		nextCommittee := &committee{
			id:                  new(big.Int).Add(currentCommittee.id, common.Big1),
			beginFastNumber:     new(big.Int).Add(currentCommittee.endFastNumber, common.Big1),
			endFastNumber:       new(big.Int).Set(common.Big0),
			firstElectionNumber: electBeginSnailNumber,
			lastElectionNumber:  electEndSnailNumber,
			switchCheckNumber:   new(big.Int).Add(e.committee.switchCheckNumber, params.ElectionPeriodNumber),
			members:             members.Members,
			backupMembers:       members.Backups,
			switches:            rawdb.ReadCommitteeStates(e.snailchain.GetDatabase(), new(big.Int).Add(currentCommittee.id, common.Big1).Uint64()),
		}
		// Reset next committee swtichinfo storage if blockchain rollback
		if len(nextCommittee.switches) > 0 {
			log.Info("Reset next committee switchinfo", "committee", nextCommittee.id, "current", fastHeadNumber)
			rawdb.WriteCommitteeStates(e.snailchain.GetDatabase(), nextCommittee.id.Uint64(), nil)
			nextCommittee.switches = []uint64{}
		}
		e.nextCommittee = nextCommittee
		// start switchover
		e.startSwitchover = true

		if e.committee.endFastNumber.Cmp(fastHeadNumber) == 0 {
			// committee has finish their work, start the new committee

			e.committee = e.nextCommittee
			e.nextCommittee = nil

			e.startSwitchover = false
		}
	}

	// send event to the subscripber
	go func(e *Election) {

		printCommittee(e.committee)
		members, backups := e.filterWithSwitchInfo(e.committee)
		e.electionFeed.Send(types.ElectionEvent{
			Option:           types.CommitteeSwitchover,
			CommitteeID:      e.committee.id,
			CommitteeMembers: members,
			BackupMembers:    backups,
			BeginFastNumber:  e.committee.beginFastNumber,
		})
		e.electionFeed.Send(types.ElectionEvent{
			Option:           types.CommitteeStart,
			CommitteeID:      e.committee.id,
			CommitteeMembers: members,
			BackupMembers:    backups,
			BeginFastNumber:  e.committee.beginFastNumber,
		})

		if e.startSwitchover {
			printCommittee(e.nextCommittee)
			e.electionFeed.Send(types.ElectionEvent{
				Option:           types.CommitteeOver,
				CommitteeID:      e.committee.id,
				CommitteeMembers: e.committee.Members(),
				BackupMembers:    e.committee.BackupMembers(),
				BeginFastNumber:  e.committee.beginFastNumber,
				EndFastNumber:    e.committee.endFastNumber,
			})
			// send switch event to the subscripber
			e.electionFeed.Send(types.ElectionEvent{
				Option:           types.CommitteeSwitchover,
				CommitteeID:      e.nextCommittee.id,
				CommitteeMembers: e.nextCommittee.Members(),
				BackupMembers:    e.nextCommittee.BackupMembers(),
				BeginFastNumber:  e.nextCommittee.beginFastNumber,
			})
		}
	}(e)

	// Start the event loop and return
	go e.loop()
	go e.switchLoop()

	return nil
}

// switchloop update committee members flag based on fast block chain event
func(e *Election) switchLoop() {
	for {
		select {
		case ev := <-e.switchEventCh:
			if ev.Block != nil {
				info := ev.Block.SwitchInfos()
				if len(info.Vals) > 0 {
					log.Info("Election receive committee switch info", "committee", info.CID)
					e.updateMembers(ev.Block.Number(), info)
				}
			}
		}
	}
}

//Monitor both chains and trigger elections at the same time
func (e *Election) loop() {
	// Keep waiting for and reacting to the various events
	for {
		select {
		// Handle ChainHeadEvent
		case se := <-e.snailChainEventCh:
			if se.Block != nil {
				//Record Numbers to open elections
				if e.committee.switchCheckNumber.Cmp(se.Block.Number()) == 0 {
					// get end fast block number
					var snailStartNumber *big.Int
					snailEndNumber := new(big.Int).Sub(se.Block.Number(), params.SnailConfirmInterval)
					if snailEndNumber.Cmp(params.ElectionPeriodNumber) < 0 {
						snailStartNumber = new(big.Int).Set(common.Big1)
					} else {
						snailStartNumber = new(big.Int).Add(new(big.Int).Sub(snailEndNumber, params.ElectionPeriodNumber), common.Big1)
					}

					lastFastNumber := e.getLastNumber(snailStartNumber, snailEndNumber)

					e.committee.endFastNumber = new(big.Int).Set(lastFastNumber)

					e.electionFeed.Send(types.ElectionEvent{
						Option:           types.CommitteeOver, //only update committee end fast black
						CommitteeID:      e.committee.id,
						CommitteeMembers: e.committee.Members(),
						BeginFastNumber:  e.committee.beginFastNumber,
						EndFastNumber:    e.committee.endFastNumber,
					})

					// elect next committee
					members := e.getElectionMembers(snailStartNumber, snailEndNumber)

					log.Info("Election BFT committee election start..", "snail", se.Block.Number(), "endfast", e.committee.endFastNumber, "members", len(members.Members))

					nextCommittee := &committee{
						id:                  new(big.Int).Div(e.committee.switchCheckNumber, params.ElectionPeriodNumber),
						firstElectionNumber: snailStartNumber,
						lastElectionNumber:  snailEndNumber,
						beginFastNumber:     new(big.Int).Add(e.committee.endFastNumber, common.Big1),
						switchCheckNumber:   new(big.Int).Add(e.committee.switchCheckNumber, params.ElectionPeriodNumber),
						members:             members.Members,
						backupMembers:       members.Backups,
					}

					if e.nextCommittee != nil {
						if e.nextCommittee.id.Cmp(nextCommittee.id) == 0 {
							// get next committee twice
							continue
						}
					}
					e.mu.Lock()
					e.nextCommittee = nextCommittee
					e.startSwitchover = true
					e.mu.Unlock()

					log.Info("Election switchover new committee", "id", e.nextCommittee.id, "startNumber", e.nextCommittee.beginFastNumber)
					printCommittee(e.nextCommittee)

					e.electionFeed.Send(types.ElectionEvent{
						Option:           types.CommitteeSwitchover, //update next committee
						CommitteeID:      e.nextCommittee.id,
						CommitteeMembers: e.nextCommittee.Members(),
						BackupMembers:    e.nextCommittee.BackupMembers(),
						BeginFastNumber:  e.nextCommittee.beginFastNumber,
					})
				}
			}
			// Make logical decisions based on the Number provided by the ChainheadEvent
		case ev := <-e.fastChainEventCh:
			if ev.Block != nil {
				if e.startSwitchover {
					if e.committee.endFastNumber.Cmp(ev.Block.Number()) == 0 {
						log.Info("Election stop committee..", "id", e.committee.id)
						e.electionFeed.Send(types.ElectionEvent{
							Option:           types.CommitteeStop,
							CommitteeID:      e.committee.id,
							CommitteeMembers: e.committee.Members(),
							BackupMembers:    e.committee.BackupMembers(),
							BeginFastNumber:  e.committee.beginFastNumber,
							EndFastNumber:    e.committee.endFastNumber,
						})

						e.mu.Lock()
						e.committee = e.nextCommittee
						e.nextCommittee = nil
						e.mu.Unlock()

						e.startSwitchover = false

						log.Info("Election start new BFT committee", "id", e.committee.id)

						e.electionFeed.Send(types.ElectionEvent{
							Option:           types.CommitteeStart,
							CommitteeID:      e.committee.id,
							CommitteeMembers: e.committee.Members(),
							BackupMembers:    e.committee.BackupMembers(),
							BeginFastNumber:  e.committee.beginFastNumber,
						})
					}
				}
			}
		}
	}
}

// SubscribeElectionEvent adds a channel to feed on committee change event
func (e *Election) SubscribeElectionEvent(ch chan<- types.ElectionEvent) event.Subscription {
	return e.scope.Track(e.electionFeed.Subscribe(ch))
}

// SetEngine set election backend consesus
func (e *Election) SetEngine(engine consensus.Engine) {
	e.engine = engine
}

func printCommittee(c *committee) {
	log.Info("Committee Info", "ID", c.id, "count", len(c.members), "start", c.beginFastNumber)
	for _, member := range c.members {
		key := crypto.FromECDSAPub(member.Publickey)
		log.Info("Committee member: ", "PKey", hex.EncodeToString(key), "coinbase", member.Coinbase)
	}
	for _, member := range c.backupMembers {
		key := crypto.FromECDSAPub(member.Publickey)
		log.Info("Committee backup member: ", "PKey", hex.EncodeToString(key), "coinbase", member.Coinbase)
	}
}
