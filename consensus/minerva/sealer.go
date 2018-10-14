// Copyright 2017 The go-ethereum Authors
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

package minerva

import (
	crand "crypto/rand"
	"math"
	"math/big"
	"math/rand"
	"runtime"
	"sync"

	"github.com/truechain/truechain-engineering-code/common"
	"github.com/truechain/truechain-engineering-code/consensus"
	"github.com/truechain/truechain-engineering-code/core/types"
	"github.com/truechain/truechain-engineering-code/log"
)

// Seal implements consensus.Engine, attempting to find a nonce that satisfies
// the block's difficulty requirements.
func (m *Minerva) Seal(chain consensus.SnailChainReader, block *types.SnailBlock, stop <-chan struct{}) (*types.SnailBlock, error) {
	// If we're running a fake PoW, simply return a 0 nonce immediately
	if m.config.PowMode == ModeFake || m.config.PowMode == ModeFullFake {
		header := block.Header()
		header.Nonce, header.MixDigest = types.BlockNonce{}, common.Hash{}
		return block.WithSeal(header), nil
	}
	// If we're running a shared PoW, delegate sealing to it
	if m.shared != nil {
		return m.shared.Seal(chain, block, stop)
	}
	// Create a runner and the multiple search threads it directs
	abort := make(chan struct{})
	found := make(chan *types.SnailBlock)

	m.lock.Lock()
	threads := m.threads
	if m.rand == nil {
		seed, err := crand.Int(crand.Reader, big.NewInt(math.MaxInt64))
		if err != nil {
			m.lock.Unlock()
			return nil, err
		}
		m.rand = rand.New(rand.NewSource(seed.Int64()))
	}
	m.lock.Unlock()
	if threads == 0 {
		threads = runtime.NumCPU()
	}
	if threads < 0 {
		threads = 0 // Allows disabling local mining without extra logic around local/remote
	}
	var pend sync.WaitGroup
	for i := 0; i < threads; i++ {
		pend.Add(1)
		go func(id int, nonce uint64) {
			defer pend.Done()
			m.mineSnail(block, id, nonce, abort, found)
		}(i, uint64(m.rand.Int63()))
	}
	// Wait until sealing is terminated or a nonce is found
	var result *types.SnailBlock
	select {
	case <-stop:
		// Outside abort, stop all miner threads
		close(abort)
		//TODO found function
		/*
			case result = <-found:
				// One of the threads found a block, abort all others
				close(abort)
		*/
	case <-m.update:
		// Thread count was changed on user request, restart
		close(abort)
		pend.Wait()
		return m.Seal(chain, block, stop)
	}
	// Wait for all miners to terminate and return the block
	pend.Wait()
	return result, nil
}

// ConSeal implements consensus.Engine, attempting to find a nonce that satisfies
// the block's difficulty requirements.
func (m *Minerva) ConSeal(chain consensus.SnailChainReader, block *types.SnailBlock, stop <-chan struct{}, send chan *types.SnailBlock) {
	// If we're running a fake PoW, simply return a 0 nonce immediately
	if m.config.PowMode == ModeFake || m.config.PowMode == ModeFullFake {
		header := block.Header()
		header.Nonce, header.MixDigest = types.BlockNonce{}, common.Hash{}
		send <- block.WithSeal(header)
		//return block.WithSeal(header), nil
	}
	// If we're running a shared PoW, delegate sealing to it
	if m.shared != nil {
		m.shared.ConSeal(chain, block, stop, send)
	}

	// Create a runner and the multiple search threads it directs
	abort := make(chan struct{})
	found := make(chan *types.SnailBlock)

	m.lock.Lock()
	threads := m.threads
	if m.rand == nil {
		seed, err := crand.Int(crand.Reader, big.NewInt(math.MaxInt64))
		if err != nil {
			m.lock.Unlock()
			send <- nil
			//return nil, err
		}
		m.rand = rand.New(rand.NewSource(seed.Int64()))
	}
	m.lock.Unlock()
	if threads == 0 {
		cpuNumber := runtime.NumCPU()
		log.Info("Seal get cpu number", "number", cpuNumber)

		// remain one cpu to process fast block
		threads = cpuNumber - 1
		if threads <= 0 {
			threads = 1
		}
	}
	if threads < 0 {
		threads = 0 // Allows disabling local mining without extra logic around local/remote
		//log.Error("Stop mining for CPU number less than 2 or set threads number error.")
	}
	var pend sync.WaitGroup
	for i := 0; i < threads; i++ {
		pend.Add(1)
		go func(id int, nonce uint64) {
			defer pend.Done()
			m.mineSnail(block, id, nonce, abort, found)
		}(i, uint64(m.rand.Int63()))
	}
	// Wait until sealing is terminated or a nonce is found
	var result *types.SnailBlock

mineloop:
	for {
		select {
		case <-stop:
			// Outside abort, stop all miner threads
			close(abort)
			pend.Wait()
			break mineloop
		case result = <-found:
			// One of the threads found a block or fruit return it
			send <- result
			// TODO snail need a flag to distinguish furit and block

			if block.Fruits() != nil {
				if !result.IsFruit() {
					// stop threads when get a block, wait for outside abort when result is fruit
					close(abort)
					pend.Wait()
					break mineloop
				}
			} else {
				close(abort)
				pend.Wait()
				break mineloop
			}

			break
		case <-m.update:
			// Thread count was changed on user request, restart
			close(abort)
			pend.Wait()
			m.ConSeal(chain, block, stop, send)
			break mineloop
		}
	}
	// Wait for all miners to terminate and return the block

	//send <- result
	//return result, nil
}

func (m *Minerva) mineSnail(block *types.SnailBlock, id int, seed uint64, abort chan struct{}, found chan *types.SnailBlock) {
	// Extract some data from the header
	var (
		header = block.Header()
		hash   = header.HashNoNonce().Bytes()
		target = new(big.Int).Div(maxUint128, header.Difficulty)
		fruitTarget = new(big.Int).Div(maxUint128, header.FruitDifficulty)
	)

	//m.CheckDataSetState(block.Number().Uint64())

	// Start generating random nonces until we abort or find a good one
	var (
		attempts = int64(0)
		nonce    = seed
	)
	logger := log.New("miner", id)
	log.Debug("mineSnail","miner",id,"block num",block.Number(),"fb num",block.FastNumber())
	logger.Trace("Started truehash search for new nonces", "seed", seed)
search:
	for {
		select {
		case <-abort:
			// Mining terminated, update stats and abort
			logger.Trace("m nonce search aborted", "attempts", nonce-seed)
			m.hashrate.Mark(attempts)
			break search

		default:
			// We don't have to update hash rate on every nonce, so update after after 2^X nonces
			attempts++
			if (attempts % (1 << 15)) == 0 {
				m.hashrate.Mark(attempts)
				attempts = 0
			}
			// Compute the PoW value of this nonce
			digest, result := truehashFull(*m.dataset.dataset, hash, nonce)

			headResult := result[:16]
			if new(big.Int).SetBytes(headResult).Cmp(target) <= 0 {
				// Correct nonce found, create a new header with it
				if block.Fruits() != nil {
					header = types.CopySnailHeader(header)
					header.Nonce = types.EncodeNonce(nonce)
					header.MixDigest = common.BytesToHash(digest)
					//TODO need add fruit flow
					header.Fruit = false

					// Seal and return a block (if still needed)
					select {
					case found <- block.WithSeal(header):
						logger.Trace("Truehash nonce found and reported", "attempts", nonce-seed, "nonce", nonce)
					case <-abort:
						logger.Trace("Truehash nonce found but discarded", "attempts", nonce-seed, "nonce", nonce)
					}
					break search
				}

			} else {
				lastResult := result[16:]
				if header.FastNumber.Uint64() != 0 {
					if new(big.Int).SetBytes(lastResult).Cmp(fruitTarget) <= 0 {
						// last 128 bit < Dpf, get a fruit
						header = types.CopySnailHeader(header)
						header.Nonce = types.EncodeNonce(nonce)
						header.MixDigest = common.BytesToHash(digest)
						//TODO need add fruit flow
						header.Fruit = true

						// Seal and return a block (if still needed)
						select {
						case found <- block.WithSeal(header):
							logger.Trace("IsFruit nonce found and reported", "attempts", nonce-seed, "nonce", nonce)
						case <-abort:
							logger.Trace("IsFruit nonce found but discarded", "attempts", nonce-seed, "nonce", nonce)
						}
					}
				}
			}
			nonce++
		}
	}
	// Datasets are unmapped in a finalizer. Ensure that the dataset stays live
	// during sealing so it's not unmapped while being read.
	//runtime.KeepAlive(dataset)
}

func  (m *Minerva) truehashTableInit(tableLookup []uint64){

	var table [TBLSIZE*DATALENGTH*PMTSIZE]uint32

	for k := 0; k < TBLSIZE; k++	{
		for x := 0; x < DATALENGTH*PMTSIZE; x++	{
			table[k*DATALENGTH*PMTSIZE+x] = tableOrg[k][x]
		}
		//fmt.Printf("%d,", k+1)
	}
	genLookupTable(tableLookup[:], table[:]);
	//trueInit = 1
}

func (m *Minerva) CheckDataSetState(blockNum uint64) bool{
	dataset := m.dataset
	//blockNum := block.NumberU64()
	if dataset.dateInit == 0{
		if blockNum <= UPDATABLOCKLENGTH{
			m.truehashTableInit(dataset.evenDataset)
			dataset.dataset = &dataset.evenDataset
		}else{
			bn := (blockNum / UPDATABLOCKLENGTH -1 ) * UPDATABLOCKLENGTH + STARTUPDATENUM + 1
			in :=  (blockNum / UPDATABLOCKLENGTH) % 2
			//if blockNum > UPDATABLOCKLENGTH change lookutable form odd or even
			if in == 0{
				//set dataset.even
				dataset.dataset =  &dataset.evenDataset
				dataset.oddFlag = 0
				dataset.evenFlag = 0
			}else{
				//set dataset.odd
				dataset.dataset =  &dataset.oddDataset
				dataset.oddFlag = 0
				dataset.evenFlag = 0
			}
			m.updateLookupTBL( bn, *dataset.dataset)
		}
		dataset.dateInit = 1
	}

	if blockNum %UPDATABLOCKLENGTH >= STARTUPDATENUM {
		//start update lookuptable
		in :=  (blockNum / UPDATABLOCKLENGTH) % 2
		//change lookutable to odd or even
		if in == 0{
			//now is even, update odd.
			if dataset.oddFlag == 0 {
				res := m.updateLookupTBL(blockNum, dataset.oddDataset[:])
				if res {
					dataset.oddFlag = 1
				}else{
					return false
				}
			}
		}else{
			//now is odd, set dataset.even
			if dataset.evenFlag == 0 {
				res := m.updateLookupTBL(blockNum, dataset.evenDataset[:])
				if res {
					dataset.evenFlag = 1
				}else{
					return false
				}
			}
		}
	}
	if blockNum %UPDATABLOCKLENGTH == 1{
		in :=  (blockNum / UPDATABLOCKLENGTH) % 2
		//change lookutable form odd or even
		if in == 0{
			//set dataset.even
			dataset.dataset = &dataset.evenDataset
			dataset.evenFlag = 0
		}else{
			//set dataset.odd
			dataset.dataset = &dataset.oddDataset
			dataset.oddFlag = 0
		}
	}
	return true
}


func (m *Minerva) updateLookupTBL(blockNum uint64, plookup_tbl []uint64) bool{
	const offset_cnst = 0x1f
	const skip_cnst = 0x3
	var offset [32768]int
	var skip  [32768]int
	lktWz := uint32(DATALENGTH / 64)
	lktSz := uint32(DATALENGTH)*lktWz

	cur_block_num  := blockNum
	res := cur_block_num % UPDATABLOCKLENGTH
	sblockchain := m.sbc
	//current block number is invaild
	if res <= STARTUPDATENUM {
		return false
	}
	var st_block_num uint64 = uint64(cur_block_num - res)
	for i := 0; i < 8192; i++ {
		header := sblockchain.GetHeaderByNumber(uint64(i) + st_block_num)
		val := header.Hash().Bytes()
		offset[i*4]   = (int(val[0]) & offset_cnst) - 16
		offset[i*4+1] = (int(val[1]) & offset_cnst) - 16
		offset[i*4+2] = (int(val[2]) & offset_cnst) - 16
		offset[i*4+3] = (int(val[3]) & offset_cnst) - 16
	}

	for i := 0; i < 2048; i++ {
		header := sblockchain.GetHeaderByNumber(uint64(i) + st_block_num + uint64(8192))
		val := header.Hash().Bytes()
		for k:=0; k<16; k++{
			skip[i*16+k] = (int(val[k]) & skip_cnst) + 1
		}
	}

	for k := 0; k < TBLSIZE; k++ {

		plkt := uint32(k)*lktSz

		for x := 0; x < DATALENGTH; x++ {
			idx := k*DATALENGTH + x
			pos := offset[idx] + x
			sk := skip[idx]
			pos0 := pos - sk*PMTSIZE
			pos1 := pos + sk*PMTSIZE
			for y := pos0; y < pos1; y += sk {
				if y >= 0 && y < 2048 {
					vI := uint32(y / 64)
					vR := uint32(y % 64)
					plookup_tbl[plkt+vI] |= 1 << vR
				}
			}
			plkt += lktWz
		}
	}

	return true
}