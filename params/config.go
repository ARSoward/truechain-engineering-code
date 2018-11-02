// Copyright 2016 The go-ethereum Authors
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

package params

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"

	"github.com/truechain/truechain-engineering-code/common"
	"github.com/truechain/truechain-engineering-code/common/math"
)

// Genesis hashes to enforce below configs on.
var (
	MainnetGenesisHash  = common.HexToHash("0x285dec13c2d2d18d8a5cf3665aa8e003d211a3ed01babbbad740eccf56336666")
	TestnetGenesisHash  = common.HexToHash("0xcf49d73c99b1c47340b7f302c952f6457f1c0de1fd5b3252bd4707399827134f")
	TestnetGenesisFHash = common.HexToHash("0xcf49d73c99b1c47340b7f302c952f6457f1c0de1fd5b3252bd4707399827134f")
	TestnetGenesisSHash = common.HexToHash("0x00bffb8790c8e80c3b6ca76ab6986d30fdd28f67cc4a9dff2cc054e9cb1a1b09")
)

var (
	// MainnetChainConfig is the chain parameters to run a node on the main network.
	MainnetChainConfig = &ChainConfig{
		ChainID: big.NewInt(1),
		Minerva: &(MinervaConfig{
			MinimumDifficulty:      big.NewInt(2000000),
			MinimumFruitDifficulty: big.NewInt(2000),
			DurationLimit:          big.NewInt(600),
		}),
	}

	// TestnetChainConfig contains the chain parameters to run a node on the Ropsten test network.
	TestnetChainConfig = &ChainConfig{
		ChainID: big.NewInt(18928),
		Minerva: &(MinervaConfig{
			MinimumDifficulty:      big.NewInt(2000000),
			MinimumFruitDifficulty: big.NewInt(2000),
			DurationLimit:          big.NewInt(600),
		}),
	}

	// RinkebyChainConfig contains the chain parameters to run a node on the Rinkeby test network.
	// RinkebyChainConfig = &ChainConfig{
	// 	ChainID: big.NewInt(100),
	// 	Clique: &CliqueConfig{
	// 		Period: 15,
	// 		Epoch:  30000,
	// 	},
	// }

	// AllEthashProtocolChanges contains every protocol change (EIPs) introduced
	// and accepted by the Ethereum core developers into the Ethash consensus.
	//
	// This configuration is intentionally not using keyed fields to force anyone
	// adding flags to the config to also have to set these fields.
	AllMinervaProtocolChanges = &ChainConfig{big.NewInt(1337), new(MinervaConfig)}

	// AllCliqueProtocolChanges contains every protocol change (EIPs) introduced
	// and accepted by the Ethereum core developers into the Clique consensus.
	//
	// This configuration is intentionally not using keyed fields to force anyone
	// adding flags to the config to also have to set these fields.
	// AllCliqueProtocolChanges = &ChainConfig{big.NewInt(1337), nil, &CliqueConfig{Period: 0, Epoch: 30000}}

	TestChainConfig = &ChainConfig{big.NewInt(1), new(MinervaConfig)}
)

// ChainConfig is the core config which determines the blockchain settings.
//
// ChainConfig is stored in the database on a per block basis. This means
// that any network, identified by its genesis block, can have its own
// set of configuration options.
type ChainConfig struct {
	ChainID *big.Int `json:"chainId"` // chainId identifies the current chain and is used for replay protection

	// Various consensus engines
	Minerva *MinervaConfig `json:"minerva,omitempty"`
	//Clique *CliqueConfig  `json:"clique,omitempty"`
}

// MinervaConfig is the consensus engine configs for proof-of-work based sealing.
type MinervaConfig struct {
	MinimumDifficulty      *big.Int `json:"minimumDifficulty,omitempty"`
	MinimumFruitDifficulty *big.Int `json:"minimumFruitDifficulty,omitempty"`
	DurationLimit          *big.Int `json:"durationLimit,omitempty"`
}

func (c *MinervaConfig) UnmarshalJSON(input []byte) error {
	type MinervaConfig struct {
		MinimumDifficulty      *math.HexOrDecimal256 `json:"minimumDifficulty,omitempty"`
		MinimumFruitDifficulty *math.HexOrDecimal256 `json:"minimumFruitDifficulty,omitempty"`
		DurationLimit          *math.HexOrDecimal256 `json:"durationLimit,omitempty"`
	}
	var dec MinervaConfig
	if err := json.Unmarshal(input, &dec); err != nil {
		return err
	}
	if dec.MinimumDifficulty == nil {
		return errors.New("missing required field 'MinimumDifficulty' for Genesis")
	}
	c.MinimumDifficulty = (*big.Int)(dec.MinimumDifficulty)
	if dec.MinimumFruitDifficulty == nil {
		return errors.New("missing required field 'MinimumFruitDifficulty' for Genesis")
	}
	c.MinimumFruitDifficulty = (*big.Int)(dec.MinimumFruitDifficulty)
	if dec.DurationLimit == nil {
		return errors.New("missing required field 'DurationLimit' for Genesis")
	}
	c.DurationLimit = (*big.Int)(dec.DurationLimit)
	return nil
}

func (c MinervaConfig) MarshalJSON() ([]byte, error) {
	type MinervaConfig struct {
		MinimumDifficulty      *math.HexOrDecimal256 `json:"minimumDifficulty,omitempty"`
		MinimumFruitDifficulty *math.HexOrDecimal256 `json:"minimumFruitDifficulty,omitempty"`
		DurationLimit          *math.HexOrDecimal256 `json:"durationLimit,omitempty"`
	}
	var enc MinervaConfig
	enc.MinimumDifficulty = (*math.HexOrDecimal256)(c.MinimumDifficulty)
	enc.MinimumFruitDifficulty = (*math.HexOrDecimal256)(c.MinimumFruitDifficulty)
	enc.DurationLimit = (*math.HexOrDecimal256)(c.DurationLimit)
	return json.Marshal(&enc)
}

// String implements the stringer interface, returning the consensus engine details.
func (c *MinervaConfig) String() string {
	return "minerva"
}

// CliqueConfig is the consensus engine configs for proof-of-authority based sealing.
// type CliqueConfig struct {
// 	Period uint64 `json:"period"` // Number of seconds between blocks to enforce
// 	Epoch  uint64 `json:"epoch"`  // Epoch length to reset votes and checkpoint
// }

// // String implements the stringer interface, returning the consensus engine details.
// func (c *CliqueConfig) String() string {
// 	return "clique"
// }

// String implements the fmt.Stringer interface.
func (c *ChainConfig) String() string {
	var engine interface{}
	switch {
	case c.Minerva != nil:
		engine = c.Minerva
	// case c.Clique != nil:
	// 	engine = c.Clique
	default:
		engine = "unknown"
	}
	return fmt.Sprintf("{ChainID: %v Engine: %v}",
		c.ChainID,
		engine,
	)
}

// GasTable returns the gas table corresponding to the current phase (homestead or homestead reprice).
//
// The returned GasTable's fields shouldn't, under any circumstances, be changed.
func (c *ChainConfig) GasTable(num *big.Int) GasTable {
	return GasTableEIP158
}

// CheckCompatible checks whether scheduled fork transitions have been imported
// with a mismatching chain configuration.
func (c *ChainConfig) CheckCompatible(newcfg *ChainConfig, height uint64) *ConfigCompatError {
	bhead := new(big.Int).SetUint64(height)

	// Iterate checkCompatible to find the lowest conflict.
	var lasterr *ConfigCompatError
	for {
		err := c.checkCompatible(newcfg, bhead)
		if err == nil || (lasterr != nil && err.RewindTo == lasterr.RewindTo) {
			break
		}
		lasterr = err
		bhead.SetUint64(err.RewindTo)
	}
	return lasterr
}

func (c *ChainConfig) checkCompatible(newcfg *ChainConfig, head *big.Int) *ConfigCompatError {
	return nil
}

// isForkIncompatible returns true if a fork scheduled at s1 cannot be rescheduled to
// block s2 because head is already past the fork.
func isForkIncompatible(s1, s2, head *big.Int) bool {
	return (isForked(s1, head) || isForked(s2, head)) && !configNumEqual(s1, s2)
}

// isForked returns whether a fork scheduled at block s is active at the given head block.
func isForked(s, head *big.Int) bool {
	if s == nil || head == nil {
		return false
	}
	return s.Cmp(head) <= 0
}

func configNumEqual(x, y *big.Int) bool {
	if x == nil {
		return y == nil
	}
	if y == nil {
		return x == nil
	}
	return x.Cmp(y) == 0
}

// ConfigCompatError is raised if the locally-stored blockchain is initialised with a
// ChainConfig that would alter the past.
type ConfigCompatError struct {
	What string
	// block numbers of the stored and new configurations
	StoredConfig, NewConfig *big.Int
	// the block number to which the local chain must be rewound to correct the error
	RewindTo uint64
}

func newCompatError(what string, storedblock, newblock *big.Int) *ConfigCompatError {
	var rew *big.Int
	switch {
	case storedblock == nil:
		rew = newblock
	case newblock == nil || storedblock.Cmp(newblock) < 0:
		rew = storedblock
	default:
		rew = newblock
	}
	err := &ConfigCompatError{what, storedblock, newblock, 0}
	if rew != nil && rew.Sign() > 0 {
		err.RewindTo = rew.Uint64() - 1
	}
	return err
}

func (err *ConfigCompatError) Error() string {
	return fmt.Sprintf("mismatching %s in database (have %d, want %d, rewindto %d)", err.What, err.StoredConfig, err.NewConfig, err.RewindTo)
}

// Rules wraps ChainConfig and is merely syntatic sugar or can be used for functions
// that do not have or require information about the block.
//
// Rules is a one time interface meaning that it shouldn't be used in between transition
// phases.
type Rules struct {
	ChainID *big.Int
}

// Rules ensures c's ChainID is not nil.
func (c *ChainConfig) Rules(num *big.Int) Rules {
	chainID := c.ChainID
	if chainID == nil {
		chainID = new(big.Int)
	}
	return Rules{ChainID: new(big.Int).Set(chainID)}
}
