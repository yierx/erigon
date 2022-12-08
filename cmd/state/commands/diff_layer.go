package commands

import (
	"bytes"
	"fmt"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/rlp"
	"io"
	"math/big"
)

type DiffLayer struct {
	Accounts  []DiffAccount
	Codes     []DiffCode
	Destructs []common.Address
	Storages  []DiffStorage
}

type DiffCode struct {
	Hash common.Hash
	Code []byte
}

type DiffAccount struct {
	Account common.Address
	Blob    []byte
}

type DiffStorage struct {
	Account common.Address
	Keys    []string
	Vals    [][]byte
}

type Account struct {
	Nonce    uint64
	Balance  *big.Int
	Root     []byte
	CodeHash []byte
}

type accountRLPShadow Account

var (
	// EmptyRoot is the known root hash of an empty trie.
	EmptyRoot = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")

	// EmptyCodeHash is the known hash of the empty EVM bytecode.
	EmptyCodeHash = crypto.Keccak256Hash(nil)
)

func (a *Account) EncodeRLP(w io.Writer) error {
	a1 := accountRLPShadow(*a)
	if bytes.Equal(a1.Root, EmptyRoot.Bytes()) {
		a1.Root = nil
	}
	if bytes.Equal(a1.CodeHash, EmptyCodeHash.Bytes()) {
		a1.CodeHash = nil
	}

	return rlp.Encode(w, a1)
}

// Implements core/state/StateWriter to provide state diffs
type DiffLayerWriter struct {
	layer DiffLayer

	dirtyCodeAddress map[common.Address]struct{}
	storageSlot      map[common.Address]int
}

func NewDiffLayerWriter() *DiffLayerWriter {
	return &DiffLayerWriter{
		dirtyCodeAddress: map[common.Address]struct{}{},
		storageSlot:      map[common.Address]int{},
	}
}

func AccountEqual(original, account *accounts.Account) bool {
	return original.Nonce == account.Nonce && original.CodeHash == account.CodeHash &&
		original.Balance == account.Balance && original.Incarnation == account.Incarnation &&
		original.Root == account.Root
}

func (dlw *DiffLayerWriter) UpdateAccountData(address common.Address, original, account *accounts.Account) error {
	if _, ok := dlw.storageSlot[address]; AccountEqual(original, account) && !ok {
		return nil
	}

	acc := &Account{
		Nonce:    account.Nonce,
		Balance:  account.Balance.ToBig(),
		Root:     account.Root.Bytes(),
		CodeHash: account.CodeHash.Bytes(),
	}

	accData, err := rlp.EncodeToBytes(acc)
	if err != nil || len(accData) == 0 {
		panic(fmt.Errorf("encode failed: %v", err))
	}
	dlw.layer.Accounts = append(dlw.layer.Accounts, DiffAccount{
		Account: address,
		Blob:    accData,
	})
	return nil
}

func (dlw *DiffLayerWriter) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	dlw.layer.Codes = append(dlw.layer.Codes, DiffCode{
		Hash: codeHash,
		Code: code,
	})
	dlw.dirtyCodeAddress[address] = struct{}{}
	return nil
}

func (dlw *DiffLayerWriter) DeleteAccount(address common.Address, original *accounts.Account) error {
	if original == nil || !original.Initialised {
		return nil
	}

	dlw.layer.Destructs = append(dlw.layer.Destructs, address)
	return nil
}

func (dlw *DiffLayerWriter) WriteAccountStorage(address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	if *original == *value {
		return nil
	}

	idx, ok := dlw.storageSlot[address]
	if !ok {
		idx = len(dlw.layer.Storages)
		dlw.storageSlot[address] = idx
		dlw.layer.Storages = append(dlw.layer.Storages, DiffStorage{
			Account: address,
			Keys:    nil,
			Vals:    nil,
		})
	}

	if address != dlw.layer.Storages[idx].Account {
		panic("account mismatch")
	}

	dlw.layer.Storages[idx].Keys = append(dlw.layer.Storages[idx].Keys, string(key.Bytes()))
	val := value.Bytes()
	if len(val) > 0 {
		val, _ = rlp.EncodeToBytes(val)
	}
	dlw.layer.Storages[idx].Vals = append(dlw.layer.Storages[idx].Vals, val)
	return nil
}

func (dlw *DiffLayerWriter) CreateContract(address common.Address) error {
	dlw.dirtyCodeAddress[address] = struct{}{}
	return nil
}

func (dlw *DiffLayerWriter) GetData() []byte {
	diffAccountMap := make(map[common.Address]struct{})
	for _, acc := range dlw.layer.Accounts {
		delete(dlw.dirtyCodeAddress, acc.Account)
		diffAccountMap[acc.Account] = struct{}{}
	}

	if len(dlw.dirtyCodeAddress) != 0 {
		panic("diff account missing")
	}

	for _, stor := range dlw.layer.Storages {
		if _, ok := diffAccountMap[stor.Account]; !ok {
			panic("diff account x missing")
		}
	}

	data, _ := rlp.EncodeToBytes(dlw.layer)
	return data
}
