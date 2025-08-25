package main

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"log"
	"math/big"
	"os"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
)

// Import bindings từ file SimpleStorage.go
// import "path/to/SimpleStorage.go"

// SPDX-License-Identifier: MIT


func main() {
	// 1. Kết nối tới node Ethereum (ví dụ: testnet Ropsten hoặc Ganache)
	client, err := ethclient.Dial("http://127.0.0.1:8545")
	if err != nil {
		log.Fatalf("Failed to connect to the Ethereum client: %v", err)
	}

	// 2. Tải khóa riêng của tài khoản
	privateKey, err := crypto.HexToECDSA(os.Getenv("PRIVATE_KEY"))
	if err != nil {
		log.Fatalf("Failed to load private key: %v", err)
	}

	publicKey := privateKey.Public()
	publicKeyECDSA, ok := publicKey.(*ecdsa.PublicKey)
	if !ok {
		log.Fatal("Cannot assert type: publicKey is not of type *ecdsa.PublicKey")
	}

	fromAddress := crypto.PubkeyToAddress(*publicKeyECDSA)

	// 3. Lấy nonce
	nonce, err := client.PendingNonceAt(context.Background(), fromAddress)
	if err != nil {
		log.Fatalf("Failed to get nonce: %v", err)
	}

	// 4. Lấy gas price
	gasPrice, err := client.SuggestGasPrice(context.Background())
	if err != nil {
		log.Fatalf("Failed to get gas price: %v", err)
	}

	// 5. Tạo TransactOpts
	auth := bind.NewKeyedTransactor(privateKey)
	auth.Nonce = big.NewInt(int64(nonce))
	auth.Value = big.NewInt(0)      // 0 Ether
	auth.GasLimit = uint64(3000000) // Đặt gas limit
	auth.GasPrice = gasPrice

	// 6. Triển khai hợp đồng
	address, tx, instance, err := DeploySimpleStorage(auth, client)
	if err != nil {
		log.Fatalf("Failed to deploy new token contract: %v", err)
	}

	fmt.Println("Contract deployed at address:", address.Hex())
	fmt.Println("Transaction hash:", tx.Hash().Hex())

	// 7. Gọi hàm trên hợp đồng
	_ = instance
	// Ví dụ: gọi hàm setNumber(123)
	tx, err = instance.SetNumber(auth, big.NewInt(123))
	if err != nil {
		log.Fatalf("Failed to call SetNumber: %v", err)
	}

	fmt.Println("SetNumber transaction hash:", tx.Hash().Hex())

	// Ví dụ: gọi hàm getNumber()
	// Lưu ý: hàm view không cần tx, chỉ cần call
	// number, err := instance.GetNumber(&bind.CallOpts{})
	// if err != nil {
	//     log.Fatalf("Failed to call GetNumber: %v", err)
	// }
	// fmt.Println("Current number is:", number.String())
}
