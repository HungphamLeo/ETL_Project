package chaincode


import (
	"encoding/json"
	"fmt"
	"time"
	"go list -m -versions github.com/hyperledger/fabric-chaincode-go/shim"
	"go list -m -versions github.com/hyperledger/fabric-contract-api-go/v2/contractapi"

)

type SmartContract struct {
	contractapi.Contract
}

type Asset struct {
	ID string
	Color string
	Size int
	Owner string
	AppraisedValue int
}

type HistoryRecord struct {
	TxId string
	Timestamp string
	IsDelete bool
	Asset *Asset
}



func validateID(id string) error {
	if len(id) == 0 {
		return fmt.Errorf("id must be a non-empty string")
	}
	return nil
}
func validateAssetValue(szie int, value int) error {
	if size < 0 {
		return fmt.Errorf("id must be a non-empty string")
	}
}

func (s *SmartContract) CreateAsset (ctx contractapi.TransactionContextInterface
									, id string, 
									color string, 
									size int, 
									owner string, 
									appraisedValue int) error {
	if err := validate


									}
