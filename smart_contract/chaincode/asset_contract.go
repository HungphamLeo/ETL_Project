
package chaincode

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/hyperledger/fabric-chaincode-go/shim"
	"github.com/hyperledger/fabric-contract-api-go/v2/contractapi"
)

// SmartContract cung cấp các method để quản lý Asset trong world state.
// Thiết kế hướng tới: rõ ràng, an toàn tối thiểu (validate input), và có events để off-chain listener dùng.
type SmartContract struct {
	contractapi.Contract
}

// Asset biểu diễn một tài sản đơn giản
type Asset struct {
	ID             string `json:"ID"`
	Color          string `json:"Color"`
	Size           int    `json:"Size"`
	Owner          string `json:"Owner"`
	AppraisedValue int    `json:"AppraisedValue"`
}

// HistoryRecord lưu thông tin lịch sử thay đổi 1 key
type HistoryRecord struct {
	TxId      string `json:"txId"`
	Timestamp string `json:"timestamp"`
	IsDelete  bool   `json:"isDelete"`
	Asset     *Asset `json:"asset,omitempty"`
}

// ===================== Helper / Validation =====================

// validateID kiểm tra ID không rỗng
func validateID(id string) error {
	if len(id) == 0 {
		return fmt.Errorf("id must be a non-empty string")
	}
	return nil
}

// validateAssetValue kiểm tra các giá trị số hợp lý
func validateAssetValue(size int, value int) error {
	if size < 0 {
		return fmt.Errorf("size must be >= 0")
	}
	if value < 0 {
		return fmt.Errorf("appraised value must be >= 0")
	}
	return nil
}

// ===================== CRUD + Extra =====================

// CreateAsset tạo một asset mới vào world state.
// - Trả về error nếu ID đã tồn tại hoặc dữ liệu không hợp lệ.
// - Emit event `CreateAsset` với payload là asset JSON.
func (s *SmartContract) CreateAsset(ctx contractapi.TransactionContextInterface, id string, color string, size int, owner string, appraisedValue int) error {
	if err := validateID(id); err != nil {
		return err
	}
	if err := validateAssetValue(size, appraisedValue); err != nil {
		return err
	}
	exists, err := s.AssetExists(ctx, id)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("asset %s already exists", id)
	}

	asset := Asset{ID: id, Color: color, Size: size, Owner: owner, AppraisedValue: appraisedValue}
	assetJSON, err := json.Marshal(asset)
	if err != nil {
		return err
	}

	if err := ctx.GetStub().PutState(id, assetJSON); err != nil {
		return err
	}

	// Emit event for off-chain listeners
	_ = ctx.GetStub().SetEvent("CreateAsset", assetJSON)
	return nil
}

// ReadAsset đọc asset theo ID. Trả về *Asset hoặc lỗi khi không tồn tại.
func (s *SmartContract) ReadAsset(ctx contractapi.TransactionContextInterface, id string) (*Asset, error) {
	if err := validateID(id); err != nil {
		return nil, err
	}
	data, err := ctx.GetStub().GetState(id)
	if err != nil {
		return nil, fmt.Errorf("failed to read world state: %v", err)
	}
	if data == nil {
		return nil, fmt.Errorf("asset %s does not exist", id)
	}
	var asset Asset
	if err := json.Unmarshal(data, &asset); err != nil {
		return nil, err
	}
	return &asset, nil
}

// UpdateAsset cập nhật asset đã tồn tại. Trả lỗi nếu không tồn tại.
// Emit event `UpdateAsset`.
func (s *SmartContract) UpdateAsset(ctx contractapi.TransactionContextInterface, id string, color string, size int, owner string, appraisedValue int) error {
	if err := validateID(id); err != nil {
		return err
	}
	if err := validateAssetValue(size, appraisedValue); err != nil {
		return err
	}
	exists, err := s.AssetExists(ctx, id)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("asset %s does not exist", id)
	}
	asset := Asset{ID: id, Color: color, Size: size, Owner: owner, AppraisedValue: appraisedValue}
	b, err := json.Marshal(asset)
	if err != nil {
		return err
	}
	if err := ctx.GetStub().PutState(id, b); err != nil {
		return err
	}
	_ = ctx.GetStub().SetEvent("UpdateAsset", b)
	return nil
}

// DeleteAsset xóa asset khỏi world state. Emit event `DeleteAsset`.
func (s *SmartContract) DeleteAsset(ctx contractapi.TransactionContextInterface, id string) error {
	if err := validateID(id); err != nil {
		return err
	}
	exists, err := s.AssetExists(ctx, id)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("asset %s does not exist", id)
	}
	if err := ctx.GetStub().DelState(id); err != nil {
		return err
	}
	_ = ctx.GetStub().SetEvent("DeleteAsset", []byte(id))
	return nil
}

// AssetExists trả true nếu key tồn tại trong world state.
func (s *SmartContract) AssetExists(ctx contractapi.TransactionContextInterface, id string) (bool, error) {
	if err := validateID(id); err != nil {
		return false, err
	}
	data, err := ctx.GetStub().GetState(id)
	if err != nil {
		return false, fmt.Errorf("failed to read world state: %v", err)
	}
	return data != nil, nil
}

// TransferAsset chuyển ownership của asset. Emit event `TransferAsset` cùng payload mới.
func (s *SmartContract) TransferAsset(ctx contractapi.TransactionContextInterface, id string, newOwner string) error {
	if err := validateID(id); err != nil {
		return err
	}
	asset, err := s.ReadAsset(ctx, id)
	if err != nil {
		return err
	}
	asset.Owner = newOwner
	b, err := json.Marshal(asset)
	if err != nil {
		return err
	}
	if err := ctx.GetStub().PutState(id, b); err != nil {
		return err
	}
	_ = ctx.GetStub().SetEvent("TransferAsset", b)
	return nil
}

// GetAllAssets trả về tất cả assets (cẩn thận với số lượng lớn trong production).
func (s *SmartContract) GetAllAssets(ctx contractapi.TransactionContextInterface) ([]*Asset, error) {
	resultsIterator, err := ctx.GetStub().GetStateByRange("", "")
	if err != nil {
		return nil, err
	}
	defer resultsIterator.Close()

	var assets []*Asset
	for resultsIterator.HasNext() {
		qr, err := resultsIterator.Next()
		if err != nil {
			return nil, err
		}
		var asset Asset
		if err := json.Unmarshal(qr.Value, &asset); err != nil {
			// skip unparsable entry to be robust
			continue
		}
		assets = append(assets, &asset)
	}
	return assets, nil
}

// QueryAssetsByOwner lọc assets theo owner (simple filter trên key range; nếu dùng CouchDB thì có thể dùng rich query).
func (s *SmartContract) QueryAssetsByOwner(ctx contractapi.TransactionContextInterface, owner string) ([]*Asset, error) {
	if len(owner) == 0 {
		return nil, fmt.Errorf("owner must be a non-empty string")
	}
	resultsIterator, err := ctx.GetStub().GetStateByRange("", "")
	if err != nil {
		return nil, err
	}
	defer resultsIterator.Close()

	var assets []*Asset
	for resultsIterator.HasNext() {
		qr, err := resultsIterator.Next()
		if err != nil {
			return nil, err
		}
		var a Asset
		if err := json.Unmarshal(qr.Value, &a); err != nil {
			continue
		}
		if a.Owner == owner {
			assets = append(assets, &a)
		}
	}
	return assets, nil
}

// GetAssetHistory trả về lịch sử thay đổi giá trị cho key (txId, timestamp, isDelete, asset)
func (s *SmartContract) GetAssetHistory(ctx contractapi.TransactionContextInterface, id string) ([]*HistoryRecord, error) {
	if err := validateID(id); err != nil {
		return nil, err
	}
	historyIter, err := ctx.GetStub().GetHistoryForKey(id)
	if err != nil {
		return nil, err
	}
	defer historyIter.Close()

	var records []*HistoryRecord
	for historyIter.HasNext() {
		mod, err := historyIter.Next()
		if err != nil {
			return nil, err
		}
		rec := &HistoryRecord{TxId: mod.TxId, IsDelete: mod.IsDelete}
		if mod.Timestamp != nil {
			rec.Timestamp = time.Unix(mod.Timestamp.Seconds, int64(mod.Timestamp.Nanos)).UTC().Format(time.RFC3339)
		}
		if !mod.IsDelete && mod.Value != nil {
			var a Asset
			if err := json.Unmarshal(mod.Value, &a); err == nil {
				rec.Asset = &a
			}
		}
		records = append(records, rec)
	}
	return records, nil
}

// BatchCreateAssets nhận 1 JSON array string: [{"ID":"a1","Color":"blue","Size":5,"Owner":"Tom","AppraisedValue":100}] và lưu nhiều asset 1 lần.
// Trả lỗi nếu bất kỳ asset nào invalid hoặc trùng ID.
func (s *SmartContract) BatchCreateAssets(ctx contractapi.TransactionContextInterface, assetsJSON string) error {
	var assets []Asset
	if err := json.Unmarshal([]byte(assetsJSON), &assets); err != nil {
		return fmt.Errorf("invalid json payload: %v", err)
	}
	for _, a := range assets {
		if err := validateID(a.ID); err != nil {
			return fmt.Errorf("invalid id for one asset: %v", err)
		}
		if err := validateAssetValue(a.Size, a.AppraisedValue); err != nil {
			return err
		}
		exists, err := s.AssetExists(ctx, a.ID)
		if err != nil {
			return err
		}
		if exists {
			return fmt.Errorf("asset %s already exists", a.ID)
		}
		b, _ := json.Marshal(a)
		if err := ctx.GetStub().PutState(a.ID, b); err != nil {
			return err
		}
	}
	_ = ctx.GetStub().SetEvent("BatchCreateAssets", []byte(fmt.Sprintf("created=%d", len(assets))))
	return nil
}

