package main

import (
	"log"

	"github.com/hyperledger/fabric-contract-api-go/v2/contractapi"
	"your-project/chaincode"
)

func main() {
	chaincode, err := contractapi.NewChaincode(&chaincode.SmartContract{})
	if err != nil {
		log.Panicf("Error creating chaincode: %v", err)
	}
	if err := chaincode.Start(); err != nil {
		log.Panicf("Error starting chaincode: %v", err)
	}
}
```

> **Note:** sửa `module` / import path trong `go.mod` cho khớp với project của bạn.

---

## `go.mod` (ví dụ)

```text
module your-project

go 1.21

require (
	github.com/hyperledger/fabric-chaincode-go v0.0.0-... // tuỳ version local
	github.com/hyperledger/fabric-contract-api-go/v2 v2.0.0
)
```

(Thay `...` bằng phiên bản tương ứng bạn cài thông qua `go get` hoặc theo hướng dẫn Fabric docs.)

---

## Giải thích thiết kế (tại sao viết như vậy)

1. **Tách helper / validation**: để code dễ đọc, mọi kiểm tra input đều gom vào hàm nhỏ, dễ test.
2. **Emit event**: Khi contract thay đổi state, emit event cho backend/UI lắng nghe và cập nhật off-chain. Event rất cần cho hệ thống realtime.
3. **GetAllAssets vs QueryByOwner**: `GetAllAssets` trả mọi record (dễ test). `QueryAssetsByOwner` filter ở layer ứng dụng; nếu dùng CouchDB có thể build rich query (tối ưu) nhưng tôi giữ simple để người mới không bị rối.
4. **History**: Dùng `GetHistoryForKey` để debug & audit. Thấy rõ previous values and txId.
5. **BatchCreateAssets**: tiện cho import data test, ví dụ migrate CSV -> JSON -> batch create.
6. **Robustness**: Skip entries mà không parse được trong `GetAllAssets` để service không crash vì 1 record lỗi.

---

## Hướng dẫn chi tiết — dev → deploy → test (quickstart)

### Prerequisites

* Go 1.20+
* Docker & Docker Compose
* Fabric samples (test-network)
* Fabric binaries (peer, orderer) — tuân theo docs Hyperledger Fabric

### 1) Clone fabric-samples và start local test network

```bash
git clone https://github.com/hyperledger/fabric-samples.git
cd fabric-samples/test-network
./network.sh up createChannel -ca
```

### 2) Package & Deploy chaincode (ví dụ dùng `network.sh deployCC` helper)

Giả sử bạn đặt chaincode ở `../my-chaincode/chaincode-go`:

```bash
# từ fabric-samples/test-network
./network.sh deployCC -ccn basic -ccp ../my-chaincode/chaincode-go -ccl go
```

Hoặc theo lifecycle manual (package, install, approve, commit) — xem docs.

### 3) Test bằng peer CLI

**Invoke tạo 1 asset (soạn JSON args tuỳ tool)**

```bash
peer chaincode invoke -o localhost:7050 --ordererTLSHostnameOverride orderer.example.com \
  --tls --cafile $ORDERER_CA \
  -C mychannel -n basic \
  --peerAddresses localhost:7051 --tlsRootCertFiles $PEER0_ORG1_CA \
  -c '{"Args":["CreateAsset","asset1","blue","5","Alice","300"]}'
```

**Query**:

```bash
peer chaincode query -C mychannel -n basic -c '{"Args":["ReadAsset","asset1"]}'
```

Expected output (JSON):

```json
{"ID":"asset1","Color":"blue","Size":5,"Owner":"Alice","AppraisedValue":300}
```

### 4) Test history

```bash
peer chaincode query -C mychannel -n basic -c '{"Args":["GetAssetHistory","asset1"]}'
```

Expected: JSON array with txId/timestamp/asset snapshots.

### 5) Batch create (ví dụ từ client)

Gửi json array string làm arg cho `BatchCreateAssets` hoặc gọi qua Go Gateway.

### 6) Dùng Go client (Fabric Gateway) — ví dụ nhanh

Sử dụng snippet gateway (ở tài liệu trên). `contract.SubmitTransaction("CreateAsset", "asset2","red","3","Bob","200")` và `contract.EvaluateTransaction("ReadAsset","asset2")`.

---

## Demo kết quả từng hàm (ví dụ đầu ra)

* `CreateAsset("a1","blue",5,"Tom",100)` → trả `nil` nếu thành công; world state có key `a1` chứa `{"ID":"a1","Color":"blue"...}`; event `CreateAsset` phát.
* `ReadAsset("a1")` → trả struct Asset (JSON khi query CLI).
* `UpdateAsset("a1",...,...)` → cập nhật giá trị; event `UpdateAsset` phát.
* `DeleteAsset("a1")` → xóa key; event `DeleteAsset` phát.
* `TransferAsset("a1","Jerry")` → owner thay đổi; event `TransferAsset` phát.
* `GetAllAssets()` → trả array JSON của tất cả asset.
* `QueryAssetsByOwner("Tom")` → trả array assets của owner Tom.
* `GetAssetHistory("a1")` → trả array history record (txId, timestamp, asset snapshot).
* `BatchCreateAssets('[{...},{...}]')` → tạo nhiều assets; event `BatchCreateAssets` (payload created=N).

---

## Unit test mẫu (mock)

```go
package chaincode_test

import (
	"encoding/json"
	"testing"

	"github.com/hyperledger/fabric-chaincode-go/shim"
	"github.com/hyperledger/fabric-contract-api-go/v2/contractapi"
	"github.com/stretchr/testify/require"

	"your-project/chaincode"
)

func TestCreateReadAsset(t *testing.T) {
	cc := new(chaincode.SmartContract)
	stub := shim.NewMockStub("mockStub", contractapi.NewChaincode(cc))

	// Invoke CreateAsset via MockStub
	resp := stub.MockInvoke("1", [][]byte{[]byte("CreateAsset"), []byte("asset1"), []byte("blue"), []byte("5"), []byte("Alice"), []byte("300")})
	require.Equal(t, int32(200), resp.Status, string(resp.Message))

	// Read by calling contract function directly
	// (or use stub.MockInvoke to call ReadAsset)
	respQuery := stub.MockInvoke("1", [][]byte{[]byte("ReadAsset"), []byte("asset1")})
	require.Equal(t, int32(200), respQuery.Status)

	var a chaincode.Asset
	err := json.Unmarshal(respQuery.Payload, &a)
	require.NoError(t, err)
	require.Equal(t, "asset1", a.ID)
}
```


