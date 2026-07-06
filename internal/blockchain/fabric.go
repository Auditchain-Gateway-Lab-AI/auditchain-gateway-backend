package blockchain

import (
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"go-blockchain-api/internal/models"

	"github.com/google/uuid"
	"github.com/hyperledger/fabric-gateway/pkg/client"
	"github.com/hyperledger/fabric-gateway/pkg/identity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"gorm.io/gorm"
)

type FabricService struct {
	Contract *client.Contract
	gw       *client.Gateway
	conn     *grpc.ClientConn
	DB       *gorm.DB
}

// InitFabricGateway menginisialisasi koneksi ke jaringan Fabric
func InitFabricGateway(db *gorm.DB) (*FabricService, error) {
	mspID := os.Getenv("FABRIC_MSP_ID")
	peerEndpoint := os.Getenv("FABRIC_PEER_ENDPOINT")

	tlsCertPath := os.Getenv("FABRIC_TLS_CERT_PATH")
	certPool := x509.NewCertPool()
	tlsCert, err := os.ReadFile(filepath.Clean(tlsCertPath))
	if err != nil {
		return nil, fmt.Errorf("gagal membaca TLS cert: %v", err)
	}
	certPool.AppendCertsFromPEM(tlsCert)
	transportCredentials := credentials.NewClientTLSFromCert(certPool, "")

	conn, err := grpc.NewClient(peerEndpoint, grpc.WithTransportCredentials(transportCredentials))
	if err != nil {
		return nil, fmt.Errorf("gagal membuat koneksi gRPC: %v", err)
	}

	certPath := os.Getenv("FABRIC_CERT_PATH")
	certBytes, err := os.ReadFile(filepath.Clean(certPath))
	if err != nil {
		return nil, fmt.Errorf("gagal membaca sertifikat: %v", err)
	}
	certBlock, _ := pem.Decode(certBytes)
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("gagal parsing sertifikat: %v", err)
	}
	id, err := identity.NewX509Identity(mspID, cert)
	if err != nil {
		return nil, err
	}

	keyPath := os.Getenv("FABRIC_KEY_PATH")
	keyBytes, err := os.ReadFile(filepath.Clean(keyPath))
	if err != nil {
		return nil, fmt.Errorf("gagal membaca private key: %v", err)
	}
	keyBlock, _ := pem.Decode(keyBytes)
	privateKey, err := x509.ParsePKCS8PrivateKey(keyBlock.Bytes)
	if err != nil {
		privateKey, err = x509.ParseECPrivateKey(keyBlock.Bytes)
		if err != nil {
			return nil, fmt.Errorf("gagal parsing private key: %v", err)
		}
	}
	sign, err := identity.NewPrivateKeySign(privateKey)
	if err != nil {
		return nil, err
	}

	gw, err := client.Connect(
		id,
		client.WithSign(sign),
		client.WithClientConnection(conn),
		client.WithEvaluateTimeout(5*time.Second),
		client.WithEndorseTimeout(15*time.Second),
	)
	if err != nil {
		return nil, err
	}

	network := gw.GetNetwork(os.Getenv("FABRIC_CHANNEL"))
	contract := network.GetContract(os.Getenv("FABRIC_CHAINCODE"))

	log.Println("✅ Terhubung ke Hyperledger Fabric Gateway!")

	return &FabricService{
		Contract: contract,
		gw:       gw,
		conn:     conn,
		DB:       db,
	}, nil
}

// AnchorSingleHash mem-anchor SATU log secara langsung menggunakan
// HashValue individual (BUKAN Merkle Root batch). Dipanggil event-driven
// dari kafkaconsumer.Engine tepat setelah log berhasil disimpan sebagai
// HASHED — bukan menunggu ticker atau agregasi Merkle Tree.
//
// TESTING PURPOSE: memungkinkan pengukuran selisih waktu murni antara
// db_timestamp (gateway) dan blockchain_timestamp (on-chain) per log
// individual, tanpa distorsi dari batching.
func (f *FabricService) AnchorSingleHash(logItem *models.AuditLog) error {
	anchorID := uuid.New().String()

	// anchorTime dipakai untuk DUA hal: (1) parameter timestamp yang
	// dikirim ke chaincode, dan (2) nilai kolom blockchain_timestamp di DB.
	// Sengaja satu variabel yang sama agar keduanya identik persis.
	anchorTime := time.Now()
	timestamp := anchorTime.Format(time.RFC3339Nano)
	sourceGateway := "AuditChain_Gateway_Node1"

	// batchSize selalu "1" karena setiap log di-anchor sendiri-sendiri
	_, err := f.Contract.SubmitTransaction("StoreMerkleRoot", anchorID, logItem.HashValue, timestamp, sourceGateway, "1", "System_Signature")
	if err != nil {
		log.Printf("[Anchoring-Direct] ❌ Gagal mengirim ke Fabric untuk log %s: %v\n", logItem.LogID, err)
		return err
	}

	blockchainTxID := anchorID

	err = f.DB.Model(&models.AuditLog{}).
		Where("log_id = ?", logItem.LogID).
		Updates(map[string]interface{}{
			"status":               "ANCHORED",
			"blockchain_tx_id":     blockchainTxID,
			"blockchain_timestamp": anchorTime,
			// merkle_root diisi HashValue individual — bukan hasil agregasi.
			// Ini menjaga kompatibilitas VerifyLogIntegrity (Lapis 4) yang
			// membandingkan auditLog.MerkleRoot vs fabricResponse.MerkleRoot.
			"merkle_root": logItem.HashValue,
		}).Error

	if err != nil {
		log.Printf("[Anchoring-Direct] ⚠️  Gagal update DB untuk log %s: %v\n", logItem.LogID, err)
		return err
	}

	log.Printf("[Anchoring-Direct] ✅ Sukses! log=%s Hash=%s TxID=%s", logItem.LogID, logItem.HashValue, blockchainTxID)
	return nil
}

// GetAnchorFromLedger menarik data yang tersimpan di dalam jaringan Fabric
func (f *FabricService) GetAnchorFromLedger(anchorID string) (string, error) {
	resultBytes, err := f.Contract.EvaluateTransaction("QueryMerkleRoot", anchorID)
	if err != nil {
		return "", err
	}
	return string(resultBytes), nil
}

func (f *FabricService) Close() {
	if f.gw != nil {
		f.gw.Close()
	}
	if f.conn != nil {
		f.conn.Close()
	}
}
