package models

import "time"

// AuditLog merepresentasikan struktur metadata log transaksi
type AuditLog struct {
	LogID    string `gorm:"primaryKey;type:varchar(100)" json:"log_id"`
	ClientID string `gorm:"type:varchar(36);not null;index" json:"client_id"`

	Actor                string     `gorm:"type:varchar(100);index" json:"actor"`
	Action               string     `gorm:"type:varchar(100)" json:"action"`
	Resource             string     `gorm:"type:varchar(255)" json:"resource"`
	Timestamp            time.Time  `gorm:"index" json:"timestamp"`
	DBTimestamp          *time.Time `gorm:"index;autoCreateTime" json:"db_timestamp"`
	SourceSystem         string     `gorm:"type:varchar(100);index" json:"source_system"`
	AuthorizationContext string     `gorm:"type:text" json:"authorization_context"`
	Metadata             string     `gorm:"type:jsonb" json:"metadata"`

	// ID baris di audit_trail DB klien (diisi dari payload Agent, jika ada)
	SourceRecordID string `gorm:"type:varchar(100);index" json:"source_record_id"`

	// Elemen Kriptografi & Blockchain
	HashValue    string `gorm:"type:varchar(64);uniqueIndex" json:"hash_value"`
	PreviousHash string `gorm:"type:varchar(64)" json:"previous_hash"`

	// TESTING: MerkleRoot tetap dipertahankan sebagai kolom (supaya alur
	// verifikasi VerifyLogIntegrity yang membandingkan db_root vs chain_root
	// tidak perlu diubah), tapi nilainya sekarang diisi dengan HashValue
	// individual — BUKAN root dari agregasi banyak log. Merkle Tree tidak
	// lagi dibangun/dipakai di eksperimen ini.
	MerkleRoot     string  `gorm:"type:varchar(64);index" json:"merkle_root"`
	BlockchainTxID *string `gorm:"type:varchar(100)" json:"blockchain_tx_id"`

	// BlockchainTimestamp menyimpan waktu saat log ini di-anchor ke Fabric
	// (nilai sama dengan parameter timestamp yang dikirim ke chaincode).
	// Nil selama status belum ANCHORED. Dipakai untuk mengukur selisih
	// waktu terhadap DBTimestamp/Timestamp.
	BlockchainTimestamp *time.Time `gorm:"index" json:"blockchain_timestamp"`

	Status string `gorm:"type:varchar(20);default:'RECEIVED'" json:"status"`
}

type MerkleMetadata struct {
	TreeID         uint      `gorm:"primaryKey"`
	MerkleRoot     string    `gorm:"type:varchar(64);uniqueIndex"`
	BatchTimestamp time.Time `gorm:"autoCreateTime"`
	BatchSize      int       `gorm:"type:int"`
}

type MerkleProof struct {
	ID              uint   `gorm:"primaryKey"`
	TransactionHash string `gorm:"type:varchar(64);index"`
	SiblingHash     string `gorm:"type:varchar(64)"`
	TreeLevel       int    `gorm:"type:int"`
	MerkleRoot      string `gorm:"type:varchar(64);index"`
}
