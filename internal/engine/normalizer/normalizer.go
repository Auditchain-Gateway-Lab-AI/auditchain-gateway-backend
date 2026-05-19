package normalizer

import (
	"encoding/json"
	"errors"
	"fmt"
	"go-blockchain-api/internal/models"
	"time"

	"github.com/google/uuid"
)

// ClientFieldMapping adalah kamus pemetaan field khusus untuk klien tertentu yang diambil dari database berdasarkan ClientID yang sedang login.
type ClientFieldMapping struct {
	ActorField    string `json:"actor_field"`    // misal: "user_name"
	ActionField   string `json:"action_field"`   // misal: "event_type"
	ResourceField string `json:"resource_field"` // misal: "table_name"
}

// RawLogInput adalah representasi data mentah yang dikirim oleh sistem klien
type RawLogInput struct {
	ClientID             string                 `json:"-"`
	LogID                string                 `json:"log_id"`
	Actor                string                 `json:"actor"`
	Action               string                 `json:"action"`
	Resource             string                 `json:"resource"`
	Timestamp            string                 `json:"timestamp"`
	SourceSystem         string                 `json:"source_system"`
	AuthorizationContext map[string]interface{} `json:"authorization_context"`
	Metadata             map[string]interface{} `json:"metadata"`
}

// Fungsi ini menerjemahkan JSON dinamis dari klien menjadi RawLogInput yang baku
func MapDynamicPayload(dynamicPayload map[string]interface{}, mapping *ClientFieldMapping) (RawLogInput, error) {
	var input RawLogInput

	getString := func(key string) string {
		if val, ok := dynamicPayload[key]; ok {
			return fmt.Sprintf("%v", val)
		}
		return ""
	}

	// 1. Tentukan kunci (Mapping)
	keyActor := "actor"
	keyAction := "action"
	keyResource := "resource"

	if mapping != nil {
		if mapping.ActorField != "" {
			keyActor = mapping.ActorField
		}
		if mapping.ActionField != "" {
			keyAction = mapping.ActionField
		}
		if mapping.ResourceField != "" {
			keyResource = mapping.ResourceField
		}
	}

	// 2. Ekstraksi nilai
	input.Actor = getString(keyActor)
	input.Action = getString(keyAction)
	input.Resource = getString(keyResource)
	input.SourceSystem = getString("source_system")
	input.LogID = getString("log_id")
	input.Timestamp = getString("timestamp")

	// 3. Ekstraksi Metadata
	if metaVal, exists := dynamicPayload["metadata"]; exists {
		if metaMap, ok := metaVal.(map[string]interface{}); ok {
			input.Metadata = metaMap
		} else {
			// Jika metadata bukan map (misal string JSON), coba unmarshal
			// Ini untuk menangani kasus di mana klien mengirim metadata sebagai string
			var tempMap map[string]interface{}
			if err := json.Unmarshal([]byte(fmt.Sprintf("%v", metaVal)), &tempMap); err == nil {
				input.Metadata = tempMap
			}
		}
	}

	return input, nil
}

// Normalize mengubah RawLogInput menjadi models.AuditLog yang standar
// Normalize mengubah RawLogInput menjadi models.AuditLog yang standar
func Normalize(input RawLogInput) (*models.AuditLog, error) {
	// 1. Validasi manual tambahan jika diperlukan
	if input.Actor == "" || input.Action == "" || input.Resource == "" || input.SourceSystem == "" {
		// 👇 Menambahkan detail cetak nilai variabel
		errMsg := fmt.Sprintf("field wajib kosong! Isi terbaca -> Actor: '%s', Action: '%s', Resource: '%s', SourceSystem: '%s'",
			input.Actor, input.Action, input.Resource, input.SourceSystem)
		return nil, errors.New(errMsg)
	}

	// 2. Generate Log ID jika sistem eksternal tidak memberikannya
	logID := input.LogID
	if logID == "" {
		logID = uuid.New().String()
	}

	// 3. Normalisasi Timestamp
	logTime := time.Now()
	if input.Timestamp != "" {
		parsedTime, err := time.Parse(time.RFC3339, input.Timestamp)
		if err == nil {
			logTime = parsedTime
		}
	}

	// 4. Konversi interface{} (JSON Object) menjadi string agar deterministik saat di-hash
	authCtxBytes, _ := json.Marshal(input.AuthorizationContext)
	metaBytes, _ := json.Marshal(input.Metadata)

	// 5. Mapping ke Model Data Internal
	standardLog := &models.AuditLog{
		LogID:                logID,
		Actor:                input.Actor,
		Action:               input.Action,
		Resource:             input.Resource,
		Timestamp:            logTime,
		SourceSystem:         input.SourceSystem,
		AuthorizationContext: string(authCtxBytes),
		Metadata:             string(metaBytes),
		Status:               "RECEIVED",
	}

	return standardLog, nil
}
