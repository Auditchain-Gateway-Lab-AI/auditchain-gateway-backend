package crypto

import (
	"encoding/hex"

	"golang.org/x/crypto/sha3"
)

// MerkleProofData menyimpan informasi sibling hash untuk validasi
type MerkleProofData struct {
	SiblingHash string
	IsLeft      bool // true = sibling ada di kiri, false = sibling ada di kanan
	TreeLevel   int
}

// MerkleResult menyimpan output akhir agregasi
type MerkleResult struct {
	Root   string
	Proofs map[string][]MerkleProofData
}

// hashNodes menggabungkan dua hash menjadi parent menggunakan SHA3-256
func hashNodes(left, right string) string {
	leftBytes, _ := hex.DecodeString(left)
	rightBytes, _ := hex.DecodeString(right)
	combined := append(leftBytes, rightBytes...)
	hash := sha3.Sum256(combined)
	return hex.EncodeToString(hash[:])
}

// BuildMerkleTree membangun pohon secara berpasangan dan menghasilkan Root
func BuildMerkleTree(leafHashes []string) *MerkleResult {
	if len(leafHashes) == 0 {
		return nil
	}

	proofs := make(map[string][]MerkleProofData)
	for _, h := range leafHashes {
		proofs[h] = []MerkleProofData{}
	}

	currentLevel := leafHashes
	levelIndex := 0

	for len(currentLevel) > 1 {
		var nextLevel []string

		for i := 0; i < len(currentLevel); i += 2 {
			left := currentLevel[i]
			var right string

			if i+1 == len(currentLevel) {
				right = left
			} else {
				right = currentLevel[i+1]
			}

			// Simpan posisi sibling secara eksplisit (IsLeft)
			// agar VerifyMerkleProof bisa merekonstruksi secara deterministik
			if levelIndex == 0 {
				// Proof untuk node kiri: sibling-nya ada di kanan (IsLeft=false)
				proofs[left] = append(proofs[left], MerkleProofData{
					SiblingHash: right,
					IsLeft:      false,
					TreeLevel:   levelIndex,
				})
				// Proof untuk node kanan: sibling-nya ada di kiri (IsLeft=true)
				if left != right {
					proofs[right] = append(proofs[right], MerkleProofData{
						SiblingHash: left,
						IsLeft:      true,
						TreeLevel:   levelIndex,
					})
				}
			}

			parentHash := hashNodes(left, right)
			nextLevel = append(nextLevel, parentHash)
		}
		currentLevel = nextLevel
		levelIndex++
	}

	return &MerkleResult{
		Root:   currentLevel[0],
		Proofs: proofs,
	}
}

// VerifyMerkleProof merekonstruksi hash dari leaf ke root secara deterministik
// menggunakan informasi posisi IsLeft yang disimpan saat build.
func VerifyMerkleProof(transactionHash string, proofs []MerkleProofData, expectedRoot string) bool {
	currentHash := transactionHash

	for _, p := range proofs {
		if p.IsLeft {
			// Sibling ada di kiri: hash(sibling, current)
			currentHash = hashNodes(p.SiblingHash, currentHash)
		} else {
			// Sibling ada di kanan: hash(current, sibling)
			currentHash = hashNodes(currentHash, p.SiblingHash)
		}
	}

	return currentHash == expectedRoot
}
