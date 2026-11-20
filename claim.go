package kolobraid

import (
	"encoding/binary"
	"fmt"

	kolostore "github.com/ciranda-dev/kolo-store"

	"github.com/ciranda-dev/kolo-braid/internal/dag"
	"github.com/ciranda-dev/kolo-braid/internal/settle"
)

// maxClaimsPerSeat bounds the uncertified positions one seat can claim, so a seat that claims positions nobody reaches costs
// a fixed amount: 16 checkpoints is 16 x CheckpointEvery settled positions of lag.
const maxClaimsPerSeat = 16

func claimKey(pos uint64, key [32]byte) []byte { return append(be8(keyClaim, pos), key[:]...) }

// noteClaim counts a seat's first claim for a position; q equal claims certify the position.
func (b *Braid) noteClaim(n *node, c pulseClaim, bt kolostore.Batch) {
	cfg := b.st.Config()
	k := key32(n.writer)
	if !seatSet(cfg.Seats)[settle.Seat{Key: k, Strand: dag.StrandID(n.strand)}] {
		return
	}
	if _, done := b.certs[c.Position]; done {
		if d := b.certs[c.Position]; d != c.Digest {
			b.logf("kolobraid: a seat claimed another digest than the certified one", "position", c.Position)
		}
		return
	}
	m := b.claims[c.Position]
	if _, ok := m[k]; ok {
		return
	}
	if m == nil {
		count := 0
		for _, other := range b.claims {
			if _, ok := other[k]; ok {
				count++
			}
		}
		if count >= maxClaimsPerSeat {
			return
		}
		m = map[[32]byte]Digest{}
		b.claims[c.Position] = m
	}
	m[k] = c.Digest
	bt.Put(claimKey(c.Position, k), c.Digest[:])
	same := 0
	for _, d := range m {
		if d == c.Digest {
			same++
		}
	}
	if same >= cfg.Quorum() {
		b.certify(c.Position, c.Digest, bt)
	}
}

// certify records a digest that q seats claimed and has the applier compare it with this peer's own.
func (b *Braid) certify(pos uint64, d Digest, bt kolostore.Batch) {
	for k, x := range b.claims[pos] {
		if x != d {
			b.logf("kolobraid: a seat claimed another digest than the certified one", "position", pos)
		}
		bt.Delete(claimKey(pos, k))
	}
	delete(b.claims, pos)
	b.certs[pos] = d
	b.jobs = append(b.jobs, &verifyJob{pos: pos, d: d})
}

// onCheckpoint is called by the applier when a settled batch ends at a checkpoint: a seat claims its digest in its next pulse,
// and a certified digest that was waiting for this position is compared now.
func (b *Braid) onCheckpoint(cp checkpointRec) {
	if _, _, ok := b.mySeat(); ok {
		c := pulseClaim(cp)
		b.myClaim = &c
		var e [41]byte
		n := binary.PutUvarint(e[:], c.Position)
		_ = b.db.Put([]byte(keyMyClaim), append(e[:n:n], c.Digest[:]...))
		b.signal()
	}
	if d, ok := b.certs[cp.Position]; ok {
		b.jobs = append(b.jobs, &verifyJob{pos: cp.Position, d: d})
		b.cond.Broadcast()
	}
}

func (b *Braid) clearMyClaim() {
	b.myClaim = nil
	_ = b.db.Delete([]byte(keyMyClaim))
}

// loadClaims reads the stored claims and the own pending claim at Open.
func (b *Braid) loadClaims() error {
	if err := scanPrefix(b.db, []byte(keyClaim), func(k, v []byte) error {
		if len(k) != len(keyClaim)+8+32 || len(v) != hashSize {
			return fmt.Errorf("%w: claim record", ErrCorrupt)
		}
		pos := beUint(k[len(keyClaim) : len(keyClaim)+8])
		m := b.claims[pos]
		if m == nil {
			m = map[[32]byte]Digest{}
			b.claims[pos] = m
		}
		m[[32]byte(k[len(keyClaim)+8:])] = Digest(v)
		return nil
	}); err != nil {
		return err
	}
	v, err := getOpt(b.db, []byte(keyMyClaim))
	if err != nil || v == nil {
		return err
	}
	pos, n := binary.Uvarint(v)
	if n <= 0 || len(v) != n+hashSize {
		return fmt.Errorf("%w: own claim record", ErrCorrupt)
	}
	b.myClaim = &pulseClaim{Position: pos, Digest: Digest(v[n:])}
	return nil
}
