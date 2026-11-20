package devices

import (
	"errors"
	"fmt"

	kolocodec "github.com/ciranda-dev/kolo-codec"
	koloid "github.com/ciranda-dev/kolo-id"

	kolobraid "github.com/ciranda-dev/kolo-braid"
)

// tag starts the body of every roster entry.
const tag = "kolo-braid/devices/v0"

const (
	opCert       = 1
	opRevocation = 2
)

// record is one decoded roster entry body.
type record struct {
	op     uint64
	cert   *koloid.DeviceCert
	rev    *koloid.Revocation
	target *koloid.DeviceCert // the certificate an approver-signed revocation names
}

// isRecord reports whether body starts with the roster tag, so that Apply handles it.
func isRecord(body []byte) bool {
	t, err := kolocodec.NewDecoder(body).String(len(tag))
	return err == nil && t == tag
}

func malformed(why string, err error) error {
	if err != nil {
		return fmt.Errorf("%w: roster entry: %s: %w", kolobraid.ErrMalformed, why, err)
	}
	return fmt.Errorf("%w: roster entry: %s", kolobraid.ErrMalformed, why)
}

// decodeRecord decodes a roster entry body in its canonical form; anything else wraps kolobraid.ErrMalformed.
func decodeRecord(body []byte) (record, error) {
	d := kolocodec.NewDecoder(body)
	if t, err := d.String(len(tag)); err != nil || t != tag {
		return record{}, malformed("not a roster entry", nil)
	}
	var r record
	var err error
	if r.op, err = d.Uvarint(); err != nil {
		return record{}, malformed("op", err)
	}
	switch r.op {
	case opCert:
		raw, err := d.Bytes(maxCertBytes)
		if err != nil {
			return record{}, malformed("certificate", err)
		}
		if r.cert, err = koloid.DecodeDeviceCert(raw); err != nil {
			return record{}, malformed("certificate", err)
		}
	case opRevocation:
		raw, err := d.Bytes(maxRevocationBytes)
		if err != nil {
			return record{}, malformed("revocation", err)
		}
		if r.rev, err = koloid.DecodeRevocation(raw); err != nil {
			return record{}, malformed("revocation", err)
		}
		has, err := d.Bool()
		if err != nil {
			return record{}, malformed("target flag", err)
		}
		if has {
			raw, err := d.Bytes(maxCertBytes)
			if err != nil {
				return record{}, malformed("target", err)
			}
			if r.target, err = koloid.DecodeDeviceCert(raw); err != nil {
				return record{}, malformed("target", err)
			}
		}
	default:
		return record{}, malformed(fmt.Sprintf("op %d", r.op), nil)
	}
	if err := d.Done(); err != nil {
		return record{}, malformed("trailing bytes", err)
	}
	return r, nil
}

// The kolo-id encodings are at most 514 (a certificate with an approver parent) and 472 (a revocation) bytes.
const (
	maxCertBytes       = 514
	maxRevocationBytes = 472
)

// CertBody returns the body of the data entry that records c in a roster.
func CertBody(c *koloid.DeviceCert) ([]byte, error) {
	raw := c.Encode()
	if raw == nil {
		return nil, fmt.Errorf("%w: nil certificate", kolobraid.ErrInvalidArgument)
	}
	var e kolocodec.Encoder
	e.PutString(tag)
	e.PutUvarint(opCert)
	e.PutBytes(raw)
	return e.Bytes(), nil
}

// RevocationBody returns the body of the data entry that records r; an approver-signed revocation needs target, the other kinds
// take nil.
func RevocationBody(r *koloid.Revocation, target *koloid.DeviceCert) ([]byte, error) {
	raw := r.Encode()
	switch {
	case raw == nil:
		return nil, fmt.Errorf("%w: nil revocation", kolobraid.ErrInvalidArgument)
	case (r.TargetHash() != nil) != (target != nil):
		return nil, fmt.Errorf("%w: a target certificate goes with an approver-signed revocation and with no other", kolobraid.ErrInvalidArgument)
	}
	var e kolocodec.Encoder
	e.PutString(tag)
	e.PutUvarint(opRevocation)
	e.PutBytes(raw)
	e.PutBool(target != nil)
	if target != nil {
		e.PutBytes(target.Encode())
	}
	return e.Bytes(), nil
}

// proofTag starts the join request data of a device that asks to be admitted.
const proofTag = "kolo-braid/devices/proof/v0"

// proof is the data of a join request that carries a device certificate.
type proof struct {
	cert *koloid.DeviceCert
	sig  []byte
	app  []byte
}

func (p proof) encode() []byte {
	var e kolocodec.Encoder
	e.PutString(proofTag)
	e.PutBytes(p.cert.Encode())
	e.PutFixed(p.sig)
	e.PutBytes(p.app)
	return e.Bytes()
}

var errNoProof = errors.New("kolobraid: the join request carries no device proof")

func decodeProof(b []byte) (proof, error) {
	d := kolocodec.NewDecoder(b)
	if t, err := d.String(len(proofTag)); err != nil || t != proofTag {
		return proof{}, errNoProof
	}
	var p proof
	raw, err := d.Bytes(maxCertBytes)
	if err != nil {
		return proof{}, malformed("proof certificate", err)
	}
	if p.cert, err = koloid.DecodeDeviceCert(raw); err != nil {
		return proof{}, malformed("proof certificate", err)
	}
	if p.sig, err = d.Fixed(64); err != nil {
		return proof{}, malformed("proof signature", err)
	}
	if p.app, err = d.Bytes(d.Len()); err != nil {
		return proof{}, malformed("proof data", err)
	}
	if err := d.Done(); err != nil {
		return proof{}, malformed("proof trailing bytes", err)
	}
	return p, nil
}
