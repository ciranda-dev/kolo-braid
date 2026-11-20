package kolobraid

import (
	"crypto/ed25519"
	"fmt"
	"log/slog"
	"time"

	kololog "github.com/ciranda-dev/kolo-log"
	kolostore "github.com/ciranda-dev/kolo-store"
)

// Options are local to this peer.
type Options struct {
	// Backend holds every strand, the view and the braid's own records; required. Close does not close it.
	Backend kolostore.Backend
	// Prefix is the key prefix of the braid inside Backend, so several braids may share one; nil keeps the braid at the root of
	// Backend. A prefix that nests with another braid's in the same Backend is refused; see PrefixBackend.
	Prefix []byte
	// App is required and must equal the genesis tag.
	App string
	// Apply turns ordered entries into the view; required.
	Apply ApplyFunc
	// Pins are peers that get ciphertext only and can confirm the own strand.
	Pins []ed25519.PublicKey
	// Limits are the local bounds; nil means the defaults.
	Limits *Limits
	// AppendUnconfirmed writes before the own strand is confirmed after Open. A device restored from an older copy then signs a
	// second history of its strand; for a settler that halts settlement, and with 3 or 5 settlers under Majority it can split it.
	AppendUnconfirmed bool
	// Rebuild replays the view from genesis along the stored committed pivots at Open.
	Rebuild bool
	// Predecessor is the old braid, as this peer holds it, of a successor braid it joins: once the successor's genesis is
	// held, its founder must have been an admin of the old settled roster at the old position the genesis names, and the old
	// settled view replayed to that position must have the content digest it names; otherwise the braid halts with
	// ErrNotAdmin or ErrBadSeed, or with ErrNotFound while the old braid has not settled that position (Open checks again).
	// Without it a joiner trusts the successor's founder for both.
	Predecessor *Braid
	// Logger receives operational logs; nil means none. Keys, bodies and view data are never logged.
	Logger *slog.Logger
}

// Limits are the local bounds of a peer. A zero field takes its default.
type Limits struct {
	// MaxTail is the most tentative entries applied; default 2048 (a reorg replays them within 50 ms), 64 to 2^20.
	MaxTail int
	// MaxTailBytes bounds the tentative overlay; default 4 x 16 x MaxBody (four entries at the write budget) within 1 MiB to 4 GiB.
	MaxTailBytes int64
	// MaxPendingBytes bounds the entries that wait for a predecessor; default 16 MiB, 1 MiB to 1 GiB.
	MaxPendingBytes int64
	// MaxForks is how many second histories of one strand are kept; default 4, 1 to 64: a restored backup that writes on is one,
	// and each one a hostile pair signs costs every peer a segment and a slot in later clocks. At the bound the entries of the next
	// history are refused and no longer fetched, so an entry that needs one never places on this peer.
	MaxForks int
	// MaxPullAhead is how far past its settled length a member's strand is fetched, and a settler's past its newest pulse
	// held; what a held entry links is fetched past it. It is checked every AnnounceGap. Default MaxTail, 64 to 2^20.
	MaxPullAhead int
	// PulseGap is the least time between two pulses of this seat; default 500ms, 10ms to 1m.
	PulseGap time.Duration
	// PivotWait is how long a seat waits for a pivot before voting without it; default 3s, 100ms to 5m.
	PivotWait time.Duration
	// AnnounceGap is the least time between two head announcements on one connection; default 100ms, 10ms to 10s.
	AnnounceGap time.Duration
	// ConfirmWait is how long a seat waits for the other seats' heads after Open; default 10s, 1s to 5m.
	ConfirmWait time.Duration
	// MaxEvents is how many events wait for Next; default 1024, 16 to 2^16.
	MaxEvents int
}

// minMaxPullAhead is the least MaxPullAhead; pulseSpan depends on it.
const minMaxPullAhead = 64

const (
	defaultMaxTail         = 2048
	defaultMaxPendingBytes = 16 << 20
	defaultMaxForks        = 4
	defaultPulseGap        = 500 * time.Millisecond
	defaultPivotWait       = 3 * time.Second
	defaultAnnounceGap     = 100 * time.Millisecond
	defaultConfirmWait     = 10 * time.Second
	defaultMaxEvents       = 1024
)

// withDefaults returns l with every zero field set; maxBody is the group's MaxBody.
func (l Limits) withDefaults(maxBody int) Limits {
	if l.MaxTail == 0 {
		l.MaxTail = defaultMaxTail
	}
	if l.MaxTailBytes == 0 {
		l.MaxTailBytes = min(max(4*budgetFactor*int64(maxBody), minMaxTailBytes), maxMaxTailBytes)
	}
	if l.MaxPendingBytes == 0 {
		l.MaxPendingBytes = defaultMaxPendingBytes
	}
	if l.MaxForks == 0 {
		l.MaxForks = defaultMaxForks
	}
	if l.MaxPullAhead == 0 {
		l.MaxPullAhead = l.MaxTail
	}
	if l.PulseGap == 0 {
		l.PulseGap = defaultPulseGap
	}
	if l.PivotWait == 0 {
		l.PivotWait = defaultPivotWait
	}
	if l.AnnounceGap == 0 {
		l.AnnounceGap = defaultAnnounceGap
	}
	if l.ConfirmWait == 0 {
		l.ConfirmWait = defaultConfirmWait
	}
	if l.MaxEvents == 0 {
		l.MaxEvents = defaultMaxEvents
	}
	return l
}

func (l Limits) validate() error {
	var why string
	switch {
	case l.MaxTail < minMaxTail || l.MaxTail > maxMaxTail:
		why = "MaxTail must be 64 to 2^20"
	case l.MaxTailBytes < minMaxTailBytes || l.MaxTailBytes > maxMaxTailBytes:
		why = "MaxTailBytes must be 1 MiB to 4 GiB"
	case l.MaxPendingBytes < 1<<20 || l.MaxPendingBytes > 1<<30:
		why = "MaxPendingBytes must be 1 MiB to 1 GiB"
	case l.MaxForks < 1 || l.MaxForks > 64:
		why = "MaxForks must be 1 to 64"
	case l.MaxPullAhead < minMaxPullAhead || l.MaxPullAhead > 1<<20:
		why = "MaxPullAhead must be 64 to 2^20"
	case l.PulseGap < 10*time.Millisecond || l.PulseGap > time.Minute:
		why = "PulseGap must be 10ms to 1m"
	case l.PivotWait < 100*time.Millisecond || l.PivotWait > 5*time.Minute:
		why = "PivotWait must be 100ms to 5m"
	case l.AnnounceGap < 10*time.Millisecond || l.AnnounceGap > 10*time.Second:
		why = "AnnounceGap must be 10ms to 10s"
	case l.ConfirmWait < time.Second || l.ConfirmWait > 5*time.Minute:
		why = "ConfirmWait must be 1s to 5m"
	case l.MaxEvents < 16 || l.MaxEvents > 1<<16:
		why = "MaxEvents must be 16 to 2^16"
	default:
		return nil
	}
	return fmt.Errorf("%w: %s", ErrInvalidArgument, why)
}

func (o *Options) check() error {
	switch {
	case o.Backend == nil:
		return fmt.Errorf("%w: Options.Backend is required", ErrInvalidArgument)
	case o.Apply == nil:
		return fmt.Errorf("%w: Options.Apply is required", ErrInvalidArgument)
	case o.App == "":
		return fmt.Errorf("%w: Options.App is required", ErrInvalidArgument)
	}
	for _, p := range o.Pins {
		if len(p) != ed25519.PublicKeySize {
			return fmt.Errorf("%w: pin key of %d bytes", ErrInvalidArgument, len(p))
		}
	}
	if o.Limits != nil {
		return o.Limits.withDefaults(defaultMaxBody).validate()
	}
	return nil
}

func (o *Options) limits(maxBody int) (Limits, error) {
	var l Limits
	if o.Limits != nil {
		l = *o.Limits
	}
	l = l.withDefaults(maxBody)
	return l, l.validate()
}

// EntryRef names one entry the braid wrote: its strand, its index there and its ID.
type EntryRef struct {
	Strand kololog.LogID
	Index  uint64
	ID     EntryID
}
