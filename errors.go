package kolobraid

import "errors"

var (
	// ErrClosed is returned by a call on a closed braid.
	ErrClosed = errors.New("kolobraid: closed")
	// ErrInvalidArgument is returned for a nil or out of range argument.
	ErrInvalidArgument = errors.New("kolobraid: invalid argument")
	// ErrInvalidConfig is returned for a GroupConfig outside its ranges.
	ErrInvalidConfig = errors.New("kolobraid: invalid group config")
	// ErrNotMember is returned when the caller's key is not an active member.
	ErrNotMember = errors.New("kolobraid: not an active member")
	// ErrNotAdmin is returned when the caller's key is not an admin.
	ErrNotAdmin = errors.New("kolobraid: not an admin")
	// ErrRemoved is returned when the caller's strand was removed from the braid.
	ErrRemoved = errors.New("kolobraid: member removed")
	// ErrTooLarge is returned for a value over its bound.
	ErrTooLarge = errors.New("kolobraid: value too large")
	// ErrRejected is wrapped by the error that Reject returns.
	ErrRejected = errors.New("kolobraid: entry rejected")
	// ErrViewLimit is returned by a Tx call over a per-entry bound.
	ErrViewLimit = errors.New("kolobraid: view limit exceeded")
	// ErrTxDone is returned by a Tx used after apply returned.
	ErrTxDone = errors.New("kolobraid: transaction done")
	// ErrNotFound is returned for a key or entry that does not exist.
	ErrNotFound = errors.New("kolobraid: not found")
	// ErrHalted is returned when apply failed on this peer and nothing more is applied.
	ErrHalted = errors.New("kolobraid: view halted")
	// ErrDiverged is returned when this peer's running digest differs from one that a quorum of seats claimed.
	ErrDiverged = errors.New("kolobraid: view diverged from a certified digest")
	// ErrErased is returned when an entry the braid needs was erased from the own strand.
	ErrErased = errors.New("kolobraid: strand entry erased")
	// ErrEquivocation is returned when a writer signed two histories of its strand.
	ErrEquivocation = errors.New("kolobraid: writer signed two histories")
	// ErrWrongApp is returned when an app tag differs from the genesis tag.
	ErrWrongApp = errors.New("kolobraid: wrong app tag")
	// ErrWrongGroup is returned for an entry or message of another group.
	ErrWrongGroup = errors.New("kolobraid: wrong group")
	// ErrCorrupt is returned for stored data that does not decode or verify.
	ErrCorrupt = errors.New("kolobraid: corrupt data")
	// ErrProtocol is returned for a stream message that breaks the protocol.
	ErrProtocol = errors.New("kolobraid: protocol violation")
	// ErrUnconfirmed is returned when the own strand is not yet confirmed and the call's context ended.
	ErrUnconfirmed = errors.New("kolobraid: own strand not confirmed")
	// ErrStrandLost is returned when a history of the own strand conflicts with the local one.
	ErrStrandLost = errors.New("kolobraid: own strand lost")
	// ErrBadSeed is returned when the seed entries of a successor braid do not match their content digest.
	ErrBadSeed = errors.New("kolobraid: seed does not match its digest")
	// ErrReorged is returned by Entries when a position it already yielded changed.
	ErrReorged = errors.New("kolobraid: order changed")
	// ErrKnownStrand is returned by Admit for a strand log ID that is already used or known.
	ErrKnownStrand = errors.New("kolobraid: strand already known")
	// ErrGroupFull is returned by CheckAdmit when the group already has MaxMembers active members.
	ErrGroupFull = errors.New("kolobraid: group full")
	// ErrActiveKey is returned by Admit for a key that is already an active member.
	ErrActiveKey = errors.New("kolobraid: key already active")
	// ErrMalformed is returned by a decoder for bytes that break a format rule or are not canonical.
	ErrMalformed = errors.New("kolobraid: malformed encoding")
)
