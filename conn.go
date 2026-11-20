package kolobraid

import (
	"context"
	"crypto/ed25519"
	"reflect"

	kolotransport "github.com/ciranda-dev/kolo-transport"
)

// Conn is an authenticated connection to one peer, direct or relayed, such as a *kolosecure.Conn, a *kolorelay.Conn or a
// koloswarm.Conn; RemotePublicKey must return the key the peer proved in the end-to-end handshake.
type Conn interface {
	RemotePublicKey() ed25519.PublicKey
	OpenStream(ctx context.Context) (*kolotransport.Stream, error)
	AcceptStream(ctx context.Context) (*kolotransport.Stream, error)
	OpenUniStream(ctx context.Context) (*kolotransport.SendStream, error)
	AcceptUniStream(ctx context.Context) (*kolotransport.ReceiveStream, error)
	Context() context.Context
	Close() error
	CloseWithError(code uint64, msg string) error
}

// noConn reports whether c is nil or holds a nil pointer, such as a nil *kolosecure.Conn.
func noConn(c Conn) bool {
	if c == nil {
		return true
	}
	v := reflect.ValueOf(c)
	return v.Kind() == reflect.Pointer && v.IsNil()
}
